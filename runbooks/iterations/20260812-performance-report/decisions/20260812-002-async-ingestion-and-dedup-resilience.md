# 20260812-002: Async Ingestion, Deduplication Resilience, Memory Reuse & Store Windowing

**Status**: accepted  
**Date**: 2026-08-12  
**Deciders**: Engineering / Platform Team  

---

## ── Context & Problem Statement ──────────────────────────────────────────────────
1. **Synchronous Cascade Bottleneck**: Ingestion HTTP handlers block on synchronous Kafka `ProduceSync` with `AllISRAcks`, capping worker thread throughput to ~40 req/sec per thread.
2. **Premature Key Mutex / Data Loss**: Redis `SetNX` records event IDs before downstream Kafka production completes. If Kafka production fails, retries see `SetNX` success and drop the event as a duplicate without producing it to Kafka (silent data loss).
3. **Thread Safety Panic**: Global package maps (`registry`, `customProcessors`) in `ingestion-api/src/features/events/service.go` are mutated and read concurrently without mutex locks, risking runtime panics.
4. **Heap Allocation Churn**: `encodeAvro` allocates new `bytes.Buffer` and slice headers on every HTTP request, driving GC overhead under high throughput.
5. **Unbounded Stream State Growth**: Kafka Streams `DedupTransformer` retains `eventId` entries indefinitely in RocksDB without TTL eviction, causing disk exhaustion over time.

---

## ── Failure Modes Solved ─────────────────────────────────────────────
* **FM-1 (Data Loss on Ingest Retry)**: Redis key committed before Kafka send -> Send fails -> Retry returns 200 OK (duplicate) -> Event lost forever.
* **FM-2 (API Handlers Hanging / Throttled)**: Synchronous Kafka network wait stalls HTTP goroutines on broker hiccups.
* **FM-3 (Runtime Concurrent Map Panic)**: Concurrent config reloads / API requests cause process termination.
* **FM-4 (Unbounded RocksDB Disk Growth)**: Persistent dedup key store expands indefinitely without TTL pruning.

---

## ── Decision Drivers ─────────────────────────────────────────────────────────
* **D1**: Ingestion throughput must scale asynchronously (>10,000 req/sec per node) without blocking HTTP handlers on broker network RTT.
* **D2**: Failed Kafka sends must allow client retries to succeed (zero silent data drops).
* **D3**: Concurrent read/write safety across all global registry maps must be absolute.
* **D4**: Zero heap buffer allocations in the Avro serialization hot path.
* **D5**: Stream processor state stores must enforce strict TTL retention bounds.

---

## ── Decision Outcome ─────────────────────────────────────────────────────────
1. **Async Kafka Producing**: Switch from synchronous `ProduceSync` to non-blocking asynchronous `Produce` with background batching (`ProducerBatchMaxBytes`, `ProducerLinger`).
2. **Transactional Deduplication Cleanup**: If message pipeline evaluation fails downstream of deduplication checking, the set Redis dedup key is immediately deleted / rolled back so client retries succeed cleanly.
3. **Thread-Safe Registries**: Wrap `registry` and `customProcessors` in `events/service.go` with `sync.RWMutex`.
4. **Buffer Pooling (`sync.Pool`)**: Use a `sync.Pool` of `bytes.Buffer` objects in `encodeAvro` to reuse memory across HTTP requests.
5. **Deduplication Store Windowing/TTL**: In `DedupTransformer`, check timestamp boundaries against a configured retention TTL window and prune stale entries.
6. **Cached OTEL Tracers**: Cache `otel.Tracer` references globally / at instance initialization instead of performing map lookups per event.

---

## ── Failure Modes Created ────────────────────────────────────────────────────
* **FM-NEW-1 (Async Produce Queue Buffer Overflow)**:
  * *Symptom*: If Kafka brokers are entirely unreachable for an extended period, background producer buffer fills up.
  * *Detection*: Monitor Kafka client buffer usage metrics and error logs.
  * *Recovery*: Producer returns error on queue buffer full, API returns 503 so client backs off with jitter.

---

## ── Review Trigger ───────────────────────────────────────────────────────────
Review if ingestion throughput exceeds 500,000 events/sec or if multi-region Kafka replication requires cross-datacenter deduplication coordination.
