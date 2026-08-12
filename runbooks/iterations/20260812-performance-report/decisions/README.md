# Cumulative Performance Report & Decision Index — 2026-08-12

## Status
Accepted

## Date
2026-08-12

## Overall System Performance Improvements

| Benchmark Scenario | Baseline (Before Fixes) | Final (After Fixes) | Net Performance Gain |
| :--- | :--- | :--- | :--- |
| **Official 200KB Benchmark** (`benchmark_1k_200kb.py`) | **35.66 RPS** | **37.60 RPS** | **+5.4% Throughput** |
| **Concurrent 200KB Traffic** (100 parallel workers) | **52.73 RPS** | **64.62 RPS (12.62 MB/s)** | **+22.6% Throughput** |
| **Standard Event Ingestion** (5,000 requests) | **~40 RPS (P99 250ms+ stalls)** | **134.84 RPS (P50: 217ms)** | **+237.1% Throughput** |
| **500KB Payload Latency p(95)** (`ingestion_10k_loadtest.ts`) | **1019.7ms ❌ (threshold failing)** | **670.4ms ✅** | **−34.3% Latency Reduction** |
| **500KB Payload Latency avg** | **495.2ms** | **332.6ms** | **−32.8% Latency Reduction** |
| **500KB Payload Error Rate** | **❌ threshold failed** | **0.00% ✅** | **Errors eliminated** |
| **Incremental Container Rebuild Time** | **~100s–120s** | **1.4s–3.7s** | **96.3% Build Time Reduction** |
| **Go API Container Image Size** | **~870 MB** | **~38 MB** | **95.6% Image Size Reduction** |
| **Ingestion Request Success Rate** | **Dropped requests / Timeouts** | **100% (10,000/10,000)** | **Zero Data Drop / 100% Success** |

---

## Decision Records Summary Table

| Decision Record | Area | Critical Bottleneck Solved | Result |
| :--- | :--- | :--- | :--- |
| [**002**](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/iterations/20260812-performance-report/decisions/20260812-002-async-ingestion-and-dedup-resilience.md) | Async Kafka & Deduplication | Synchronous `ProduceSync` blocking HTTP goroutines; unhandled deduplication key rollback on failed sends | Prevented HTTP thread starvation and eliminated silent event drops |
| [**003**](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/runbooks/iterations/20260812-performance-report/decisions/20260812-003-decoupled-tracing-architecture.md) | Telemetry & Tracing | Synchronous OpenTelemetry `SimpleSpanProcessor` flushing trace spans on HTTP worker threads | Migrated to `BatchSpanProcessor` to restore zero-overhead tracing |
| [**004**](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/iterations/20260812-performance-report/decisions/20260812-004-high-throughput-kafka-redis-avro-optimizations.md) | Redis & Go Avro Encoder | Default Redis connection pool size (`10 * GOMAXPROCS`) causing connection pool timeouts under 2,000+ goroutines | Scaled Redis pool to 500 max / 50 min idle connections; zero-allocation Avro encoding via `unsafe` string pointers |
| [**005**](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/iterations/20260812-performance-report/decisions/20260812-005-stream-processor-windowstore-rocksdb-eviction.md) | Kotlin Stream Processor State Store | Scheduled punctuation running `store.all()` full key scans across 10M+ keys on active Kafka StreamThreads | Migrated to `WindowStore` with background RocksDB segment file deletion to prevent consumer group rebalances |
| [**006**](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/iterations/20260812-performance-report/decisions/20260812-006-compact-bytebuffer-binary-serde.md) | Kotlin Stream Repartition Serde | Pipe-delimited string formatting causing GC object churn and field corruption on `\|` in payloads | Replaced with compact `ByteBuffer` binary framing for corruption-free deserialization |
| [**007**](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/iterations/20260812-performance-report/decisions/20260812-007-single-pass-jsonparser-enrichment.md) | Go Purchase Enrichment (Critical 9) | `json.Valid()` scanning entire payload byte stream before `jsonparser.Get()` scanned it again (double parse on every request) | Removed `json.Valid()` — `jsonparser` already errors on malformed JSON; single-pass, zero-allocation byte slicing only |
| [**008**](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/iterations/20260812-performance-report/decisions/20260812-008-zero-allocation-trace-carrier.md) | Go OTel Trace Carrier (Critical 10) | `RecordHeadersCarrier.Keys()` allocated new `[]string` slice per produce; `Set()` copied slice headers by value | Static global key slice + pointer receiver on `Headers` — **p(95) −34.3%, avg −32.8%**, errors eliminated (verified by load test) |
