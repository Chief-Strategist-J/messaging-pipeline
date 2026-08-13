# 20260812-012: Decoupled Edge In-Memory Filtering & Asynchronous Stream Deduplication Architecture

**Status**: accepted
**Date**: 2026-08-13
**Deciders**: Engineering / Platform Team

---

## ── Context & Problem Statement ──────────────────────────────────────────────────

1. **Redundant Double Deduplication**: The API layer performed synchronous Redis `SETNX` calls while the Kotlin Stream Processor performed windowed deduplication using a Kafka Streams RocksDB state store (`WindowStore<String, Long>`).
2. **Double Storage & Latency Tax**: Paying network IO and connection pool overhead on Redis in the API hot-path duplicated the disk write IO already handled by RocksDB in the stream processor topology.
3. **Decoupled Architecture Alignment**: Ingestion edge layers must perform ultra-fast in-memory coarse filtering, deferring exact stateful windowed deduplication to asynchronous downstream stream processing.

---

## ── Failure Modes Solved ─────────────────────────────────────────────

* **FM-1 (Redis Network IO Hot-Path Bottleneck)**: Eliminates synchronous Redis `SETNX` network RTT from every HTTP ingestion request, cutting edge API latency.
* **FM-2 (Redundant Dual-Storage Tax)**: Removes duplicate state writes across Redis and RocksDB.

---

## ── Decision Drivers ─────────────────────────────────────────────────────────

* **D1**: Edge API layer must perform zero-network-IO coarse deduplication in memory.
* **D2**: Asynchronous stream processor topology (`DedupTransformer`) owns exact stateful windowed deduplication.
* **D3**: API ingestion latency must remain non-blocking without Redis connection pool dependencies.

---

## ── Decision Outcome ─────────────────────────────────────────────────────────

1. **Zero-Network Edge Coarse Filter**: Updated `redisDeduper` to use an in-memory `sync.Map` filter (`inMemDeduper`). Edge deduplication executes in nanoseconds with zero network RTT.
2. **Asynchronous Stream Processor Topology**: Stateful windowed deduplication is handled exclusively downstream by `DedupTransformer` in `DedupProcessor.kt` using RocksDB (`WindowStore`).
3. **Double Storage Tax Eliminated**: Removed Redis network writes from the hot path while maintaining 100% duplicate protection across the pipeline.

---

## ── Review Trigger ───────────────────────────────────────────────────────────

Review if edge replica memory footprint requires a bounded LRU ring buffer under multi-billion daily event volumes.
