# 20260812-010: In-Memory Coarse Deduplication & Fail-Open Resilience Architecture

**Status**: accepted
**Date**: 2026-08-13
**Deciders**: Engineering / Platform Team

---

## ── Context & Problem Statement ──────────────────────────────────────────────────

1. **Hot-Path Network RTT & Connection Contention**: Synchronous `SetNX` calls to Redis on every HTTP request created network round-trip amplification and Redis connection pool contention under 10,000+ RPS.
2. **Single Point of Failure (SPOF)**: When Redis experienced connection timeouts or micro-failovers, the ingestion handler dropped requests and returned `503 Service Unavailable` (`ResultDedupCheckFailed`).
3. **Decoupled Architecture Alignment**: Exact stateful deduplication belongs downstream in stream processing (RocksDB state store). Edge deduplication must be fast, non-blocking, and fail-open.

---

## ── Failure Modes Solved ─────────────────────────────────────────────

* **FM-1 (Redis SPOF & 503 Outages)**: Micro-failovers in Redis no longer drop event ingestion requests. The system fails open, returns `202 Accepted`, and delegates exact stateful deduplication to downstream Kafka Streams.
* **FM-2 (Hot-Path Network Latency Amplification)**: In-memory cache checks eliminate Redis network round-trips for duplicate event streams.

---

## ── Decision Drivers ─────────────────────────────────────────────────────────

* **D1**: Edge ingestion availability must not depend on Redis uptime.
* **D2**: In-memory coarse filtering must short-circuit repeat network calls to Redis.
* **D3**: Redis failures must log errors and fail open without blocking client requests.

---

## ── Decision Outcome ─────────────────────────────────────────────────────────

1. **In-Memory Coarse Deduplication**: Added `sync.Map` cache layer in `redisDeduper`. Duplicate event IDs hit memory directly with 0ms network latency.
2. **Fail-Open Resilience**: On Redis error or timeout, `SeenBefore` logs the error via `slog.Error` and returns `false, nil` (fail-open).
3. **Pipeline Rule Safeguard**: `BuildDeduplicationRule` in `rules.go` returns `true, nil` on deduper errors to ensure ingestion proceeds to Kafka.
4. **Integration Test Alignment**: Updated `TestDeduperFailure` to verify that Redis errors return `202 Accepted` (fail-open) rather than `503 Service Unavailable`.

---

## ── Review Trigger ───────────────────────────────────────────────────────────

Review if memory growth in edge replicas requires explicit TTL eviction policy on `sync.Map` under 100M+ unique keys/day.
