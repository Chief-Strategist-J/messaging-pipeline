# 20260812-011: Zero-Allocation EvaluationContext Pooling & Bounded Body Reader Architecture

**Status**: accepted
**Date**: 2026-08-13
**Deciders**: Engineering / Platform Team

---

## ── Context & Problem Statement ──────────────────────────────────────────────────

1. **GC Heap Churn & STW Pauses**: Allocating a fresh `EvaluationContext` struct and `map[string]interface{}` on every HTTP request created ~10,000 heap allocations per second at 10,000 RPS, triggering frequent Go Stop-The-World (STW) GC pauses.
2. **CPU Cache-Line Bouncing**: Synchronous `sync.RWMutex` lock acquisition (`RLock`/`Lock`) on `EvaluationContext.Metadata` added unnecessary CPU cache-line invalidation across multi-core processors for single-threaded request evaluation.
3. **Unbounded Memory Exposure**: `io.ReadAll(r.Body)` allocated un-bounded slices on raw HTTP request bodies, creating memory allocation spikes under large payloads.

---

## ── Failure Modes Solved ─────────────────────────────────────────────

* **FM-1 (GC STW Stalls under High RPS)**: Reusing `EvaluationContext` structs via `sync.Pool` eliminates 10,000 heap allocations/sec, smoothing latency percentiles (p95/p99).
* **FM-2 (Multi-Core CPU Cache Bouncing)**: Removing `RWMutex` overhead from sequential request context metadata lookups eliminates atomic lock operations.
* **FM-3 (Unbounded Request Memory Exhaustion)**: `http.MaxBytesReader` bounds request payload reads at 10MB (`MaxBodyBytes`), preventing memory exhaustion.

---

## ── Decision Drivers ─────────────────────────────────────────────────────────

* **D1**: `EvaluationContext` structs and metadata maps must be recycled using `sync.Pool`.
* **D2**: Request evaluation metadata access must be 0-overhead (no mutex locks required for sequential request goroutines).
* **D3**: Request body reading must enforce explicit payload boundaries (`MaxBodyBytes`).

---

## ── Decision Outcome ─────────────────────────────────────────────────────────

1. **`sync.Pool` for `EvaluationContext`**: Added `evalCtxPool` in `engine.go`. `NewEvaluationContext` retrieves from pool, and `PutEvaluationContext` resets fields/map keys and recycles context structs.
2. **0-Lock Metadata Access**: Removed `sync.RWMutex` from `EvaluationContext`, replacing mutex calls with direct zero-overhead map operations.
3. **`http.MaxBytesReader` in Handler**: Wrapped `r.Body` with `http.MaxBytesReader(w, r.Body, constants.MaxBodyBytes)` in `handler.go`.
4. **Context Lifecycle**: Handler defers `rules.PutEvaluationContext(evalCtx)` to return contexts to the pool upon request completion.

---

## ── Review Trigger ───────────────────────────────────────────────────────────

Review if multi-goroutine evaluation of a single request context is introduced in future pipeline features, requiring lockless concurrent metadata maps.
