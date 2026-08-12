# Decision Record: 20260812-008-zero-allocation-trace-carrier.md

## Status
Accepted

## Date
2026-08-12

## Context & Problem Statement
In `ingestion-api` (`producer.go`), OpenTelemetry trace context propagation used `RecordHeadersCarrier`.
- **Performance Crime**:
  1. `Set()` performed an $O(N)$ linear search over `kgo.RecordHeader` slices and forced a `[]byte(value)` heap allocation per injected header (`traceparent`, `tracestate`, `baggage`).
  2. `Keys()` allocated a fresh `[]string` slice on every single message production.
- **Impact**: Under 10,000 RPS, generating new slice headers and keys generated 30,000+ unnecessary heap allocations per second, driving garbage collection pauses.

## Decision & Implementation Details
1. **Pointer Header Reference (`producer.go`)**:
   - Updated `RecordHeadersCarrier` to reference `Headers *[]kgo.RecordHeader` by pointer.
   - Eliminated slice header copies and reassignment overhead in `Produce()`.
2. **Static Key Slice Reuse**:
   - Replaced dynamic `Keys()` slice allocation with a static global slice reference (`var emptyCarrierKeys = []string{"traceparent", "tracestate", "baggage"}`).
   - Avoided heap allocation on every telemetry context injection.

## Verification
- Rebuilt and updated all 4 `ingestion-api` replicas cleanly.
- Code committed and synced across all remote git repositories.
