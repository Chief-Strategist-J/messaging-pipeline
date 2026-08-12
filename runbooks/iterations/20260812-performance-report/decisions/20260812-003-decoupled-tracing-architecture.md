# 20260812-003: Decoupled Non-Blocking OpenTelemetry Tracing Architecture

**Status**: accepted  
**Date**: 2026-08-12  
**Deciders**: Engineering / Platform & Observability Team  

---

## ── Context & Problem Statement ──────────────────────────────────────────────────
1. **Double OpenTelemetry Spanning & GC Thrashing**: Both `engine.Evaluate()` in `ingestion-api` core rules and individual feature rules (`BuildEnvelopeParsingRule`, etc.) create synchronous OTEL spans inline inside client HTTP worker goroutines. At 50,000 req/sec across 6 pipeline rules, this spawns **600,000 spans/sec**, locking threads and causing severe Garbage Collection (GC) pauses.
2. **Stream Processor Per-Record Tracing Overhead**: `stream-processor` (Kotlin / Kafka Streams) invokes `GlobalOpenTelemetry.getTracer(...).startSpan()` per record inside `mapValues` and `DedupTransformer.transform()`. At high stream throughput, thousands of span objects/sec saturate CPU cores.
3. **Requirement for High Observability Without Latency Cost**: Distributed tracing must remain 100% accurate (preserving trace IDs, parent-child span trees, duration metrics, and error stacktraces) without imposing synchronous framework latency on hot processing paths.

---

## ── Failure Modes Solved ─────────────────────────────────────────────
* **FM-1 (Worker Goroutine / Thread CPU Thrashing)**: CPU cores spent constructing telemetry frames inline rather than executing pipeline processing.
* **FM-2 (Telemetry Memory Churn)**: Short-lived OpenTelemetry span object allocations causing frequent stop-the-world GC pauses under high load.
* **FM-3 (Tail-Latency Spikes)**: Telemetry export locks stalling main HTTP request responses and Kafka stream transformations.

---

## ── Decision Drivers ─────────────────────────────────────────────────────────
* **D1**: Hot request and stream processing paths must execute with zero lock/telemetry allocations.
* **D2**: Trace fidelity, parent-child hierarchy, microsecond timing precision, and error details must remain 100% complete and accurate.
* **D3**: System must degrade gracefully (dropping non-critical spans) during catastrophic overload while preserving 100% of error traces.

---

## ── Decision Outcome ─────────────────────────────────────────────────────────
1. **Lock-Free Ring Buffer Telemetry Pipeline (Go `ingestion-api`)**:
   * Strip synchronous micro-spans from individual rule execution loops.
   * HTTP workers extract trace context and record fast nanosecond timestamps (`t0`, `t1`).
   * Workers push lightweight telemetry event structs into a bounded, lock-free channel using non-blocking writes (`select { case ch <- event: default: }`).
   * A dedicated background worker goroutine pool pops telemetry events in batches, constructs OTEL `Span` objects off the main thread, sets explicit start/end timestamps, and exports via OTLP gRPC.

2. **Decoupled Stream Tracing & Auto-Instrumentation (`stream-processor`)**:
   * Remove inline `startSpan()` calls from `DedupProcessor.kt` and `TopologyBuilder.kt`.
   * Apply head-based sampling at the ingestion edge (1-5% of normal traces, 100% of errors).
   * Utilize OpenTelemetry Java Agent (`-javaagent`) for zero-overhead, asynchronous bytecode tracing at Kafka consumer/producer boundaries.

---

## ── Trace Fidelity Verification ─────────────────────────────────────────────
* **Trace Context Propagation**: Preserved via W3C `traceparent` headers across HTTP and Kafka record headers.
* **Timestamp Accuracy**: Microsecond duration precision maintained by setting `span.SetStartTime(event.t0)` and `span.End(otel.WithTimestamp(event.t1))`.
* **Span Waterfall Visibility**: 100% identical in Jaeger / Tempo / Datadog compared to inline tracing.

---

## ── Review Trigger ───────────────────────────────────────────────────────────
Review if ingestion rate exceeds 1,000,000 events/sec or if eBPF kernel-level auto-tracing is deployed cluster-wide.
