# 20260812-009: True Fire-and-Forget Kafka Produce — Eliminate Synchronous errChan Block

**Status**: accepted
**Date**: 2026-08-13
**Deciders**: Engineering / Platform Team

---

## ── Context & Problem Statement ──────────────────────────────────────────────────

1. **Synchronous errChan Block on Every HTTP Goroutine**: `kafkaProducer.Produce` created a `chan error` per request, called `kgo.Client.Produce` with that channel as the callback target, then immediately blocked on `<-errChan` — converting kgo's non-blocking internal batcher call into a synchronous network-RTT wait on every HTTP handler goroutine.
2. **Goroutine Leak on ctx.Done()**: The `select` branched on `<-ctx.Done()` and returned `ctx.Err()` before the kgo callback fired. The kgo callback eventually wrote to the abandoned `errChan`, leaving it unreachable until GC and leaking the goroutine for the delivery window.
3. **Architecture Doc / Code Mismatch**: `scaling_architecture.md` claimed `202 Accepted` is returned asynchronously in ~9ms, while the implementation blocked until Kafka ACK (`AllISRAcks`), contradicting the documented behaviour.
4. **OTEL Span Closed Before Delivery**: `tracedProducer` used `defer span.End()` scoped to the HTTP handler call, so the span closed at enqueue time — not at delivery time — making Kafka latency invisible in traces.

---

## ── Failure Modes Solved ─────────────────────────────────────────────

* **FM-1 (HTTP Goroutine Starvation)**: At 10,000 RPS, thousands of goroutines blocked on Kafka RTT exhausted the worker pool, amplifying broker backpressure into HTTP timeouts.
* **FM-2 (Goroutine Leak on Client Timeout)**: ctx cancellation returned early from `select` but left a live kgo callback referencing the abandoned `errChan` until GC collected it.
* **FM-3 (Invisible Kafka Latency in Traces)**: Span closed at enqueue time — real broker ACK latency was not recorded.

---

## ── Decision Drivers ─────────────────────────────────────────────────────────

* **D1**: HTTP goroutines must never block on Kafka network round-trips. `202 Accepted` must be returned as soon as the record enters kgo's in-memory batch.
* **D2**: Delivery errors must not be silently discarded — they must be logged and recorded on the OTEL span.
* **D3**: No `chan` allocation per request. Zero goroutine leaks regardless of ctx cancellation timing.
* **D4**: OTEL span must close inside the kgo delivery callback so it accurately reflects real broker ACK latency.
* **D5**: Graceful shutdown must drain all in-flight records before the process exits.

---

## ── Decision Outcome ─────────────────────────────────────────────────────────

1. **Removed errChan + select block**: `kafkaProducer.Produce` no longer creates a channel or blocks. `kgo.Client.Produce` is called with `context.Background()` (not the HTTP request context) so kgo's internal batcher lifecycle is decoupled from request cancellation.
2. **Async delivery callback**: The kgo callback captures `span` and `slog` — on delivery error it calls `span.SetStatus(codes.Error, ...)` and `slog.Error(...)`, then calls `span.End()`. No error is propagated back to the caller; the HTTP response has already been sent.
3. **Span lifetime tied to broker ACK**: `tracedProducer.Produce` starts the span, builds `spanCtx := trace.ContextWithSpan(ctx, span)`, and passes it via `otel.GetTextMapPropagator().Inject` into the record headers. `span.End()` is called exclusively inside the delivery callback.
4. **Graceful drain on shutdown**: `kafkaProducer.Close` calls `p.client.Flush(context.Background())` before `p.client.Close()`, blocking until all pending records are acknowledged or permanently failed.
5. **`scaling_architecture.md` corrected**: Diagram edge label updated to "Fire-and-Forget Non-Blocking Produce"; prose updated to accurately describe the fire-and-forget path with async error handling.

---

## ── Failure Modes Created ────────────────────────────────────────────────────

* **FM-NEW-1 (Delivery Errors Are Post-Response)**:
  * *Symptom*: If a Kafka broker rejects a batch after the `202 Accepted` has been sent, the client has no way to know. The event is lost unless the producer retries at the kgo level.
  * *Detection*: `slog.Error("kafka delivery failed")` log line + OTEL span `status=ERROR` on the `kafka.produce` span in Tempo.
  * *Recovery*: kgo's internal retry logic handles transient broker errors. Permanent failures (e.g. auth, schema mismatch) surface in logs and traces. Callers should monitor error rates via Prometheus kgo metrics.

---

## ── Review Trigger ───────────────────────────────────────────────────────────

Review if at-least-once delivery guarantees must be surfaced synchronously to the HTTP client, or if a durable outbox pattern (write-to-DB → CDC → Kafka) is required for compliance.
