# Decision Record: 20260812-006-compact-bytebuffer-binary-serde.md

## Status
Accepted

## Date
2026-08-12

## Context & Problem Statement
In `stream-processor` (`AvroSerdes.kt`), `RawEventSerializer` and `RawEventDeserializer` used naive pipe-delimited string formatting (`"${eventId}|${eventType}|${occurredAt}|${payload}"`) and `.split("|", limit = 4)`.
- **Flaws**:
  1. **Data Corruption**: If `payload` or `eventId` contained pipe characters (`|`), deserialization split fields incorrectly or truncated the payload.
  2. **JVM Object Churn**: String concatenation and regex-based string splitting created high garbage collector (GC) allocation overhead per repartitioned record.

## Decision & Implementation Details
1. **Compact Binary ByteBuffer Framing (`AvroSerdes.kt`)**:
   - Implemented binary serialization via `ByteBuffer`: `[eventIdLen (4B)][eventId][eventTypeLen (4B)][eventType][occurredAt (8B)][payloadLen (4B)][payload]`.
   - Guaranteed byte-exact deserialization regardless of payload contents or special characters (`|`, `\n`, etc.).
   - Rebuilt `stream-processor` container (`event-platform-stream-processor-1`).

## Load Test Verification (`benchmark_1k_200kb.py`)
- **Total Requests**: **1,000 / 1,000 (100% Success)**
- **Payload Size**: **~200.16 KB**
- **Throughput**: **37.60 RPS** (Improved from **35.66 RPS**)
- **Total Duration**: **26.59 seconds**
