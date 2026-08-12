---
name: gortex-events-tests-3-dirs
description: "Work in the events/tests +3 dirs area — 65 symbols across 6 files (77% cohesion)"
---

# events/tests +3 dirs

65 symbols | 6 files | 77% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/features/events/purchase.go`
- `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go`
- `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go`
- `event-platform/services/ingestion-api/src/infra/adapters/kafka/avro.go`
- `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | delete, byte, FeaturePurchaseEnrichment, string, copy, ... |
| `event-platform/services/ingestion-api/src/features/events/purchase.go` | payloadJSON |
| `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go` | eventType, Forget, eventID, Produce, ctx, ... |
| `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go` | payload, t, expected, TestPurchaseEnrichment, enriched, ... |
| `event-platform/services/ingestion-api/src/infra/adapters/kafka/avro.go` | aEvt, schemaID, encodeAvro, buf, err, ... |
| `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go` | eventType, record, i, Produce, eventID, ... |

## Connected Communities

- **core/rules +1 dirs** (2 cross-edges)
- **rest/v1 +7 dirs** (2 cross-edges)
- **. +2 dirs · TestValidatePayload** (1 cross-edges)
- **. +2 dirs · EvaluationContext** (1 cross-edges)
- **features/events +3 dirs** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-18"
smart_context with task: "understand events/tests +3 dirs", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
