---
name: gortex-features-events-3-dirs
description: "Work in the features/events +3 dirs area — 27 symbols across 6 files (71% cohesion)"
---

# features/events +3 dirs

27 symbols | 6 files | 71% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/features/events/purchase.go`
- `event-platform/services/ingestion-api/src/features/events/rules.go`
- `event-platform/services/ingestion-api/src/features/events/service.go`
- `external-call::dep:github.com/buger/jsonparser`
- `external-call::dep:go.opentelemetry.io/otel/attribute`

## Key Files

| File | Symbols |
|------|---------|
| `` | ToUpper, len, Valid, strings, json |
| `event-platform/services/ingestion-api/src/features/events/purchase.go` | err, updated, err, PurchaseEnrichment, dataType, ... |
| `event-platform/services/ingestion-api/src/features/events/rules.go` | BuildEnvelopeParsingRule, closure@17 |
| `event-platform/services/ingestion-api/src/features/events/service.go` | ValidatePayload, cfg, val, rule, err, ... |
| `external-call::dep:github.com/buger/jsonparser` | github.com/buger/jsonparser |
| `external-call::dep:go.opentelemetry.io/otel/attribute` | go.opentelemetry.io/otel/attribute |

## Entry Points

- `event-platform/services/ingestion-api/src/features/events/purchase.go::PurchaseEnrichment`

## Connected Communities

- **events/tests +3 dirs** (3 cross-edges)
- **events/tests +2 dirs** (3 cross-edges)
- **rest/v1 +7 dirs** (2 cross-edges)
- **features/events +1 dirs** (2 cross-edges)

## How to Explore

```
get_communities with id: "community-25"
smart_context with task: "understand features/events +3 dirs", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/features/events/purchase.go::PurchaseEnrichment", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
