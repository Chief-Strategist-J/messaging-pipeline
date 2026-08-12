---
name: gortex-2-dirs-testvalidatepayload
description: "Work in the . +2 dirs · TestValidatePayload area — 20 symbols across 3 files (81% cohesion)"
---

# . +2 dirs · TestValidatePayload

20 symbols | 3 files | 81% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/cmd/server/main.go`
- `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | FeatureValidatePayload, signal, Notify, context, WithTimeout, ... |
| `event-platform/services/ingestion-api/cmd/server/main.go` | srv, cancel, ctx, waitForShutdown, stop |
| `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go` | t, cfg, err, payloadTooLong, err, ... |

## Entry Points

- `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go::TestValidatePayload`

## Connected Communities

- **. +2 dirs · EvaluationContext** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-41"
smart_context with task: "understand . +2 dirs · TestValidatePayload", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/features/events/tests/unit_test.go::TestValidatePayload", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
