---
name: gortex-2-dirs-testeventsapiintegration
description: "Work in the . +2 dirs · TestEventsApiIntegration area — 55 symbols across 3 files (88% cohesion)"
---

# . +2 dirs · TestEventsApiIntegration

55 symbols | 3 files | 88% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/api/rest/v1/handler.go`
- `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | FeatureCreateIngestionPipeline, httptest, NewRecorder, Error, ReadAll, ... |
| `event-platform/services/ingestion-api/src/api/rest/v1/handler.go` | errMsg, d, errMsg, p, err, ... |
| `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go` | handler, t, reqDup, deduper, reqBody, ... |

## Entry Points

- `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go::TestEventsApiIntegration`

## Connected Communities

- **features/events +3 dirs** (3 cross-edges)
- **core/rules +1 dirs** (1 cross-edges)
- **. +2 dirs · EvaluationContext** (1 cross-edges)
- **. +1 dirs · TestLoadFromConfigAndGet** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-40"
smart_context with task: "understand . +2 dirs · TestEventsApiIntegration", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/features/events/tests/integration_test.go::TestEventsApiIntegration", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
