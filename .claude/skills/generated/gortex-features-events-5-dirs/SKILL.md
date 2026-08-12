---
name: gortex-features-events-5-dirs
description: "Work in the features/events +5 dirs area — 60 symbols across 9 files (78% cohesion)"
---

# features/events +5 dirs

60 symbols | 9 files | 78% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/core/rules/engine.go`
- `event-platform/services/ingestion-api/src/features/events/index.go`
- `event-platform/services/ingestion-api/src/features/events/rules.go`
- `event-platform/services/ingestion-api/src/features/events/service.go`
- `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go`
- `event-platform/services/ingestion-api/src/features/events/types.go`
- `event-platform/services/ingestion-api/src/infra/adapters/redis/dedup.go`
- `event-platform/services/ingestion-api/src/infra/tracing/tracer.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | bool, error |
| `event-platform/services/ingestion-api/src/core/rules/engine.go` | ok, ctx, GetMetadata, Evaluate, evalCtx, ... |
| `event-platform/services/ingestion-api/src/features/events/index.go` | Rule |
| `event-platform/services/ingestion-api/src/features/events/rules.go` | BuildDeduplicationRule, BuildEventTypeLookupRule, deduper, closure@103, closure@87, ... |
| `event-platform/services/ingestion-api/src/features/events/service.go` | name, fn, ok, RegisterCustomProcessor, name, ... |
| `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go` | ctx, seen, mockDeduper, SeenBefore, eventID |
| `event-platform/services/ingestion-api/src/features/events/types.go` | PayloadRules, Topic, Name, CustomProcessor, EventTypeConfig |
| `event-platform/services/ingestion-api/src/infra/adapters/redis/dedup.go` | SeenBefore, redisDeduper, eventID, ctx, setByUs, ... |
| `event-platform/services/ingestion-api/src/infra/tracing/tracer.go` | err |

## Entry Points

- `event-platform/services/ingestion-api/src/features/events/rules.go::CreateIngestionPipeline`

## Connected Communities

- **features/events +3 dirs** (2 cross-edges)
- **events/tests +3 dirs** (1 cross-edges)
- **core/rules +1 dirs** (1 cross-edges)
- **. +2 dirs · EvaluationContext** (1 cross-edges)
- **events/tests +2 dirs** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-14"
smart_context with task: "understand features/events +5 dirs", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/features/events/rules.go::CreateIngestionPipeline", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
