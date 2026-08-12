---
name: gortex-2-dirs-evaluationcontext
description: "Work in the . +2 dirs · EvaluationContext area — 20 symbols across 3 files (69% cohesion)"
---

# . +2 dirs · EvaluationContext

20 symbols | 3 files | 69% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/core/rules/engine.go`
- `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | make |
| `event-platform/services/ingestion-api/src/core/rules/engine.go` | RawPayload, val, rawPayload, OccurredAt, NewEvaluationContext, ... |
| `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go` | keys, h, Keys, i |

## Connected Communities

- **features/events +3 dirs** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-12"
smart_context with task: "understand . +2 dirs · EvaluationContext", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
