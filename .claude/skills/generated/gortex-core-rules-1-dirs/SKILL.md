---
name: gortex-core-rules-1-dirs
description: "Work in the core/rules +1 dirs area — 54 symbols across 3 files (81% cohesion)"
---

# core/rules +1 dirs

54 symbols | 3 files | 81% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/core/rules/engine.go`
- `event-platform/services/ingestion-api/src/core/rules/engine_test.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | append, errors, SliceStable, Is, sort |
| `event-platform/services/ingestion-api/src/core/rules/engine.go` | rules, closure@91, sortRules, Engine, Register, ... |
| `event-platform/services/ingestion-api/src/core/rules/engine_test.go` | err, engine, t, t, expectedErr, ... |

## Entry Points

- `event-platform/services/ingestion-api/src/core/rules/engine_test.go::TestRulesEnginePriorityOrder`

## Connected Communities

- **. +2 dirs · EvaluationContext** (3 cross-edges)
- **. +2 dirs · TestValidatePayload** (3 cross-edges)
- **events/tests +2 dirs** (1 cross-edges)
- **features/events +3 dirs** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-13"
smart_context with task: "understand core/rules +1 dirs", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/core/rules/engine_test.go::TestRulesEnginePriorityOrder", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
