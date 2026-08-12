---
name: gortex-features-events-1-dirs
description: "Work in the features/events +1 dirs area — 13 symbols across 3 files (83% cohesion)"
---

# features/events +1 dirs

13 symbols | 3 files | 83% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/features/events/service.go`
- `event-platform/services/ingestion-api/src/features/events/types.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | fmt, Errorf, ReadFile |
| `event-platform/services/ingestion-api/src/features/events/service.go` | path, data, LoadFromConfig, et, err, ... |
| `event-platform/services/ingestion-api/src/features/events/types.go` | RegistryConfig, EventTypes |

## Entry Points

- `event-platform/services/ingestion-api/src/features/events/service.go::LoadFromFile`

## How to Explore

```
get_communities with id: "community-38"
smart_context with task: "understand features/events +1 dirs", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/features/events/service.go::LoadFromFile", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
