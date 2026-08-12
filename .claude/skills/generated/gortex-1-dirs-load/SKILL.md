---
name: gortex-1-dirs-load
description: "Work in the . +1 dirs · Load area — 22 symbols across 2 files (88% cohesion)"
---

# . +1 dirs · Load

22 symbols | 2 files | 88% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/shared/config/config.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | uint32, strconv, Getenv, ParseUint |
| `event-platform/services/ingestion-api/src/shared/config/config.go` | Config, ListenAddr, MaxConcurrent, schemaID, Load, ... |

## How to Explore

```
get_communities with id: "community-20"
smart_context with task: "understand . +1 dirs · Load", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
