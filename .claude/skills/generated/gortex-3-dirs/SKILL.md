---
name: gortex-3-dirs
description: "Work in the . +3 dirs area — 21 symbols across 4 files (77% cohesion)"
---

# . +3 dirs

21 symbols | 4 files | 77% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/cmd/server/main.go`
- `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go`
- `event-platform/services/ingestion-api/src/infra/adapters/redis/dedup.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | ListenAndServe, FeatureRegisterCustomProcessor, FeatureLoadFromFile, Exit |
| `event-platform/services/ingestion-api/cmd/server/main.go` | handler, srv, main, err, producer, ... |
| `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go` | Close |
| `event-platform/services/ingestion-api/src/infra/adapters/redis/dedup.go` | NewRedisDeduper, addr |

## Entry Points

- `event-platform/services/ingestion-api/cmd/server/main.go::main`

## Connected Communities

- **. +2 dirs · TestValidatePayload** (2 cross-edges)
- **rest/v1 +7 dirs** (2 cross-edges)
- **. +2 dirs · TestEventsApiIntegration** (1 cross-edges)
- **adapters/kafka +1 dirs** (1 cross-edges)
- **. +1 dirs · Load** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-10"
smart_context with task: "understand . +3 dirs", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/cmd/server/main.go::main", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
