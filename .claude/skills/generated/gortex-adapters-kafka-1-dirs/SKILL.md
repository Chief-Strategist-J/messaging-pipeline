---
name: gortex-adapters-kafka-1-dirs
description: "Work in the adapters/kafka +1 dirs area — 13 symbols across 2 files (80% cohesion)"
---

# adapters/kafka +1 dirs

13 symbols | 2 files | 80% cohesion

## When to Use

Use this skill when working on files in:
- `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go`
- `external-call::dep:github.com/twmb/franz-go/pkg/kgo`

## Key Files

| File | Symbols |
|------|---------|
| `event-platform/services/ingestion-api/src/infra/adapters/kafka/producer.go` | client, kafkaProducer, Producer, client, schemaID, ... |
| `external-call::dep:github.com/twmb/franz-go/pkg/kgo` | github.com/twmb/franz-go/pkg/kgo |

## How to Explore

```
get_communities with id: "community-24"
smart_context with task: "understand adapters/kafka +1 dirs", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
