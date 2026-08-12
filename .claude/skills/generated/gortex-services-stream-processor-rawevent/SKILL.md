---
name: gortex-services-stream-processor-rawevent
description: "Work in the services/stream-processor · RawEvent area — 26 symbols across 4 files (89% cohesion)"
---

# services/stream-processor · RawEvent

26 symbols | 4 files | 89% cohesion

## When to Use

Use this skill when working on files in:
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/serde/AvroSerdes.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/DedupProcessor.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/ProcessorRegistry.kt`
- `event-platform/services/stream-processor/test/unit/ProcessorRegistryTest.kt`

## Key Files

| File | Symbols |
|------|---------|
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/serde/AvroSerdes.kt` | deserialize, component4, data, topic, topic, ... |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/DedupProcessor.kt` | transform, key, value |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/ProcessorRegistry.kt` | name, name, supplier, ProcessorRegistry, get, ... |
| `event-platform/services/stream-processor/test/unit/ProcessorRegistryTest.kt` | `unregistered processor throws IllegalStateException`, `registered processor is retrievable`, ProcessorRegistryTest |

## Entry Points

- `event-platform/services/stream-processor/test/unit/ProcessorRegistryTest.kt::ProcessorRegistryTest.`unregistered processor throws IllegalStateException``

## How to Explore

```
get_communities with id: "community-22"
smart_context with task: "understand services/stream-processor · RawEvent", format: "gcx"
find_usages with id: "event-platform/services/stream-processor/test/unit/ProcessorRegistryTest.kt::ProcessorRegistryTest.`unregistered processor throws IllegalStateException`", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
