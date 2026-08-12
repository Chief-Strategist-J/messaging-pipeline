---
name: gortex-services-stream-processor-build
description: "Work in the services/stream-processor · build area — 36 symbols across 10 files (95% cohesion)"
---

# services/stream-processor · build

36 symbols | 10 files | 95% cohesion

## When to Use

Use this skill when working on files in:
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/Application.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/Constants.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/serde/AvroSerdes.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/BuiltinSteps.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/StepRegistry.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/TopologyBuilder.kt`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/DedupProcessor.kt`
- `event-platform/services/stream-processor/test/integration/TopologyBuilderIntegrationTest.kt`
- `event-platform/services/stream-processor/test/unit/BuiltinStepsTest.kt`
- `event-platform/services/stream-processor/test/unit/StepRegistryTest.kt`

## Key Files

| File | Symbols |
|------|---------|
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/Application.kt` | main |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/Constants.kt` | Constants |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/serde/AvroSerdes.kt` | stringSerde, genericAvroSerde, AvroSerdes, rawEventSerde, longSerde, ... |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/BuiltinSteps.kt` | registerBuiltinSteps |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/StepRegistry.kt` | type, StepRegistry, get, fn, register, ... |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/TopologyBuilder.kt` | build, field, extractField, TopologyBuilder, evt |
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/DedupProcessor.kt` | context, init, DedupTransformer, close |
| `event-platform/services/stream-processor/test/integration/TopologyBuilderIntegrationTest.kt` | `topology builds successfully with valid definition`, `topology builds with empty steps`, TopologyBuilderIntegrationTest |
| `event-platform/services/stream-processor/test/unit/BuiltinStepsTest.kt` | BuiltinStepsTest, setup, `dedup step is registered`, `filterByType step is registered` |
| `event-platform/services/stream-processor/test/unit/StepRegistryTest.kt` | `registered step is retrievable`, `overwriting a step replaces it`, StepRegistryTest, `unregistered step throws IllegalStateException`, setup |

## Entry Points

- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/Application.kt::main`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/DedupProcessor.kt::DedupTransformer.init`
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/BuiltinSteps.kt::registerBuiltinSteps`
- `event-platform/services/stream-processor/test/integration/TopologyBuilderIntegrationTest.kt::TopologyBuilderIntegrationTest.`topology builds successfully with valid definition``
- `event-platform/services/stream-processor/test/unit/StepRegistryTest.kt::StepRegistryTest.`unregistered step throws IllegalStateException``

## Connected Communities

- **services/stream-processor · RawEvent** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-21"
smart_context with task: "understand services/stream-processor · build", format: "gcx"
find_usages with id: "event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/Application.kt::main", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
