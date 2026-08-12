---
name: gortex-services-stream-processor-topologydefinition
description: "Work in the services/stream-processor · TopologyDefinition area — 19 symbols across 2 files (94% cohesion)"
---

# services/stream-processor · TopologyDefinition

19 symbols | 2 files | 94% cohesion

## When to Use

Use this skill when working on files in:
- `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/TopologyDefinition.kt`
- `event-platform/services/stream-processor/test/unit/TopologyDefinitionTest.kt`

## Key Files

| File | Symbols |
|------|---------|
| `event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/topology/TopologyDefinition.kt` | component2, component4, component2, component1, component5, ... |
| `event-platform/services/stream-processor/test/unit/TopologyDefinitionTest.kt` | `step config defaults to empty map`, `empty steps list is valid`, `default values are set correctly`, `step config preserves provided values`, TopologyDefinitionTest, ... |

## Entry Points

- `event-platform/services/stream-processor/test/unit/TopologyDefinitionTest.kt::TopologyDefinitionTest.`custom values override defaults``
- `event-platform/services/stream-processor/test/unit/TopologyDefinitionTest.kt::TopologyDefinitionTest.`step config preserves provided values``

## How to Explore

```
get_communities with id: "community-23"
smart_context with task: "understand services/stream-processor · TopologyDefinition", format: "gcx"
find_usages with id: "event-platform/services/stream-processor/test/unit/TopologyDefinitionTest.kt::TopologyDefinitionTest.`custom values override defaults`", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
