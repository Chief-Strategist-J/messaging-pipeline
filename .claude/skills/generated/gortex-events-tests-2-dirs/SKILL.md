---
name: gortex-events-tests-2-dirs
description: "Work in the events/tests +2 dirs area — 23 symbols across 4 files (79% cohesion)"
---

# events/tests +2 dirs

23 symbols | 4 files | 79% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go`
- `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go`
- `event-platform/services/ingestion-api/src/features/events/types.go`

## Key Files

| File | Symbols |
|------|---------|
| `` | Now, New |
| `event-platform/services/ingestion-api/src/features/events/tests/integration_test.go` | eventID, ctx, failDeduper, eventID, Forget, ... |
| `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go` | err, t, err, evt, evtNoID, ... |
| `event-platform/services/ingestion-api/src/features/events/types.go` | EventType, RawEvent, EventID, Validate, Payload, ... |

## Entry Points

- `event-platform/services/ingestion-api/src/features/events/tests/unit_test.go::TestRawEventValidate`

## How to Explore

```
get_communities with id: "community-16"
smart_context with task: "understand events/tests +2 dirs", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/features/events/tests/unit_test.go::TestRawEventValidate", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
