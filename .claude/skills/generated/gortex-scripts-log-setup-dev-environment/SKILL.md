---
name: gortex-scripts-log-setup-dev-environment
description: "Work in the scripts · log · setup-dev-environment area — 20 symbols across 1 files (100% cohesion)"
---

# scripts · log · setup-dev-environment

20 symbols | 1 files | 100% cohesion

## When to Use

Use this skill when working on files in:
- `event-platform/scripts/setup-dev-environment.sh`

## Key Files

| File | Symbols |
|------|---------|
| `event-platform/scripts/setup-dev-environment.sh` | build_and_start, step, retry_cmd, register_avro_schemas, migrate_postgres, ... |

## Entry Points

- `event-platform/scripts/setup-dev-environment.sh::main`
- `event-platform/scripts/setup-dev-environment.sh::init_traefik_storage`
- `event-platform/scripts/setup-dev-environment.sh::display_summary`
- `event-platform/scripts/setup-dev-environment.sh::build_and_start`
- `event-platform/scripts/setup-dev-environment.sh::nuke_everything`

## How to Explore

```
get_communities with id: "community-9"
smart_context with task: "understand scripts · log · setup-dev-environment", format: "gcx"
find_usages with id: "event-platform/scripts/setup-dev-environment.sh::main", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
