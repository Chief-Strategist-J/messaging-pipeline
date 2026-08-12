---
name: gortex-loadtest-2-dirs-time
description: "Work in the loadtest +2 dirs · time area — 15 symbols across 5 files (72% cohesion)"
---

# loadtest +2 dirs · time

15 symbols | 5 files | 72% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/loadtest/benchmark_100k.py`
- `event-platform/loadtest/benchmark_1k_200kb.py`
- `event-platform/loadtest/benchmark_concurrent.py`
- `runbooks/iterations/20260812-performance-report/scripts/benchmark_concurrent.py`

## Key Files

| File | Symbols |
|------|---------|
| `` | choices, time, random, time, randint, ... |
| `event-platform/loadtest/benchmark_100k.py` | send_request, session |
| `event-platform/loadtest/benchmark_1k_200kb.py` | size_bytes, generate_random_payload |
| `event-platform/loadtest/benchmark_concurrent.py` | size_bytes, generate_random_payload |
| `runbooks/iterations/20260812-performance-report/scripts/benchmark_concurrent.py` | generate_random_payload, size_bytes |

## Entry Points

- `event-platform/loadtest/benchmark_100k.py::send_request`

## Connected Communities

- **. +2 dirs · test_load_10k_requests** (3 cross-edges)

## How to Explore

```
get_communities with id: "community-46"
smart_context with task: "understand loadtest +2 dirs · time", format: "gcx"
find_usages with id: "event-platform/loadtest/benchmark_100k.py::send_request", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
