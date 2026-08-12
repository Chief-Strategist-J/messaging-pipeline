---
name: gortex-loadtest-2-dirs-requests
description: "Work in the loadtest +2 dirs · requests area — 35 symbols across 8 files (78% cohesion)"
---

# loadtest +2 dirs · requests

35 symbols | 8 files | 78% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/loadtest/benchmark_100k.py`
- `event-platform/loadtest/benchmark_1k_200kb.py`
- `event-platform/loadtest/benchmark_concurrent.py`
- `event-platform/loadtest/test_ingestion_pipeline.py`
- `external-call::stdlib:requests`
- `runbooks/iterations/20260812-performance-report/scripts/benchmark_concurrent.py`
- `runbooks/iterations/20260812-performance-report/scripts/test_ingestion_pipeline.py`

## Key Files

| File | Symbols |
|------|---------|
| `` | encode, concurrent.futures.as_completed, perf_counter, concurrent.futures.ThreadPoolExecutor, as_completed, ... |
| `event-platform/loadtest/benchmark_100k.py` | run_benchmark |
| `event-platform/loadtest/benchmark_1k_200kb.py` | run_benchmark |
| `event-platform/loadtest/benchmark_concurrent.py` | run_benchmark, payload, session, send_request |
| `event-platform/loadtest/test_ingestion_pipeline.py` | make_event, test_malformed_json_rejected, payload, event_type, test_missing_event_id_rejected, ... |
| `external-call::stdlib:requests` | requests |
| `runbooks/iterations/20260812-performance-report/scripts/benchmark_concurrent.py` | session, payload, run_benchmark, send_request |
| `runbooks/iterations/20260812-performance-report/scripts/test_ingestion_pipeline.py` | test_invalid_event_type_rejected, test_missing_event_id_rejected, payload, event_id, event_type, ... |

## Entry Points

- `event-platform/loadtest/benchmark_concurrent.py::run_benchmark`
- `runbooks/iterations/20260812-performance-report/scripts/benchmark_concurrent.py::run_benchmark`
- `event-platform/loadtest/benchmark_1k_200kb.py::run_benchmark`
- `event-platform/loadtest/benchmark_100k.py::run_benchmark`
- `event-platform/loadtest/test_ingestion_pipeline.py::test_duplicate_event_deduped`

## Connected Communities

- **. +2 dirs · test_load_10k_requests** (15 cross-edges)
- **loadtest +2 dirs · time** (7 cross-edges)

## How to Explore

```
get_communities with id: "community-45"
smart_context with task: "understand loadtest +2 dirs · requests", format: "gcx"
find_usages with id: "event-platform/loadtest/benchmark_concurrent.py::run_benchmark", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
