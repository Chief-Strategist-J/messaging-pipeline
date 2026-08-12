---
name: gortex-2-dirs-test-load-10k-requests
description: "Work in the . +2 dirs · test_load_10k_requests area — 34 symbols across 5 files (91% cohesion)"
---

# . +2 dirs · test_load_10k_requests

34 symbols | 5 files | 91% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/loadtest/test_ingestion_pipeline.py`
- `external-call::stdlib:allure`
- `external-call::stdlib:psycopg2`
- `runbooks/iterations/20260812-performance-report/scripts/test_ingestion_pipeline.py`

## Key Files

| File | Symbols |
|------|---------|
| `` | exists, join, get, loads, json, ... |
| `event-platform/loadtest/test_ingestion_pipeline.py` | pg_conn, _snapshot_kafka_lag, test_single_event_ingestion, _snapshot_docker_stats, test_load_10k_requests, ... |
| `external-call::stdlib:allure` | allure |
| `external-call::stdlib:psycopg2` | psycopg2 |
| `runbooks/iterations/20260812-performance-report/scripts/test_ingestion_pipeline.py` | _snapshot_kafka_lag, test_load_10k_requests, pg_conn, _snapshot_docker_stats, pg_conn, ... |

## Entry Points

- `event-platform/loadtest/test_ingestion_pipeline.py::test_load_10k_requests`
- `runbooks/iterations/20260812-performance-report/scripts/test_ingestion_pipeline.py::test_load_10k_requests`
- `event-platform/loadtest/test_ingestion_pipeline.py::test_single_event_ingestion`
- `runbooks/iterations/20260812-performance-report/scripts/test_ingestion_pipeline.py::test_single_event_ingestion`

## Connected Communities

- **loadtest +2 dirs · requests** (6 cross-edges)

## How to Explore

```
get_communities with id: "community-4"
smart_context with task: "understand . +2 dirs · test_load_10k_requests", format: "gcx"
find_usages with id: "event-platform/loadtest/test_ingestion_pipeline.py::test_load_10k_requests", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
