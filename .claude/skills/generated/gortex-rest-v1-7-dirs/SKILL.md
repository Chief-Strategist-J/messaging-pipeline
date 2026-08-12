---
name: gortex-rest-v1-7-dirs
description: "Work in the rest/v1 +7 dirs area — 40 symbols across 11 files (83% cohesion)"
---

# rest/v1 +7 dirs

40 symbols | 11 files | 83% cohesion

## When to Use

Use this skill when working on files in:
- ``
- `event-platform/services/ingestion-api/src/api/rest/v1/handler.go`
- `event-platform/services/ingestion-api/src/api/rest/v1/middleware.go`
- `event-platform/services/ingestion-api/src/api/rest/v1/router.go`
- `event-platform/services/ingestion-api/src/infra/tracing/tracer.go`
- `external-call::dep:go.opentelemetry.io/otel`
- `external-call::dep:go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc`
- `external-call::dep:go.opentelemetry.io/otel/propagation`
- `external-call::dep:go.opentelemetry.io/otel/sdk/resource`
- `external-call::dep:go.opentelemetry.io/otel/sdk/trace`
- `external-call::dep:go.opentelemetry.io/otel/semconv/v1.21.0`

## Key Files

| File | Symbols |
|------|---------|
| `` | HandlerFunc, panic, Hostname, NewServeMux, http, ... |
| `event-platform/services/ingestion-api/src/api/rest/v1/handler.go` | pipeline, Handler |
| `event-platform/services/ingestion-api/src/api/rest/v1/middleware.go` | WithTracing, sem, next, propagator, maxConcurrent, ... |
| `event-platform/services/ingestion-api/src/api/rest/v1/router.go` | handler, maxConcurrent, NewRouter, closure@12, mux |
| `event-platform/services/ingestion-api/src/infra/tracing/tracer.go` | otlpEndpoint, err, closure@51, res, hostname, ... |
| `external-call::dep:go.opentelemetry.io/otel` | go.opentelemetry.io/otel |
| `external-call::dep:go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` | go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc |
| `external-call::dep:go.opentelemetry.io/otel/propagation` | go.opentelemetry.io/otel/propagation |
| `external-call::dep:go.opentelemetry.io/otel/sdk/resource` | go.opentelemetry.io/otel/sdk/resource |
| `external-call::dep:go.opentelemetry.io/otel/sdk/trace` | go.opentelemetry.io/otel/sdk/trace |
| `external-call::dep:go.opentelemetry.io/otel/semconv/v1.21.0` | go.opentelemetry.io/otel/semconv/v1.21.0 |

## Entry Points

- `event-platform/services/ingestion-api/src/api/rest/v1/middleware.go::WithTracing`
- `event-platform/services/ingestion-api/src/infra/tracing/tracer.go::InitTracing`

## Connected Communities

- **. +2 dirs · TestEventsApiIntegration** (3 cross-edges)
- **. +2 dirs · EvaluationContext** (1 cross-edges)
- **events/tests +3 dirs** (1 cross-edges)
- **. +2 dirs · TestValidatePayload** (1 cross-edges)

## How to Explore

```
get_communities with id: "community-11"
smart_context with task: "understand rest/v1 +7 dirs", format: "gcx"
find_usages with id: "event-platform/services/ingestion-api/src/api/rest/v1/middleware.go::WithTracing", format: "gcx"
```

_`format: "gcx"` returns the [GCX1 compact wire format](../../docs/wire-format.md) — round-trippable, ~27% fewer tokens than JSON. Drop it for JSON output; agents using `@gortex/wire` or the Go `github.com/gortexhq/gcx-go` package decode either._
