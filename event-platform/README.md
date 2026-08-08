# High-Throughput Event Ingestion & Stream Processing Pipeline — Architecture & Implementation

A production-grade, enterprise event processing platform built to sustain **1,000,000 requests in 10 minutes** (1,667 req/s sustained average, with 3x peak headroom at 5,000 req/s) on **minimum compute resources**.

---

## 🚀 Key Accomplishments & Architectural Deliverables

This repository contains the complete implementation of a multi-tier, data-driven event platform built following strict performance engineering principles, domain-driven boundaries, and enterprise quality rules (DRY, SRP, explicit typing, zero magic strings).

### 1. Dual-Tier Multi-Language Microservices
- **Ingestion API (Go)**: Built using light-weight goroutines (~2KB overhead each) compiled to a pure static binary inside a distroless container image (~15MB total). Leverages `franz-go` (pure Go, zero `cgo`) to achieve maximum CPU/memory efficiency and ultra-fast LZ4 compressed batching to Kafka.
- **Stream Processing Engine (Kotlin/JVM)**: Uses Kafka Streams DSL for exact-once processing guarantees (`EXACTLY_ONCE_V2`), windowed aggregations, and stateful transformations across stream partitions.

### 2. Fully Data-Driven & Extensible Design
- **Event Rules as Data**: New event types, field validation schemas, routing topics, and payload constraints are declared in [`config/event-types.yaml`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/config/event-types.yaml). Adding new event types requires **zero code changes** or recompilation of the HTTP ingestion API.
- **Topology as Data**: Kafka Stream processing steps and windowed aggregate definitions are built dynamically from immutable data definitions at startup, paying zero per-record runtime overhead.

### 3. Comprehensive Infrastructure & Container Automation ([docker-compose.yml](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/infra/docker-compose.yml))
Orchestrates 11 enterprise services:
- **Stateless Tier**: Ingestion API (x2 load-balanced replicas), Stream Processor
- **Event Backbone**: Apache Kafka (KRaft mode, 12 partitions for optimal parallelism), Confluent Schema Registry (Avro binary wire format serialization)
- **Persistence & Cache**: Redis (low-latency atomic `SETNX` deduplication with 24h TTL), PostgreSQL (relational sink for raw events & enriched aggregate counts)
- **Data Integration**: Kafka Connect with Confluent JDBC Sink (`upsert` mode for idempotent Postgres delivery)
- **Full Observability Stack**: OpenTelemetry Collector, Grafana Tempo (distributed tracing), Prometheus (metrics collection), Grafana (unified dashboards)
- **Centralized Environment Management**: All ports, replicas, limits, and service credentials extracted into [`infra/.env`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/infra/.env).

### 4. Zero Magic Values & Explicit Quality Controls
- **Go Constants**: All routes, span names, error messages, TTLs, and tracer IDs centralized in [`constants.go`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/services/ingestion-api/internal/constants/constants.go).
- **Kotlin Constants**: Topic names, config keys, env variables, and step types extracted into [`Constants.kt`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/services/stream-processor/src/main/kotlin/com/platform/streams/Constants.kt).
- **Strict SRP & DRY Boundaries**: Input handling, validation, registry lookup, and persistence integrations strictly isolated across packages and classes.

### 5. Multi-Tier Test Suite & Automated Report Generator
- **Unit Tests**: Coverage across raw event validation, schema configuration, type registration, payload validation, custom enrichment, and environment loading.
- **Integration Tests**: End-to-end HTTP handler flow using mock producers and deduplicators; topology builder assembly tests.
- **Benchmark Suite**: Go benchmark tests measuring nanoseconds per op, byte allocations, and heap allocs for payload validation and validation engines.
- **Performance Load Testing**: k6 constant-arrival-rate test scenario written in TypeScript ([ingestion_burst.ts](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/loadtest/ingestion_burst.ts)) proving 1,667 req/s over 10 minutes.
- **Automated HTML Test Report Generator**: [`scripts/run-tests.sh`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/scripts/run-tests.sh) runs all unit, integration, and benchmark tests, generating an interactive HTML dashboard in `reports/report.html` alongside code coverage metrics (`reports/unit/coverage.html`).

---

## 🛠 Repository Directory Map

```
event-platform/
├── config/
│   └── event-types.yaml                      # Declarative event definitions & field rules
├── schemas/avro/
│   ├── event-raw.avsc                        # Confluent Avro schema for raw events
│   └── event-enriched.avsc                   # Confluent Avro schema for aggregations
├── services/
│   ├── ingestion-api/                        # Go High-Throughput HTTP Ingestion Service
│   │   ├── cmd/server/main.go                # Application orchestrator
│   │   ├── internal/
│   │   │   ├── config/                       # Env configuration loader
│   │   │   ├── constants/                    # Centralized constants & error strings
│   │   │   ├── customprocessors/             # Specialized enrichment handlers (e.g. purchase)
│   │   │   ├── eventtypes/                   # Schema-driven validation & registry engine
│   │   │   ├── httpapi/                      # Transport controllers & middleware (OTel/Limiter)
│   │   │   ├── ingest/                       # Envelope structs, Avro wire encoder, franz-go producer
│   │   │   └── observability/                # OpenTelemetry SDK initialization
│   │   └── test/                             # Separate unit, integration & benchmark test suites
│   └── stream-processor/                     # Kotlin / Kafka Streams Aggregation Engine
│       ├── src/main/kotlin/com/platform/streams/
│       │   ├── Application.kt                # Kotlin application entrypoint
│       │   ├── Constants.kt                  # Kotlin constants object
│       │   ├── serde/                        # Avro Serde factories
│       │   └── topology/                     # Data-driven topology builder & step registries
│       └── test/                             # Separate unit & integration test suites
├── infra/
│   ├── .env                                  # Centralized environment & docker port config
│   ├── docker-compose.yml                    # 11-service cluster orchestrator
│   ├── kafka/                                # Connect Dockerfile & JDBC sink definitions
│   ├── otel-collector/                       # OpenTelemetry collector configuration
│   ├── prometheus/                           # Prometheus scraper configuration
│   ├── tempo/                                # Tempo distributed tracing configuration
│   └── k8s/                                  # Production Kubernetes Deployment & HPA manifests
├── loadtest/
│   └── ingestion_burst.ts                    # k6 TypeScript load test scenario (1M req / 10 min)
├── scripts/
│   └── run-tests.sh                          # Enterprise test execution & HTML report generator script
├── Makefile                                  # Unified command shortcuts for dev/test/build
└── README.md                                 # Technical documentation
```

---

## 🚦 Executing Services & Tests

### Start Infrastructure & Pipeline
```bash
# Start all 11 docker services in the background
make up

# Apply JDBC Sink connectors for Postgres raw & enriched tables
make connectors
```

### Run Tests & Generate Enterprise Reports
```bash
# Run unit tests only
make test-unit

# Run integration tests only
make test-integration

# Run benchmark tests
make test-bench

# Run full test suite with HTML Report & Coverage generation
make test-report

# Run k6 Load Test Scenario (requires services running)
make test-load
```

---

## 📊 Summary of Generated Test Artifacts

When executing `make test-report`, the platform generates the following output in `reports/`:
- **`reports/report.html`**: Executive dashboard with pass/fail ratios, unit test breakdown, integration status, benchmark metrics table, and code coverage bar.
- **`reports/unit/coverage.html`**: Interactive Go coverage document detailing statement execution across internal packages.
- **`reports/unit/results.txt`**: Detailed test logs for unit tests.
- **`reports/integration/results.txt`**: Execution logs for integration scenarios.
- **`reports/benchmark/results.txt`**: Performance statistics (`ns/op`, `B/op`, `allocs/op`).
