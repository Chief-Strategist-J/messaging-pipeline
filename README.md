# High-Throughput Event Ingestion & Stream Processing Pipeline

A production-grade, enterprise event processing platform built to sustain high-throughput ingestion and real-time stream processing on minimum compute resources.

---

## 🏗 Architectural Overview & System Flow

```mermaid
flowchart TD
    subgraph Load Generator
        K6["k6 Load Test Container\n(167 req/s, 500KB payload)"]
    end

    subgraph Event Platform Infrastructure
        API["Go Ingestion API\n(:8080)\n[4.0 CPU / 1GB RAM]"]
        REDIS[("Redis Cache\n(:6379)\n[Deduplication]")]
        KAFKA["Apache Kafka Broker\n(:9092, KRaft Mode)\n[Topic: events.raw]"]
        SCHEMA["Schema Registry\n(:8081)\n[Avro Schema v1]"]
        CONNECT["Kafka Connect JDBC Sink\n(:8083)"]
        POSTGRES[("PostgreSQL DB\n(:5432)\n[Table: raw_events]")]
        STREAMS["Kotlin Kafka Streams\n[Stream Processor]"]
    end

    subgraph Observability
        OTEL["OpenTelemetry Collector\n(:4317 / :4318)"]
        PROMETHEUS["Prometheus\n(:9090)"]
        TEMPO["Grafana Tempo\n[Distributed Tracing]"]
        GRAFANA["Grafana Dashboards\n(:3002)"]
    end

    %% Data Flow Connections
    K6 -->|1. POST /v1/events 500KB JSON| API
    API -->|2. Check/Set Key dedup:event_id| REDIS
    API -->|3. Fetch Avro Schema #1| SCHEMA
    API -->|4. Publish LZ4 Avro Batch| KAFKA
    KAFKA -->|5. Consume events.raw| CONNECT
    CONNECT -->|6. Upsert Rows| POSTGRES
    KAFKA -->|7. Consume events.raw| STREAMS

    %% Observability Connections
    API -.->|OTel Spans| OTEL
    OTEL -.->|Traces| TEMPO
    OTEL -.->|Metrics| PROMETHEUS
    PROMETHEUS -.->|Visualize| GRAFANA
    TEMPO -.->|Visualize| GRAFANA
```

---

## ⚡ Core Features & Performance Engineering

- **High-Throughput Ingestion API (Go)**: Compiled to a distroless static binary (~15MB), using `franz-go` for LZ4 compressed batching to Kafka.
- **Zero-Unescape Envelope Decoding (`json.RawMessage`)**: Optimized payload envelope decoding to avoid string unquoting overhead for 500KB payloads (`ns/op` reduced by 5.3x on benchmarks).
- **Sub-Millisecond Deduplication (Redis)**: Atomic `SET NX` checks on `dedup:<event_id>` with 24h TTL short-circuiting duplicates with HTTP 200.
- **Stream Processing Engine (Kotlin/JVM)**: Uses Kafka Streams DSL with `EXACTLY_ONCE_V2` processing for windowed aggregations.
- **Data-Driven Architecture**: Event schemas, field validation rules, and routing declared declaratively in [`event-platform/config/event-types.yaml`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/config/event-types.yaml).
- **Complete Observability Stack**: OpenTelemetry Collector, Grafana Tempo (distributed tracing), Prometheus, Grafana, and Allure HTML report publishing.

---

## 📊 Live Performance & Benchmark Metrics

### Envelope Decoding Optimization Benchmark (`BenchmarkDecodeRawEvent_500KB`)
| Version | Benchmark (`ns/op`) | Memory Allocations (`B/op`) | Heap Allocations (`allocs/op`) |
|---|---|---|---|
| **Before (`string` Payload)** | `4,858,079 ns/op` (4.85 ms) | `1,540,653 B/op` | `48 allocs/op` |
| **After (`json.RawMessage`)** | **`914,942 ns/op` (0.91 ms)** | **`524,800 B/op`** | **`18 allocs/op`** |
| **Performance Gain** | **5.3x Faster** | **66% Memory Savings** | **62.5% Allocation Drop** |

---

## 🌐 Allure HTML Report Access
- **Interactive Report URL:** [http://localhost:8088/allure_report_single.html/index.html](http://localhost:8088/allure_report_single.html/index.html)
- **Single-File HTML Path:** [`event-platform/reports/allure_report_single.html/index.html`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/allure_report_single.html/index.html)


