# Event Platform Execution & Testing Documentation

This document centrally manages all commands required to initialize, test, benchmark, and operate the Event Platform microservices.

---

## 🏗 System Architecture & Dedicated Ports

All services are bound to unique host ports in the **`274xx` port range** declared centrally in [`event-platform/infra/.env`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/infra/.env).

### Centralized Host Port Declarations

| Microservice / Component | Centralized Host Port | Protocol | Purpose / Route |
|---|---|---|---|
| **Traefik Ingress Gateway (HTTP)** | `27488` | HTTP | Edge EntryPoint (`api.scaibu.localhost`, `grafana.scaibu.localhost`) |
| **Traefik Ingress Gateway (HTTPS)** | `27443` | HTTPS | Secured TLS EntryPoint |
| **Ingestion API** | `27480` (Internal `8080`) | HTTP | Event Ingestion Cluster |
| **PostgreSQL 17 Database** | `27432` (Internal `5432`) | TCP | Relational Persistent Storage (`app` / `Scaibu@123`) |
| **Redis 7 Cache** | `27479` (Internal `6379`) | RESP | Deduplication Cache (`dedup:<event_id>`) |
| **Apache Kafka Broker** | `27492` (Internal `9092`) | PLAINTEXT | KRaft Event Broker |
| **Schema Registry** | `27481` (Internal `8081`) | HTTP | Confluent Avro Schema Manager |
| **Kafka Connect** | `27483` (Internal `8083`) | HTTP | JDBC Sink Connector Cluster |
| **Grafana Dashboards** | `27402` (Internal `3000`) | HTTP | Observability & Trace Dashboards (`admin` / `Scaibu@123`) |
| **Prometheus Metrics** | `27490` (Internal `9090`) | HTTP | Metrics Aggregation Engine |
| **OpenTelemetry Collector** | `27417` (gRPC) / `27418` (HTTP) | OTLP | Multi-tenant Trace Collector |

---

## 🚀 Environment Initialization Command

To destroy previous state cleanly (without killing external host processes) and deploy all 11 microservices:

```bash
# Execute from workspace root
./event-platform/scripts/setup-dev-environment.sh
```

---

## 🧪 Comprehensive Testing & Load Commands

### 1. Pytest Functional Test Suite & Report Generation
Runs end-to-end assertions against all HTTP routes, event types, deduplication rules, and schema validations:

```bash
cd event-platform
pytest loadtest/test_ingestion_pipeline.py -v --alluredir=loadtest/allure-results
```

### 2. High-Throughput 10,000 Request Load Test (k6)
Executes a high-concurrency load test pushing 10,000 events through the Traefik Ingress Gateway:

```bash
docker run --rm -it \
  --add-host host.docker.internal:host-gateway \
  -v $(pwd)/event-platform/loadtest:/scripts \
  grafana/k6:latest run \
  --summary-export=/scripts/k6-results-10k.json \
  /scripts/traefik_integration.ts
```

### 3. Allure Interactive Report Generation & Server
To compile raw test results into an interactive HTML dashboard:

```bash
# Generate static HTML report
allure generate event-platform/loadtest/allure-results -o event-platform/loadtest/allure-report --clean

# Serve interactive report in browser
allure serve event-platform/loadtest/allure-results -p 8088
```

---

## 📄 Test Report Structure & Analytical Metrics

When executing test suites, the generated reports include:

### 1. Functional Assertions
- **Route Validation**: Asserts `HTTP 200 OK` on `/healthz` and `HTTP 202 Accepted` on `/v1/events`.
- **Validation Rejection**: Asserts `HTTP 422 Unprocessable Entity` for missing `event_id` or unregistered `event_type`.
- **Atomic Deduplication**: Asserts `HTTP 202` on initial event write and `HTTP 200` on duplicate attempt within 24h.

### 2. Performance & Throughput Metrics (k6 Load Test)
- **Requests Per Second (RPS)**: Measured throughput sustained by Traefik and Go Ingestion API.
- **Latency Percentiles**: `p(50)`, `p(90)`, `p(95)`, and `p(99)` latency metrics (target: `p(95) < 200ms`).
- **Error & Drop Rates**: Counts `http_req_failed`, HTTP 429 rate-limited requests, and circuit breaker activations.

---

## 🛑 Operational Troubleshooting & Quick Checks

### Service Health Checks
```bash
# Ingestion API via Traefik
curl -i -H "Host: api.scaibu.localhost" http://localhost:27488/healthz

# Grafana API via Traefik
curl -i -H "Host: grafana.scaibu.localhost" -u admin:Scaibu@123 http://localhost:27488/api/health

# Schema Registry Subjects
curl -s http://localhost:27481/subjects

# Kafka Connect Active Connectors
curl -s http://localhost:27483/connectors
```
