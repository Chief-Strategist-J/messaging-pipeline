# Event Platform Infrastructure & Services

This directory contains the complete microservice source code, infrastructure declarations, database migrations, load tests, and operational setup scripts for the **Event Processing Platform**.

---

## 📄 Complete Architectural Reference

For complete system topology diagrams, end-to-end event sequence flows, Traefik security middlewares, PostgreSQL ERD schema definitions, distributed tracing guides, and port maps, please visit:

👉 **[Root Architecture & Engineering Reference (README.md)](file:///home/btpl-lap-22/live/messaging-pipeline/README.md)**

---

## 📂 Directory Structure

```
event-platform/
├── config/                  # Event type configuration declarations (event-types.yaml)
├── infra/                   # Infrastructure definitions (docker-compose, Traefik, Prometheus, Kafka Connectors)
│   ├── kafka/               # Sink connector templates (postgres-raw-sink, postgres-enriched-sink)
│   ├── postgres/            # Database initialization SQL migrations
│   ├── prometheus/          # Prometheus metric scraping configurations
│   └── traefik/             # Traefik routing rules, middleware definitions, and certificates
├── loadtest/                # k6 load testing scripts (traefik_integration.ts)
├── schemas/                 # Confluent Avro schemas (event-raw.avsc, event-enriched.avsc)
├── scripts/                 # Operational automation scripts (setup-dev-environment.sh, run-tests.sh)
└── services/                # Core application service microservices
    ├── ingestion-api/       # Go 1.22 high-performance HTTP ingestion service & Redis deduplication
    └── stream-processor/    # Kotlin Kafka Streams 1-minute window aggregation engine
```

---

## ⚡ Essential Commands

### 1. Environment Setup & Provisioning
Spins up all containers, runs database migrations, provisions Kafka topics, registers Avro schemas, and initializes sink connectors:

```bash
./scripts/setup-dev-environment.sh
```

### 2. Test Execution
Runs unit tests, integration tests, and benchmark checks:

```bash
./scripts/run-tests.sh
```
