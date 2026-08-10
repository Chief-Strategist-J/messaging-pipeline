# High-Throughput Event Ingestion & Stream Processing Pipeline — Architecture & Implementation

A production-grade, enterprise event processing platform built to sustain **1,000,000 requests in 10 minutes** (1,667 req/s sustained average, with 3x peak headroom at 5,000 req/s) on **minimum compute resources**.

---

## 🚀 Key Accomplishments & Architectural Deliverables

This repository contains the complete implementation of a multi-tier, data-driven event platform built following strict performance engineering principles, domain-driven boundaries, and enterprise quality rules (DRY, SRP, explicit typing, zero magic strings).

### 1. Traefik v3 Single Gateway & Ingress Layer
- **Traefik v3.7.10**: Functions as the single HTTP/HTTPS entry point into the system using sub-domain based routing (`api.scaibu.localhost` and `grafana.scaibu.localhost`).
- **Dynamic Service Discovery**: Discovers `ingestion-api` replicas dynamically via Docker socket labels without hardcoding IP addresses or container ports.
- **Request Management**: Enforces rate limiting (200 req/s per IP avg, 500 burst), payload body size limits (10MB max), connection timeouts, conservative retry policies (connection failures only), and circuit breaking (>50% failure threshold).
- **Security**: Insecure dashboard disabled (`insecure=false`), Docker socket mounted read-only (`:ro`), custom security headers applied, and basic authentication required for Grafana and internal admin endpoints.

### 2. Dual-Tier Multi-Language Microservices
- **Ingestion API (Go)**: Built using light-weight goroutines (~2KB overhead each) compiled to a pure static binary inside a distroless container image (~15MB total). Leverages `franz-go` (pure Go, zero `cgo`) to achieve maximum CPU/memory efficiency and ultra-fast LZ4 compressed batching to Kafka.
- **Stream Processing Engine (Kotlin/JVM)**: Uses Kafka Streams DSL for exact-once processing guarantees (`EXACTLY_ONCE_V2`), windowed aggregations, and stateful transformations across stream partitions.

### 3. Fully Data-Driven & Extensible Design
- **Event Rules as Data**: New event types, field validation schemas, routing topics, and payload constraints are declared in [`config/event-types.yaml`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/config/event-types.yaml). Adding new event types requires **zero code changes** or recompilation of the HTTP ingestion API.
- **Topology as Data**: Kafka Stream processing steps and windowed aggregate definitions are built dynamically from immutable data definitions at startup, paying zero per-record runtime overhead.

### 4. Comprehensive Infrastructure & Container Automation ([docker-compose.yml](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/infra/docker-compose.yml))
Orchestrates 12 enterprise services:
- **Ingress Tier**: Traefik v3.7.10
- **Stateless Tier**: Ingestion API (dynamic scaling), Stream Processor
- **Event Backbone**: Apache Kafka (KRaft mode, 12 partitions for optimal parallelism), Confluent Schema Registry (Avro binary wire format serialization)
- **Persistence & Cache**: Redis (low-latency atomic `SETNX` deduplication with 24h TTL), PostgreSQL (relational sink for raw events & enriched aggregate counts)
- **Data Integration**: Kafka Connect with Confluent JDBC Sink (`upsert` mode for idempotent Postgres delivery)
- **Full Observability Stack**: OpenTelemetry Collector, Grafana Tempo (distributed tracing), Prometheus (metrics collection), Grafana (unified dashboards)
- **Centralized Environment Management**: All ports, replicas, limits, and service credentials extracted into [`infra/.env`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/infra/.env).

---

## 🛠 Required Tools & Dependencies

Before running setup scripts, ensure the following dependencies are installed:

| Tool | Minimum Version | Installation Command |
|---|---|---|
| **Docker Engine** | 20.10+ | `curl -fsSL https://get.docker.com \| sh` |
| **cURL** | Any | `sudo apt install curl` or `brew install curl` |
| **wget** | Any | `sudo apt install wget` or `brew install wget` |
| **htpasswd** | Any | `sudo apt install apache2-utils` or `brew install httpd` |
| **OpenSSL** | 1.1.1+ | `sudo apt install openssl` or `brew install openssl` |
| **k6** (Optional) | 0.40+ | `sudo apt install k6` or `brew install k6` |

---

## ⚡ Zero-Downtime Fresh Environment Setup (`scripts/setup-dev-environment.sh`)

The setup script guarantees a **100% clean environment execution**. Every time `bash scripts/setup-dev-environment.sh` or `make up` is called:
1. **Prerequisites Check**: Verifies all required CLI tools are present.
2. **Environment Recreation**: Completely wipes and recreates [`infra/.env`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/infra/.env) with fresh randomly generated passwords and configuration.
3. **Nuke Existing State**: Stops all containers, removes project volumes, removes networks, and purges dangling build images.
4. **Port Verification**: Verifies dedicated unique ports (`27488, 27443, 27432, 27479, 27492, 27493, 27481, 27483, 27417, 27418, 27490, 27402, 27480`) are free without killing non-project system services.
5. **Storage & Cert Initialization**: Re-creates `acme.json` with strict `600` permissions and generates a self-signed TLS cert.
6. **Hosts Entry**: Configures local hostnames in `/etc/hosts`.
7. **Infrastructure Up**: Rebuilds and launches containers.
8. **Automated Migrations**: Applies PostgreSQL schema migrations, Kafka topic creations (12 partitions), Avro schema registrations, and Kafka Connect sink initializations.

---

## 🛠 Command Reference & Usage

```bash
# Clean setup (wipes volumes/ports, recreates .env, starts everything)
bash scripts/setup-dev-environment.sh

# Scale API replicas dynamically behind Traefik
make scale REPLICAS=4

# Run unit tests
make test-unit

# Run integration tests
make test-integration

# Run benchmark tests
make test-bench

# Run k6 progressive load test (100 -> 10,000 RPS)
make test-load

# Run sustained 1,667 RPS load test (5 mins)
make test-load-sustained

# Run full test suite & generate interactive HTML report
make test-report
```

---

## 🔒 Certificate Generation & TLS Commands

For manual TLS certificate generation or Let's Encrypt production certificates:

### 1. Manual Self-Signed Certificate Generation (Local Dev)
```bash
openssl req -x509 -nodes -days 365 \
  -newkey rsa:2048 \
  -keyout infra/traefik/certs/local-selfsigned.key \
  -out infra/traefik/certs/local-selfsigned.crt \
  -subj "/C=IN/ST=KA/L=Bangalore/O=scaibu/CN=*.scaibu.localhost" \
  -addext "subjectAltName=DNS:*.scaibu.localhost,DNS:scaibu.localhost,DNS:localhost"

chmod 600 infra/traefik/certs/local-selfsigned.key
```

### 2. ACME / Let's Encrypt Permissions Initialization
Traefik requires strict `600` file permissions for storing ACME JSON certificates:
```bash
touch infra/traefik/acme/acme.json
chmod 600 infra/traefik/acme/acme.json
```

---

## 🗺 Docker to Kubernetes Migration Map

| Docker / Traefik Component | Kubernetes / Gateway API Equivalent |
|---|---|
| Traefik Docker Provider Labels | Kubernetes Gateway API `HTTPRoute` or `IngressRoute` CRD |
| `Host("api.scaibu.com")` | `HTTPRoute.spec.hostnames: ["api.scaibu.com"]` |
| `loadbalancer.server.port=8080` | `Service.spec.ports[].targetPort: 8080` |
| `docker compose scale ingestion-api=N` | `Deployment.spec.replicas: N` |
| Container `/healthz` check | `readinessProbe` & `livenessProbe` |
| Docker `backbone` network | Kubernetes `Service` (ClusterIP DNS) |
| Environment File (`.env`) | Kubernetes `ConfigMap` & `Secret` |
| Traefik ACME configuration | `cert-manager` with `ClusterIssuer` |
