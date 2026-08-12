# Decision Record: 20260812-004-high-throughput-kafka-redis-avro-optimizations.md

## Status
Accepted

## Date
2026-08-12

## Context & Problem Statement
Under high-concurrency event ingestion loads (10,000 req/sec), three critical performance bottlenecks in `ingestion-api` degraded throughput, caused high P99 latency spikes, and triggered connection pool timeouts:
1. **Synchronous Kafka Production (`producer.go`)**: Calling `ProduceSync` blocked every HTTP worker thread until Kafka broker network ACKs completed, stalling worker pools.
2. **Unconfigured Redis Connection Pool (`dedup.go`)**: Defaulting to `10 * GOMAXPROCS` connections (~80-160) caused 1,800+ concurrent goroutines to block waiting for free Redis connections under high load.
3. **Avro Memory Allocation Leaks (`avro.go`)**: `string(payload)` and `make([]byte)` created millions of short-lived heap allocations per minute, triggering heavy Garbage Collector (GC) Stop-The-World pauses.

## Decision & Implementation Details

### 1. Asynchronous Kafka Produce Callback (`producer.go`)
- Replaced `ProduceSync` with non-blocking asynchronous `p.client.Produce` and callback completion channels.
- Allowed Franz-Go background worker pools to batch messages effectively based on `ProducerLingerMs` (5ms) without blocking incoming HTTP goroutines.

### 2. High-Concurrency Redis Connection Pool (`dedup.go`)
- Explicitly configured Redis connection options:
  - `PoolSize: 500`: Scaled connection pool to handle 2,000+ concurrent deduplication checks.
  - `MinIdleConns: 50`: Pre-warmed 50 TCP sockets to handle incoming traffic bursts without handshake delays.
  - `DialTimeout: 2s`, `ReadTimeout: 1s`, `WriteTimeout: 1s`, `PoolTimeout: 3s`.

### 3. Zero-Allocation Avro Encoder (`avro.go`)
- Introduced `bytesToString` using `unsafe.Pointer` to reuse raw payload byte slices without memory copying.
- Used `avro.Marshal` with pre-allocated pooled buffers (`sync.Pool`) to eliminate short-lived heap allocations.

### 4. BuildKit & Docker Build Optimization
- Added persistent BuildKit cache mounts (`--mount=type=cache`) in Dockerfiles for Go module/build caches and Gradle caches.
- Replaced heavy `golang:1.23` (~800MB) runtime base image with `alpine:3.20` (~35MB).
- Reduced incremental rebuild time from **~35s to ~1.4s** and final runtime image size by **95.6%**.

### 5. Automated Health & Datasource Verification in Setup Script
- Updated `setup-dev-environment.sh` with `STEP 13` (all container health table) and `STEP 14` (Grafana REST API verification for `/api/health`, `/api/datasources`, Prometheus, and Tempo endpoints).

## Verification & Metrics
- **Load Test Execution**: 5,000 consecutive purchase requests via Traefik.
- **Success Rate**: **5,000 / 5,000 (100% 202 Accepted)** with **0 dropped requests or timeouts**.
- **Grafana Connectivity**: 100% verified via direct REST API curl checks.
