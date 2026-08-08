#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   EVENT PLATFORM — DEVELOPER ENVIRONMENT SETUP & MIGRATIONS ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

echo "1. Checking Prerequisites..."
for tool in docker git curl; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "❌ Error: '$tool' is required but not installed."
        exit 1
    fi
done
echo "   ✅ Docker, Git, Curl available."
echo ""

echo "2. Building & Starting Infrastructure Services..."
cd "$PROJECT_ROOT"
docker compose -f infra/docker-compose.yml up -d --build
echo "   ✅ Infrastructure containers created."
echo ""

echo "3. Waiting for PostgreSQL to be Healthy..."
until docker exec event-platform-postgres-1 pg_isready -U app -d app >/dev/null 2>&1; do
    sleep 2
done
echo "   ✅ PostgreSQL is ready."
echo ""

echo "4. Running Database Schema Migrations..."
docker exec -i event-platform-postgres-1 psql -U app -d app < "$PROJECT_ROOT/infra/postgres/migrations/001_init_schema.sql"
echo "   ✅ PostgreSQL schema migration applied."
echo ""

echo "5. Waiting for Kafka Broker to be Healthy..."
until docker exec event-platform-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do
    sleep 2
done
echo "   ✅ Kafka Broker is ready."
echo ""

echo "6. Provisioning Kafka Topics & Schema Registry..."
docker exec event-platform-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events.raw --partitions 12 --replication-factor 1 --if-not-exists
docker exec event-platform-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events.enriched --partitions 12 --replication-factor 1 --if-not-exists

until curl -s http://localhost:8081/subjects >/dev/null; do
    sleep 2
done
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"RawEvent\",\"namespace\":\"com.platform.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"occurred_at\",\"type\":\"long\"},{\"name\":\"payload\",\"type\":\"string\"}]}"}' http://localhost:8081/subjects/events.raw-value/versions >/dev/null
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\":\"record\",\"name\":\"EnrichedCount\",\"namespace\":\"com.platform.events\",\"fields\":[{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"window_start\",\"type\":\"long\"},{\"name\":\"event_count\",\"type\":\"long\"}]}"}' http://localhost:8081/subjects/events.enriched-value/versions >/dev/null
echo "   ✅ Kafka topics and Avro Schemas registered in Schema Registry."
echo ""

echo "7. Registering Kafka Connect Sinks..."
until curl -s http://localhost:8083/connectors >/dev/null; do
    sleep 3
done
curl -s -X POST -H "Content-Type: application/json" --data @"$PROJECT_ROOT/infra/kafka/connectors/postgres-raw-sink.json" http://localhost:8083/connectors || true
curl -s -X POST -H "Content-Type: application/json" --data @"$PROJECT_ROOT/infra/kafka/connectors/postgres-enriched-sink.json" http://localhost:8083/connectors || true
echo ""
echo "   ✅ Kafka Connect sink connectors registered."
echo ""

echo "══════════════════════════════════════════════════════════════"
echo "🎉 Setup Complete! Active Services & Web Endpoints:"
echo "   - Ingestion API:     http://localhost:8080/healthz"
echo "   - pprof Profiling:   http://localhost:6060/debug/pprof/"
echo "   - Grafana Dashboard: http://localhost:3002"
echo "   - Prometheus UI:     http://localhost:9090"
echo "   - Schema Registry:   http://localhost:8081"
echo "   - Kafka Connect API: http://localhost:8083"
echo "══════════════════════════════════════════════════════════════"
