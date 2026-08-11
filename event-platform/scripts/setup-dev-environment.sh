#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# CONSTANTS & GLOBAL CONFIGURATION
# ==============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"
COMPOSE_FILE="$INFRA_DIR/docker-compose.yml"
ENV_FILE="$INFRA_DIR/.env"
ACME_FILE="$INFRA_DIR/traefik/acme/acme.json"
MIGRATION_SQL="$INFRA_DIR/postgres/migrations/001_init_schema.sql"
RAW_SINK_CONFIG="$INFRA_DIR/kafka/connectors/postgres-raw-sink.json"
ENRICHED_SINK_CONFIG="$INFRA_DIR/kafka/connectors/postgres-enriched-sink.json"

PROJECT_NAME="event-platform"
KAFKA_BOOTSTRAP="localhost:9092"
SCHEMA_REGISTRY_URL="http://localhost:27481"
KAFKA_CONNECT_URL="http://localhost:27483"
POSTGRES_CONTAINER="${PROJECT_NAME}-postgres-1"
KAFKA_CONTAINER="${PROJECT_NAME}-kafka-1"

TOPIC_EVENTS_RAW="events.raw"
TOPIC_EVENTS_ENRICHED="events.enriched"
TOPIC_PARTITIONS=12
TOPIC_REPLICATION_FACTOR=1

RAW_AVRO_SCHEMA='{\"type\":\"record\",\"name\":\"RawEvent\",\"namespace\":\"com.platform.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"occurred_at\",\"type\":\"long\"},{\"name\":\"payload\",\"type\":\"string\"}]}'
ENRICHED_AVRO_SCHEMA='{\"type\":\"record\",\"name\":\"EnrichedCount\",\"namespace\":\"com.platform.events\",\"fields\":[{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"window_start\",\"type\":\"long\"},{\"name\":\"event_count\",\"type\":\"long\"}]}'

REQUIRED_PORTS=(27488 27443 27432 27479 27492 27493 27481 27483 27417 27418 27490 27402 27480)
REQUIRED_TOOLS=(docker curl wget htpasswd openssl)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
log()  { echo "  $1"; }
ok()   { echo "  ✅ $1"; }
fail() { echo "  ❌ $1"; exit 1; }
step() { echo ""; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; echo "  $1"; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }

retry_cmd() {
    local label="$1"
    local attempts="$2"
    local delay="$3"
    shift 3
    local count=1

    until "$@"; do
        if [ "$count" -ge "$attempts" ]; then
            fail "$label failed after $attempts attempts."
        fi
        log "⚠️  $label failed (attempt $count/$attempts). Retrying in ${delay}s..."
        sleep "$delay"
        count=$((count + 1))
    done
}

wait_for() {
    local name="$1"
    local cmd="$2"
    local delay="${3:-3}"
    local retries="${4:-30}"
    local count=0
    log "Waiting for $name..."
    until eval "$cmd" >/dev/null 2>&1; do
        count=$((count + 1))
        if [ "$count" -ge "$retries" ]; then
            fail "$name did not become ready after $((retries * delay))s"
        fi
        sleep "$delay"
    done
    ok "$name is ready"
}

# ==============================================================================
# EXECUTION STEPS
# ==============================================================================
check_prerequisites() {
    step "STEP 1 — Checking prerequisites"

    for tool in "${REQUIRED_TOOLS[@]}"; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            echo ""
            echo "  ❌ Missing required tool: $tool"
            echo ""
            case "$tool" in
                docker)    echo "     Install: https://docs.docker.com/engine/install/" ;;
                curl)      echo "     Install: sudo apt install curl   OR   brew install curl" ;;
                wget)      echo "     Install: sudo apt install wget   OR   brew install wget" ;;
                htpasswd)  echo "     Install: sudo apt install apache2-utils   OR   brew install httpd" ;;
                openssl)   echo "     Install: sudo apt install openssl   OR   brew install openssl" ;;
            esac
            echo ""
            exit 1
        fi
    done

    if ! docker info >/dev/null 2>&1; then
        fail "Docker daemon is not running. Start Docker and retry."
    fi

    ok "All required tools present and Docker daemon is running"
}

recreate_env_file() {
    step "STEP 2 — Recreating infra/.env clean file"

    log "Wiping and generating clean $ENV_FILE from scratch..."
    local admin_pass="Scaibu@123"
    local bcrypt_hash
    bcrypt_hash=$(htpasswd -nbB admin "$admin_pass" | sed 's/\$/\$\$/g')

    cat > "$ENV_FILE" <<EOF
COMPOSE_PROJECT_NAME=event-platform

POSTGRES_DB=app
POSTGRES_USER=app
POSTGRES_PASSWORD=${admin_pass}
POSTGRES_PORT=27432

REDIS_PORT=27479

KAFKA_NODE_ID=1
KAFKA_INTERNAL_PORT=27492
KAFKA_CONTROLLER_PORT=27493
KAFKA_OFFSETS_REPLICATION_FACTOR=1

SCHEMA_REGISTRY_PORT=27481

CONNECT_PORT=27483
CONNECT_GROUP_ID=connect-cluster
CONNECT_CONFIG_TOPIC=connect_configs
CONNECT_OFFSET_TOPIC=connect_offsets
CONNECT_STATUS_TOPIC=connect_statuses

INGESTION_API_PORT=27480
INGESTION_API_REPLICAS=4
INGESTION_API_CPU_LIMIT=4.0
INGESTION_API_MEM_LIMIT=1G

OTEL_GRPC_PORT=27417
OTEL_HTTP_PORT=27418

PROMETHEUS_PORT=27490

GRAFANA_HOST_PORT=27402
GRAFANA_CONTAINER_PORT=3000
GF_AUTH_ANONYMOUS_ENABLED=true
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=${admin_pass}

TRAEFIK_HTTP_PORT=27488
TRAEFIK_HTTPS_PORT=27443
TRAEFIK_IMAGE=traefik:v3.7.10
TRAEFIK_LOG_LEVEL=INFO
DOMAIN_SUFFIX=scaibu.localhost
API_HOST=api.scaibu.localhost
GRAFANA_HOST=grafana.scaibu.localhost
TRAEFIK_ACME_EMAIL=ops@scaibu.com
API_RATE_LIMIT_AVERAGE=2500
API_RATE_LIMIT_BURST=5000
API_MAX_REQUEST_BODY_BYTES=10485760
TRUSTED_IPS=127.0.0.1/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
GRAFANA_TRAEFIK_BASICAUTH=${bcrypt_hash}
TRAEFIK_DIAL_TIMEOUT=5s
TRAEFIK_RESPONSE_HEADER_TIMEOUT=30s
TRAEFIK_IDLE_CONN_TIMEOUT=90s
EOF

    ok ".env file recreated successfully"
}

nuke_everything() {
    step "STEP 3 — Destroying existing runtime state (volumes, networks, containers)"

    log "Stopping and removing project containers, volumes, and networks..."
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" down \
        --volumes \
        --remove-orphans \
        2>/dev/null || true

    log "Force-removing any lingering project containers..."
    docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" \
        | xargs -r docker rm -f 2>/dev/null || true

    log "Removing project networks..."
    docker network ls --filter "name=${PROJECT_NAME}" -q \
        | xargs -r docker network rm 2>/dev/null || true

    log "Removing project volumes..."
    docker volume ls --filter "name=${PROJECT_NAME}" -q \
        | xargs -r docker volume rm -f 2>/dev/null || true

    ok "Previous runtime state destroyed (images preserved)"
}

free_ports() {
    step "STEP 4 — Verifying required ports are free"

    local conflicted_ports=()
    for port in "${REQUIRED_PORTS[@]}"; do
        local pids
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            log "Port $port is in use by PID(s): $pids"
            conflicted_ports+=("$port")
        fi
    done

    if [ "${#conflicted_ports[@]}" -gt 0 ]; then
        fail "The following unique project ports are still occupied: ${conflicted_ports[*]}. Please free them before running setup."
    fi

    ok "All required dedicated ports are free: ${REQUIRED_PORTS[*]}"
}

init_traefik_storage() {
    step "STEP 5 — Initialising Traefik certificate storage and dynamic configs"

    mkdir -p "$INFRA_DIR/traefik/acme"
    mkdir -p "$INFRA_DIR/traefik/certs"
    mkdir -p "$INFRA_DIR/traefik/dynamic"

    echo "{}" > "$ACME_FILE"
    chmod 600 "$ACME_FILE"

    ok "acme.json initialised with chmod 600 at $ACME_FILE"

    log "Generating self-signed certificate for local HTTPS testing..."
    openssl req -x509 -nodes -days 365 \
        -newkey rsa:2048 \
        -keyout "$INFRA_DIR/traefik/certs/local-selfsigned.key" \
        -out    "$INFRA_DIR/traefik/certs/local-selfsigned.crt" \
        -subj "/C=IN/ST=KA/L=Bangalore/O=scaibu/CN=*.scaibu.localhost" \
        -addext "subjectAltName=DNS:*.scaibu.localhost,DNS:scaibu.localhost,DNS:localhost" \
        2>/dev/null
    chmod 600 "$INFRA_DIR/traefik/certs/local-selfsigned.key"
    ok "Self-signed certificate generated at $INFRA_DIR/traefik/certs/"

    log "Rendering dynamic $INFRA_DIR/traefik/dynamic/middlewares.yml from template..."
    if [ -f "$ENV_FILE" ]; then
        set -a
        source "$ENV_FILE"
        set +a
    fi

    local avg="${API_RATE_LIMIT_AVERAGE:-2500}"
    local burst="${API_RATE_LIMIT_BURST:-5000}"
    local body_limit="${API_MAX_REQUEST_BODY_BYTES:-10485760}"
    local raw_auth
    raw_auth=$(htpasswd -nbB admin "Scaibu@123")

    cat "$INFRA_DIR/traefik/dynamic/middlewares.yml.template" \
        | sed "s|\${API_RATE_LIMIT_AVERAGE}|$avg|g" \
        | sed "s|\${API_RATE_LIMIT_BURST}|$burst|g" \
        | sed "s|\${API_MAX_REQUEST_BODY_BYTES}|$body_limit|g" \
        | sed "s|\${GRAFANA_TRAEFIK_BASICAUTH}|$raw_auth|g" \
        > "$INFRA_DIR/traefik/dynamic/middlewares.yml"

    ok "Dynamic middlewares.yml rendered from template"
}

configure_hosts() {
    step "STEP 6 — Checking /etc/hosts for local development"

    local hosts_entry="127.0.0.1 api.scaibu.localhost grafana.scaibu.localhost traefik.scaibu.localhost"

    if grep -q "api.scaibu.localhost" /etc/hosts 2>/dev/null; then
        ok "Hosts entries already present in /etc/hosts"
    else
        log "Host entries missing in /etc/hosts."
        log "To use local domain names, run:"
        log "  echo \"$hosts_entry\" | sudo tee -a /etc/hosts"
        ok "Skipped non-interactive sudo /etc/hosts modification"
    fi
}

build_and_start() {
    step "STEP 7 — Building and starting all services"

    cd "$PROJECT_ROOT"
    log "Checking missing base images before pulling..."
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" config --images 2>/dev/null | sort -u | while read -r img; do
        if [ -n "$img" ]; then
            if ! docker image inspect "$img" >/dev/null 2>&1; then
                log "Base image missing locally ($img). Pulling..."
                docker pull "$img" || true
            else
                log "Image exists locally: $img (skipping pull)"
            fi
        fi
    done

    retry_cmd "Docker Compose startup" 3 5 docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --no-build
    if [ $? -ne 0 ]; then
        log "Attempting docker compose build for local microservices..."
        retry_cmd "Docker Compose build and startup" 3 5 docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --build
    fi

    ok "All containers started"
}

wait_for_core_services() {
    step "STEP 8 — Waiting for core services to become healthy"

    wait_for "PostgreSQL" \
        "docker exec ${POSTGRES_CONTAINER} pg_isready -U app -d app" 2 30

    wait_for "Kafka Broker" \
        "docker exec ${KAFKA_CONTAINER} /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092" 3 30

    wait_for "Traefik" \
        "curl -sf http://localhost:8899/ping" 2 30

    wait_for "ingestion-api (via Traefik)" \
        "curl -sf -H 'Host: api.scaibu.localhost' http://localhost:27488/healthz" 3 30

    wait_for "Grafana (via Traefik)" \
        "curl -sf -o /dev/null -w '%{http_code}' -H 'Host: grafana.scaibu.localhost' http://localhost:27488/ -u admin:Scaibu@123 | grep -E '200|302'" 3 30
}

migrate_postgres() {
    step "STEP 9 — Running database migrations"

    if [ ! -f "$MIGRATION_SQL" ]; then
        log "No migration file found at $MIGRATION_SQL — skipping"
        return
    fi

    retry_cmd "PostgreSQL schema migration" 5 3 bash -c "docker exec -i '$POSTGRES_CONTAINER' psql -U app -d app < '$MIGRATION_SQL'"
    ok "PostgreSQL schema migration applied"
}

provision_kafka_topics() {
    step "STEP 10 — Provisioning Kafka topics"

    retry_cmd "Kafka topic creation ($TOPIC_EVENTS_RAW)" 5 3 \
        docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server "$KAFKA_BOOTSTRAP" \
            --create --topic "$TOPIC_EVENTS_RAW" \
            --partitions "$TOPIC_PARTITIONS" \
            --replication-factor "$TOPIC_REPLICATION_FACTOR" \
            --if-not-exists

    retry_cmd "Kafka topic creation ($TOPIC_EVENTS_ENRICHED)" 5 3 \
        docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server "$KAFKA_BOOTSTRAP" \
            --create --topic "$TOPIC_EVENTS_ENRICHED" \
            --partitions "$TOPIC_PARTITIONS" \
            --replication-factor "$TOPIC_REPLICATION_FACTOR" \
            --if-not-exists

    ok "Kafka topics provisioned: $TOPIC_EVENTS_RAW, $TOPIC_EVENTS_ENRICHED"
}

register_avro_schemas() {
    step "STEP 11 — Registering Avro schemas in Schema Registry"

    wait_for "Schema Registry" "curl -sf $SCHEMA_REGISTRY_URL/subjects" 2 30

    retry_cmd "Avro schema registration ($TOPIC_EVENTS_RAW)" 5 3 \
        curl -sf -X POST \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            --data "{\"schema\": \"${RAW_AVRO_SCHEMA}\"}" \
            "$SCHEMA_REGISTRY_URL/subjects/${TOPIC_EVENTS_RAW}-value/versions"

    retry_cmd "Avro schema registration ($TOPIC_EVENTS_ENRICHED)" 5 3 \
        curl -sf -X POST \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            --data "{\"schema\": \"${ENRICHED_AVRO_SCHEMA}\"}" \
            "$SCHEMA_REGISTRY_URL/subjects/${TOPIC_EVENTS_ENRICHED}-value/versions"

    ok "Avro schemas registered for $TOPIC_EVENTS_RAW and $TOPIC_EVENTS_ENRICHED"
}

register_kafka_connectors() {
    step "STEP 12 — Registering Kafka Connect sink connectors"

    wait_for "Kafka Connect" "curl -sf $KAFKA_CONNECT_URL/connectors" 3 30

    local pg_pass="${POSTGRES_PASSWORD:-Scaibu@123}"

    if [ -f "$RAW_SINK_CONFIG.template" ]; then
        sed "s|\${POSTGRES_PASSWORD}|$pg_pass|g" "$RAW_SINK_CONFIG.template" > "$RAW_SINK_CONFIG"
    fi
    if [ -f "$ENRICHED_SINK_CONFIG.template" ]; then
        sed "s|\${POSTGRES_PASSWORD}|$pg_pass|g" "$ENRICHED_SINK_CONFIG.template" > "$ENRICHED_SINK_CONFIG"
    fi

    if [ -f "$RAW_SINK_CONFIG" ]; then
        retry_cmd "Postgres raw sink registration" 5 3 \
            bash -c "code=\$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data @'$RAW_SINK_CONFIG' '$KAFKA_CONNECT_URL/connectors'); [[ \$code =~ ^(200|201|409)\$ ]]"
        ok "Registered: postgres-raw-sink"
    else
        log "Skipping raw sink — $RAW_SINK_CONFIG not found"
    fi

    if [ -f "$ENRICHED_SINK_CONFIG" ]; then
        retry_cmd "Postgres enriched sink registration" 5 3 \
            bash -c "code=\$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data @'$ENRICHED_SINK_CONFIG' '$KAFKA_CONNECT_URL/connectors'); [[ \$code =~ ^(200|201|409)\$ ]]"
        ok "Registered: postgres-enriched-sink"
    else
        log "Skipping enriched sink — $ENRICHED_SINK_CONFIG not found"
    fi
}

display_summary() {
    local admin_pass
    admin_pass=$(grep "^GF_SECURITY_ADMIN_PASSWORD=" "$ENV_FILE" | cut -d= -f2-)

    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║              EVENT PLATFORM — SETUP COMPLETE                        ║"
    echo "╠══════════════════════════════════════════════════════════════════════╣"
    echo "║  PUBLIC ENDPOINTS (via Traefik)                                     ║"
    echo "║  Ingestion API:       http://api.scaibu.localhost/v1/events         ║"
    echo "║  API Health:          http://api.scaibu.localhost/healthz           ║"
    echo "║  Grafana:             http://localhost:27402 (or http://grafana.scaibu.localhost) ║"
    echo "║                                                                     ║"
    echo "║  INTERNAL ENDPOINTS (direct access, dev only)                       ║"
    echo "║  Prometheus:          http://localhost:9090                         ║"
    echo "║  Schema Registry:     http://localhost:8081                         ║"
    echo "║  Kafka Connect:       http://localhost:8083                         ║"
    echo "║  OTel gRPC:           localhost:4317                                ║"
    echo "║  OTel HTTP:           localhost:4318                                ║"
    echo "║                                                                     ║"
    echo "║  CREDENTIALS                                                        ║"
    echo "║  Grafana admin:       admin / ${admin_pass}                         "
    echo "║                                                                     ║"
    echo "║  SCALE API REPLICAS                                                 ║"
    echo "║  docker compose -f infra/docker-compose.yml up -d                  ║"
    echo "║    --scale ingestion-api=4                                          ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""
}

main() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║         EVENT PLATFORM — FULL ENVIRONMENT SETUP                    ║"
    echo "║         $(date '+%Y-%m-%d %H:%M:%S %Z')                                  ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"

    check_prerequisites
    recreate_env_file
    nuke_everything
    free_ports
    init_traefik_storage
    configure_hosts
    build_and_start
    wait_for_core_services
    migrate_postgres
    provision_kafka_topics
    register_avro_schemas
    register_kafka_connectors
    display_summary
}

main "$@"
