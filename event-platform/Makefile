.PHONY: up down build logs scale connectors \
        test-unit test-integration test-bench test-all test-report \
        test-load test-load-sustained test-load-burst \
        traefik-validate traefik-logs acme-init hosts-local

COMPOSE_DIR := infra
GO_SERVICE   := services/ingestion-api
COMPOSE      := docker compose -f $(COMPOSE_DIR)/docker-compose.yml

# ---------------------------------------------------------------------------
# Core lifecycle
# ---------------------------------------------------------------------------
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

build:
	$(COMPOSE) build

logs:
	$(COMPOSE) logs -f

# Tail Traefik logs specifically
traefik-logs:
	$(COMPOSE) logs -f traefik

# ---------------------------------------------------------------------------
# Scaling ingestion-api replicas
# Traefik auto-discovers all replicas via Docker socket.
# ---------------------------------------------------------------------------
# Usage: make scale REPLICAS=4
REPLICAS ?= 1
scale:
	$(COMPOSE) up -d --scale ingestion-api=$(REPLICAS) --no-recreate

# ---------------------------------------------------------------------------
# Kafka Connect
# ---------------------------------------------------------------------------
connectors:
	curl -X POST -H "Content-Type: application/json" \
	  --data @infra/kafka/connectors/postgres-raw-sink.json \
	  http://localhost:8083/connectors
	curl -X POST -H "Content-Type: application/json" \
	  --data @infra/kafka/connectors/postgres-enriched-sink.json \
	  http://localhost:8083/connectors

# ---------------------------------------------------------------------------
# Application tests
# ---------------------------------------------------------------------------
test-unit:
	docker compose -f infra/docker-compose.yml run --entrypoint "go test ./src/features/events/tests/... -v -run=TestRawEventValidate|TestLoadFromConfigAndGet|TestValidatePayload|TestPurchaseEnrichment" ingestion-api

test-integration:
	docker compose -f infra/docker-compose.yml run --entrypoint "go test ./src/features/events/tests/... -v -run=TestEventsApiIntegration|TestMethodNotAllowed|TestDeduperFailure" ingestion-api

test-bench:
	docker compose -f infra/docker-compose.yml run --entrypoint "go test ./src/features/events/tests/... -bench=. -benchmem" ingestion-api

test-all: test-unit test-integration


test-report:
	bash scripts/run-tests.sh

# ---------------------------------------------------------------------------
# Load tests (k6)
# ---------------------------------------------------------------------------
# Progressive ramp: 100 → 500 → 1k → 1667 → 2.5k → 5k → 10k RPS
test-load:
	k6 run loadtest/traefik_integration.ts

# Sustained target rate: 1667 RPS for 5 minutes
test-load-sustained:
	SCENARIO=sustained k6 run loadtest/traefik_integration.ts

# Burst: 3× normal for 30s then recovery
test-load-burst:
	SCENARIO=burst k6 run loadtest/traefik_integration.ts

# Legacy burst test (pre-Traefik, direct to :8080)
test-load-legacy:
	k6 run loadtest/ingestion_burst.ts

# ---------------------------------------------------------------------------
# Traefik helpers
# ---------------------------------------------------------------------------

# Verify Traefik config YAML syntax before starting
traefik-validate:
	docker run --rm \
	  -v $(PWD)/infra/traefik:/etc/traefik:ro \
	  traefik:v3.7.10 traefik --configFile=/etc/traefik/traefik.yml --dry-run 2>&1 | head -50

# Initialize ACME storage file with correct permissions (must be 600)
acme-init:
	touch infra/traefik/acme/acme.json
	chmod 600 infra/traefik/acme/acme.json
	@echo "acme.json initialized at infra/traefik/acme/acme.json"

# Add local dev hostnames to /etc/hosts (Linux/macOS — requires sudo)
hosts-local:
	@echo "Adding local dev hostnames to /etc/hosts..."
	sudo bash -c 'grep -q "api.scaibu.localhost" /etc/hosts || echo "127.0.0.1 api.scaibu.localhost grafana.scaibu.localhost traefik.scaibu.localhost" >> /etc/hosts'
	@echo "Done. Test with: curl http://api.scaibu.localhost/healthz"
