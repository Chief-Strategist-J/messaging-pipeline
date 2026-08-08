.PHONY: up down build logs test-load connectors test-unit test-integration test-bench test-all test-report

COMPOSE_DIR := infra
GO_SERVICE := services/ingestion-api

up:
	docker compose -f $(COMPOSE_DIR)/docker-compose.yml up -d --build

down:
	docker compose -f $(COMPOSE_DIR)/docker-compose.yml down -v

build:
	docker compose -f $(COMPOSE_DIR)/docker-compose.yml build

logs:
	docker compose -f $(COMPOSE_DIR)/docker-compose.yml logs -f

connectors:
	curl -X POST -H "Content-Type: application/json" --data @infra/kafka/connectors/postgres-raw-sink.json http://localhost:8083/connectors
	curl -X POST -H "Content-Type: application/json" --data @infra/kafka/connectors/postgres-enriched-sink.json http://localhost:8083/connectors

test-unit:
	cd $(GO_SERVICE) && go test ./test/unit/... -v -count=1

test-integration:
	cd $(GO_SERVICE) && go test ./test/integration/... -v -count=1

test-bench:
	cd $(GO_SERVICE) && go test ./test/benchmark/... -bench=. -benchmem -benchtime=3s

test-all: test-unit test-integration test-bench

test-report:
	bash scripts/run-tests.sh

test-load:
	k6 run loadtest/ingestion_burst.ts
