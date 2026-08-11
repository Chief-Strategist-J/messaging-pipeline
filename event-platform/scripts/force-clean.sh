#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
readonly INFRA_DIR="$PROJECT_ROOT/infra"
readonly COMPOSE_FILE="$INFRA_DIR/docker-compose.yml"
readonly ENV_FILE="$INFRA_DIR/.env"
readonly PROJECT_NAME="event-platform"

readonly ALLURE_RESULTS_DIR="$PROJECT_ROOT/loadtest/allure-results"
readonly ALLURE_REPORT_DIR="$PROJECT_ROOT/loadtest/allure-report"
readonly K6_RESULTS_PATTERN="$PROJECT_ROOT/loadtest/k6-results-*.json"
readonly LOGS_DIR="$PROJECT_ROOT/logs"
readonly ACME_FILE="$INFRA_DIR/traefik/acme/acme.json"
readonly CERTS_DIR="$INFRA_DIR/traefik/certs"

readonly REQUIRED_PORTS=(27488 27443 27432 27479 27492 27493 27481 27483 27417 27418 27490 27402 27480)

log()  { echo "  $1"; }
ok()   { echo "  ✅ $1"; }
step() { 
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

destroy_docker_containers() {
    step "STEP 1 — Stopping & destroying Docker containers"
    if [ -f "$COMPOSE_FILE" ] && [ -f "$ENV_FILE" ]; then
        log "Stopping compose stack services..."
        docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" down --remove-orphans 2>/dev/null || true
    fi

    log "Force-killing any lingering project containers..."
    docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" \
        | xargs -r docker rm -f 2>/dev/null || true

    ok "Docker containers destroyed"
}

destroy_docker_networks() {
    step "STEP 2 — Removing Docker networks"
    log "Removing project networks..."
    docker network ls --filter "name=${PROJECT_NAME}" -q \
        | xargs -r docker network rm 2>/dev/null || true

    ok "Docker networks destroyed"
}

destroy_docker_volumes() {
    step "STEP 3 — Removing persistent Docker volumes & databases"
    if [ -f "$COMPOSE_FILE" ] && [ -f "$ENV_FILE" ]; then
        docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" down --volumes 2>/dev/null || true
    fi

    log "Removing project volumes..."
    docker volume ls --filter "name=${PROJECT_NAME}" -q \
        | xargs -r docker volume rm -f 2>/dev/null || true

    ok "Persistent volumes and databases destroyed"
}

destroy_docker_images() {
    step "STEP 4 — Force removing Docker images"
    log "Removing project-built and tagged Docker images..."
    docker images --filter "label=com.docker.compose.project=$PROJECT_NAME" -q \
        | xargs -r docker rmi -f 2>/dev/null || true

    docker images --filter "reference=*event-platform*" -q \
        | xargs -r docker rmi -f 2>/dev/null || true

    log "Pruning dangling docker images..."
    docker image prune -f 2>/dev/null || true

    ok "Docker images removed"
}

free_occupied_ports() {
    step "STEP 5 — Freeing occupied project ports"
    for port in "${REQUIRED_PORTS[@]}"; do
        local pids
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            log "Killing processes holding port $port (PID(s): $pids)..."
            kill -9 $pids 2>/dev/null || true
        fi
    done

    ok "Ports checked and freed: ${REQUIRED_PORTS[*]}"
}

clean_reports_logs_and_state() {
    step "STEP 6 — Cleaning generated test reports, logs, and certificates"
    log "Removing Allure results and reports..."
    rm -rf "$ALLURE_RESULTS_DIR" "$ALLURE_REPORT_DIR"

    log "Removing k6 load test results..."
    rm -f $K6_RESULTS_PATTERN

    log "Clearing application logs..."
    if [ -d "$LOGS_DIR" ]; then
        rm -rf "$LOGS_DIR"/*
    fi

    log "Clearing Traefik certificates and SSL state..."
    rm -f "$ACME_FILE"
    if [ -d "$CERTS_DIR" ]; then
        rm -rf "$CERTS_DIR"/*
    fi

    ok "Generated reports, logs, and certificates purged"
}

display_banner() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║         EVENT PLATFORM — FORCE CLEAN & DESTROY EVERYTHING            ║"
    echo "║         $(date '+%Y-%m-%d %H:%M:%S %Z')                                  ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
}

display_completion_summary() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║              FORCE PURGE COMPLETE                                    ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""
}

main() {
    display_banner
    destroy_docker_containers
    destroy_docker_networks
    destroy_docker_volumes
    destroy_docker_images
    free_occupied_ports
    clean_reports_logs_and_state
    display_completion_summary
}

main "$@"
