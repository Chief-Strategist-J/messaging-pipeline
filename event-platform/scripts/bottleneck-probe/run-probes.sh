#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NETWORK="event-platform_backbone"
OUT_DIR="${SCRIPT_DIR}/results"
K6_IMAGE="grafana/k6:latest"
PROBE_SCRIPT="/probe/probe.js"
EDGE_URL="http://traefik:80"
DIRECT_URL="http://ingestion-api:8080"
METRIC_FILTER='http_req_duration|http_reqs|checks|http_req_waiting|http_req_sending|dropped_iterations'

mkdir -p "$OUT_DIR"

run_probe() {
    local label="$1"
    shift
    echo ""
    echo "=============================================================="
    echo "  PROBE: ${label}"
    echo "=============================================================="
    docker run --rm --network "$NETWORK" -v "${SCRIPT_DIR}:/probe:ro" "$@" \
        "$K6_IMAGE" run --no-color "$PROBE_SCRIPT" 2>&1 \
        | grep -E "$METRIC_FILTER" \
        | tee "${OUT_DIR}/${label}.txt"
}

run_probe "1-healthz-via-traefik" \
    -e MODE=health -e URL="${EDGE_URL}/healthz" -e VUS=100 -e DUR=20s

run_probe "2-healthz-direct" \
    -e MODE=health -e URL="${DIRECT_URL}/healthz" -e VUS=100 -e DUR=20s

run_probe "3-ingest-small-direct" \
    -e URL="${DIRECT_URL}/v1/events" -e PAYLOAD_KB=0 -e VUS=100 -e DUR=30s

run_probe "4-ingest-small-via-traefik" \
    -e URL="${EDGE_URL}/v1/events" -e PAYLOAD_KB=0 -e VUS=100 -e DUR=30s

run_probe "5-ingest-500kb-direct" \
    -e URL="${DIRECT_URL}/v1/events" -e PAYLOAD_KB=500 -e VUS=50 -e DUR=30s

run_probe "6-ingest-500kb-via-traefik" \
    -e URL="${EDGE_URL}/v1/events" -e PAYLOAD_KB=500 -e VUS=50 -e DUR=30s

echo ""
echo "Results written to ${OUT_DIR}"
