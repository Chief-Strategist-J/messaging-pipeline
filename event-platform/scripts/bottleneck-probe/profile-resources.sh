#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NETWORK="event-platform_backbone"
OUT_DIR="${SCRIPT_DIR}/results"
K6_IMAGE="grafana/k6:latest"
PROBE_SCRIPT="/probe/probe.js"
SAMPLE_INTERVAL=3

mkdir -p "$OUT_DIR"

LABEL="${1:?usage: profile-resources.sh <label> <k6-env-flags...>}"
shift

STATS_FILE="${OUT_DIR}/${LABEL}-resources.txt"
: > "$STATS_FILE"

sample_stats() {
    while true; do
        docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' >> "$STATS_FILE"
        echo "---" >> "$STATS_FILE"
        sleep "$SAMPLE_INTERVAL"
    done
}

sample_stats &
SAMPLER_PID=$!

docker run --rm --network "$NETWORK" -v "${SCRIPT_DIR}:/probe:ro" "$@" \
    "$K6_IMAGE" run --no-color "$PROBE_SCRIPT" 2>&1 \
    | grep -E 'http_req_duration|http_reqs|checks_succeeded|checks_failed'

kill "$SAMPLER_PID" 2>/dev/null
wait "$SAMPLER_PID" 2>/dev/null

echo ""
echo "PEAK CPU PER CONTAINER"
awk '$1 != "---" { gsub(/%/,"",$2); if ($2+0 > peak[$1]) peak[$1] = $2+0 }
     END { for (name in peak) printf "  %-42s %7.1f%%\n", name, peak[name] }' "$STATS_FILE" \
    | sort -k2 -rn

echo ""
echo "PEAK MEMORY PER CONTAINER"
awk '$1 != "---" { split($3, mem, "MiB|GiB"); unit = ($0 ~ /GiB \/|GiB\//) ? 1024 : 1;
     value = mem[1] + 0; if (value > peak[$1]) peak[$1] = value }
     END { for (name in peak) printf "  %-42s %9.1f\n", name, peak[name] }' "$STATS_FILE" \
    | sort -k2 -rn
