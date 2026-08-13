#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Automatic k6 Benchmark Runner + Grafana Annotator + Report Generator
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNBOOKS_DIR="$(cd "$PROJECT_ROOT/.." && pwd)/runbooks/iterations"
REPORT_DIR="${RUNBOOKS_DIR}/20260812-performance-report/reports"

VUS="${1:-50}"
ITERATIONS="${2:-1000}"
SCENARIO="${3:-shared-1k}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:27402}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:Scaibu@123}"

mkdir -p "$REPORT_DIR"

TIMESTAMP=$(date +%s%3N)
HUMAN_DATE=$(date "+%Y-%m-%d %H:%M:%S")

echo "======================================================================="
echo " 🚀 Running Automated k6 Benchmark ($VUS VUs, $ITERATIONS Iterations)"
echo "======================================================================="

# Step 1: Post Start Annotation to Grafana
start_time=$(date +%s%3N)
curl -s -X POST "${GRAFANA_URL}/api/annotations" \
  -u "${GRAFANA_AUTH}" \
  -H "Content-Type: application/json" \
  -d '{
    "dashboardUID": "pipeline-analytics",
    "time": '"${start_time}"',
    "text": "<b>🚀 Benchmark Started</b><br/>VUs: '"${VUS}"' | Iterations: '"${ITERATIONS}"' | Scenario: '"${SCENARIO}"'",
    "tags": ["loadtest", "k6", "start"]
  }' > /dev/null || true

# Step 2: Execute k6 run via Docker and capture output
RESULTS_JSON="/tmp/k6-run-${timestamp:-$start_time}.json"
OUTPUT_LOG="/tmp/k6-log-${timestamp:-$start_time}.txt"

docker run --rm \
  --add-host host.docker.internal:host-gateway \
  -v "${PROJECT_ROOT}/loadtest:/loadtest" \
  grafana/k6 run /loadtest/ingestion_10k_loadtest.ts \
  --vus "${VUS}" \
  --iterations "${ITERATIONS}" \
  -e TARGET_URL=http://host.docker.internal:27488/v1/events \
  2>&1 | tee "${OUTPUT_LOG}"

end_time=$(date +%s%3N)

# Step 3: Parse key metrics from k6 stdout
P95_LATENCY=$(grep "http_req_duration" "${OUTPUT_LOG}" | grep "p(95)=" | tail -1 | sed -E 's/.*p\(95\)=([^ ]+).*/\1/' || echo "N/A")
TOTAL_REQ=$(grep "http_reqs" "${OUTPUT_LOG}" | tail -1 | awk '{print $2}' || echo "${ITERATIONS}")
RPS=$(grep "http_reqs" "${OUTPUT_LOG}" | tail -1 | awk '{print $3}' || echo "N/A")
ERR_RATE=$(grep "http_req_failed" "${OUTPUT_LOG}" | tail -1 | awk '{print $2}' || echo "0.00%")
DATA_SENT=$(grep "data_sent" "${OUTPUT_LOG}" | tail -1 | awk '{print $2, $3}' || echo "N/A")
DATA_RATE=$(grep "data_sent" "${OUTPUT_LOG}" | tail -1 | awk '{print $4, $5}' || echo "N/A")

echo ""
echo "======================================================================="
echo " 📊 Benchmark Metrics Summary"
echo "======================================================================="
echo "  • Total Requests:  ${TOTAL_REQ}"
echo "  • Throughput:      ${RPS}"
echo "  • p95 Latency:     ${P95_LATENCY}"
echo "  • Error Rate:      ${ERR_RATE}"
echo "  • Data Sent:       ${DATA_SENT}"
echo "  • Data Speed:      ${DATA_RATE}"
echo "======================================================================="

# Step 4: Post End Annotation to Grafana
curl -s -X POST "${GRAFANA_URL}/api/annotations" \
  -u "${GRAFANA_AUTH}" \
  -H "Content-Type: application/json" \
  -d '{
    "dashboardUID": "pipeline-analytics",
    "time": '"${start_time}"',
    "timeEnd": '"${end_time}"',
    "text": "<b>✅ Benchmark Completed</b><br/>Reqs: '"${TOTAL_REQ}"' | RPS: '"${RPS}"'<br/>p95: '"${P95_LATENCY}"' | Errors: '"${ERR_RATE}"'<br/>Data Speed: '"${DATA_RATE}"' ('"${DATA_SENT}"' total)",
    "tags": ["loadtest", "k6", "completed"]
  }' > /dev/null || true

# Step 5: Append to Runbook Performance Log
PERF_LOG_MD="${REPORT_DIR}/benchmark-execution-history.md"

if [ ! -f "${PERF_LOG_MD}" ]; then
  cat > "${PERF_LOG_MD}" << 'EOF'
# Automated Benchmark Execution History & Comparison Log

| Timestamp | Scenario | VUs | Reqs | Throughput (RPS) | p95 Latency | Error Rate | Data Speed | Total Payload |
|---|---|---|---|---|---|---|---|---|
EOF
fi

echo "| ${HUMAN_DATE} | ${SCENARIO} | ${VUS} | ${TOTAL_REQ} | ${RPS} | ${P95_LATENCY} | ${ERR_RATE} | ${DATA_RATE} | ${DATA_SENT} |" >> "${PERF_LOG_MD}"

echo "✅ Grafana Timeline Annotation Posted!"
echo "✅ Results saved to ${PERF_LOG_MD}"
