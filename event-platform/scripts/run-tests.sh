#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"
GO_SERVICE="$PROJECT_ROOT/services/ingestion-api"
COMPOSE_FILE="$INFRA_DIR/docker-compose.yml"
ENV_FILE="$INFRA_DIR/.env"

REPORTS_DIR="$PROJECT_ROOT/reports"
UNIT_REPORTS_DIR="$REPORTS_DIR/unit"
INTEG_REPORTS_DIR="$REPORTS_DIR/integration"
BENCH_REPORTS_DIR="$REPORTS_DIR/benchmark"
LOAD_REPORTS_DIR="$REPORTS_DIR/loadtest"
HTML_REPORT_FILE="$REPORTS_DIR/report.html"

UNIT_PKG_PATH="./test/unit/..."
INTEG_PKG_PATH="./test/integration/..."
BENCH_PKG_PATH="./test/benchmark/..."

API_HOST="${API_HOST:-api.scaibu.localhost}"
TRAEFIK_PING="${TRAEFIK_PING:-http://localhost:8899/ping}"

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
DATE_DISPLAY=$(date -u +"%B %d, %Y %H:%M UTC")

UNIT_PASS=0; UNIT_FAIL=0; UNIT_SKIP=0; UNIT_TOTAL=0; UNIT_DURATION="0s"
INTEG_PASS=0; INTEG_FAIL=0; INTEG_SKIP=0; INTEG_TOTAL=0; INTEG_DURATION="0s"
BENCH_OUTPUT=""
COVERAGE_PCT="0.0"
UNIT_STATUS="PASS"
INTEG_STATUS="PASS"
UNIT_RAW=""
INTEG_RAW=""
LOAD_STATUS="SKIPPED"
LOAD_SUMMARY=""

init_report_dirs() {
    mkdir -p "$UNIT_REPORTS_DIR" "$INTEG_REPORTS_DIR" "$BENCH_REPORTS_DIR" "$LOAD_REPORTS_DIR"
}

count_matches() {
    grep -c "$1" "$2" 2>/dev/null || echo "0"
}

check_infrastructure() {
    echo "═══════════════════════════════════════════════"
    echo " INFRASTRUCTURE HEALTH CHECK"
    echo "═══════════════════════════════════════════════"

    local healthy=true

    if curl -sf "$TRAEFIK_PING" >/dev/null 2>&1; then
        echo "  ✅ Traefik: healthy"
    else
        echo "  ⚠️  Traefik: not reachable at $TRAEFIK_PING"
        healthy=false
    fi

    if curl -sf "http://${API_HOST}/healthz" >/dev/null 2>&1; then
        echo "  ✅ ingestion-api (via Traefik): healthy"
    else
        echo "  ⚠️  ingestion-api: not reachable at http://${API_HOST}/healthz"
        healthy=false
    fi

    if docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps kafka 2>/dev/null | grep -q "healthy"; then
        echo "  ✅ Kafka: healthy"
    else
        echo "  ⚠️  Kafka: not healthy"
        healthy=false
    fi

    if docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps redis 2>/dev/null | grep -q "healthy"; then
        echo "  ✅ Redis: healthy"
    else
        echo "  ⚠️  Redis: not healthy"
        healthy=false
    fi

    echo ""
    if [ "$healthy" = false ]; then
        echo "  ⚠️  Some services are not running. Run: bash scripts/setup-dev-environment.sh"
        echo "     Unit and benchmark tests will still run. Integration and load tests may fail."
    fi
    echo ""
}

run_go_test_suite() {
    local suite_type="$1"
    local pkg_path="$2"
    local output_subdir="$3"

    echo "═══════════════════════════════════════════════"
    echo " ${suite_type} TESTS"
    echo "═══════════════════════════════════════════════"
    cd "$GO_SERVICE"

    local start_time
    start_time=$(date +%s%N)
    local raw_output=""
    local exit_code=0

    set +e
    if [ "$suite_type" = "UNIT" ]; then
        raw_output=$(go test "$pkg_path" -v -count=1 \
            -coverprofile="$UNIT_REPORTS_DIR/coverage.out" \
            -covermode=atomic 2>&1)
        exit_code=$?
    else
        raw_output=$(go test "$pkg_path" -v -count=1 2>&1)
        exit_code=$?
    fi
    set -e

    local end_time
    end_time=$(date +%s%N)
    local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    local duration="${elapsed_ms}ms"
    local results_file="$REPORTS_DIR/$output_subdir/results.txt"

    echo "$raw_output"
    echo "$raw_output" > "$results_file"

    local pass_count fail_count skip_count total_count status
    pass_count=$(count_matches "--- PASS:" "$results_file")
    fail_count=$(count_matches "--- FAIL:" "$results_file")
    skip_count=$(count_matches "--- SKIP:" "$results_file")
    total_count=$((pass_count + fail_count + skip_count))
    status="PASS"
    if [ "$exit_code" -ne 0 ]; then status="FAIL"; fi

    if [ "$suite_type" = "UNIT" ]; then
        UNIT_RAW="$raw_output"
        UNIT_PASS=$pass_count; UNIT_FAIL=$fail_count
        UNIT_SKIP=$skip_count; UNIT_TOTAL=$total_count
        UNIT_DURATION=$duration; UNIT_STATUS=$status
        if [ -f "$UNIT_REPORTS_DIR/coverage.out" ]; then
            COVERAGE_PCT=$(go tool cover -func="$UNIT_REPORTS_DIR/coverage.out" 2>/dev/null \
                | grep total: | awk '{print $3}' || echo "0.0%")
            go tool cover -html="$UNIT_REPORTS_DIR/coverage.out" \
                -o "$UNIT_REPORTS_DIR/coverage.html" 2>/dev/null || true
        fi
    else
        INTEG_RAW="$raw_output"
        INTEG_PASS=$pass_count; INTEG_FAIL=$fail_count
        INTEG_SKIP=$skip_count; INTEG_TOTAL=$total_count
        INTEG_DURATION=$duration; INTEG_STATUS=$status
    fi

    echo ""
    echo "${suite_type}: ${pass_count} passed, ${fail_count} failed, ${skip_count} skipped (${duration})"
    echo ""
}

run_benchmarks() {
    echo "═══════════════════════════════════════════════"
    echo " BENCHMARK TESTS"
    echo "═══════════════════════════════════════════════"
    cd "$GO_SERVICE"

    set +e
    BENCH_OUTPUT=$(go test "$BENCH_PKG_PATH" -bench=. -benchmem -benchtime=3s -count=1 2>&1)
    set -e

    echo "$BENCH_OUTPUT"
    echo "$BENCH_OUTPUT" > "$BENCH_REPORTS_DIR/results.txt"
    echo ""
}

run_load_test() {
    local scenario="${1:-rampup}"

    echo "═══════════════════════════════════════════════"
    echo " LOAD TEST — scenario: $scenario"
    echo "═══════════════════════════════════════════════"

    if ! command -v k6 >/dev/null 2>&1; then
        echo "  ⚠️  k6 not installed — skipping load test"
        echo "  Install: https://grafana.com/docs/k6/latest/set-up/install-k6/"
        LOAD_STATUS="SKIPPED (k6 not installed)"
        return
    fi

    if ! curl -sf "http://${API_HOST}/healthz" >/dev/null 2>&1; then
        echo "  ⚠️  API not reachable at http://${API_HOST}/healthz — skipping load test"
        LOAD_STATUS="SKIPPED (API not reachable)"
        return
    fi

    local load_script="$PROJECT_ROOT/loadtest/traefik_integration.ts"
    if [ ! -f "$load_script" ]; then
        load_script="$PROJECT_ROOT/loadtest/ingestion_burst.ts"
    fi

    local output_file="$LOAD_REPORTS_DIR/${scenario}_$(date +%Y%m%d_%H%M%S).json"

    set +e
    SCENARIO="$scenario" API_HOST="$API_HOST" \
        k6 run \
            --out json="$output_file" \
            --summary-export="$LOAD_REPORTS_DIR/${scenario}_summary.json" \
            "$load_script" 2>&1 | tee "$LOAD_REPORTS_DIR/${scenario}_output.txt"
    local exit_code=$?
    set -e

    if [ "$exit_code" -eq 0 ]; then
        LOAD_STATUS="PASS"
    else
        LOAD_STATUS="FAIL (thresholds breached or error)"
    fi

    if [ -f "$LOAD_REPORTS_DIR/${scenario}_summary.json" ]; then
        LOAD_SUMMARY=$(cat "$LOAD_REPORTS_DIR/${scenario}_summary.json" | \
            python3 -c "
import json,sys
d=json.load(sys.stdin)
m=d.get('metrics',{})
rps=m.get('http_reqs',{}).get('rate',0)
p95=m.get('http_req_duration',{}).get('values',{}).get('p(95)',0)
p99=m.get('http_req_duration',{}).get('values',{}).get('p(99)',0)
err=m.get('http_req_failed',{}).get('rate',0)
print(f'RPS={rps:.1f} p95={p95:.0f}ms p99={p99:.0f}ms err={err*100:.2f}%')
" 2>/dev/null || echo "see $LOAD_REPORTS_DIR/${scenario}_output.txt")
    fi

    echo ""
    echo "Load test ${LOAD_STATUS}: ${LOAD_SUMMARY}"
    echo ""
}

parse_test_rows() {
    local raw_content="$1"
    local rows=""

    while IFS= read -r line; do
        if echo "$line" | grep -q '--- PASS:\|--- FAIL:\|--- SKIP:'; then
            local status="PASS"
            local row_color="#22c55e"
            if echo "$line" | grep -q '--- FAIL:'; then status="FAIL"; row_color="#ef4444"; fi
            if echo "$line" | grep -q '--- SKIP:'; then status="SKIP"; row_color="#f59e0b"; fi
            local test_name
            test_name=$(echo "$line" | sed 's/.*--- [A-Z]*: //' | awk '{print $1}')
            local duration
            duration=$(echo "$line" | grep -o '([0-9.]*s)' || echo "(0.00s)")
            rows="${rows}<tr><td>${test_name}</td><td style=\"color:${row_color};font-weight:600\">${status}</td><td>${duration}</td></tr>"
        fi
    done <<< "$raw_content"
    echo "$rows"
}

parse_benchmark_rows() {
    local bench_content="$1"
    local rows=""

    while IFS= read -r line; do
        if echo "$line" | grep -qE '^Benchmark'; then
            local bench_name iterations ns_op bytes_op allocs_op
            bench_name=$(echo "$line" | awk '{print $1}')
            iterations=$(echo "$line" | awk '{print $2}')
            ns_op=$(echo "$line" | awk '{print $3}')
            bytes_op=$(echo "$line" | awk '{print $5}' || echo "N/A")
            allocs_op=$(echo "$line" | awk '{print $7}' || echo "N/A")
            rows="${rows}<tr><td>${bench_name}</td><td>${iterations}</td><td>${ns_op}</td><td>${bytes_op}</td><td>${allocs_op}</td></tr>"
        fi
    done <<< "$bench_content"
    echo "$rows"
}

generate_html_report() {
    local overall_status="PASS"
    if [ "$UNIT_STATUS" = "FAIL" ] || [ "$INTEG_STATUS" = "FAIL" ]; then
        overall_status="FAIL"
    fi

    local total_pass=$((UNIT_PASS + INTEG_PASS))
    local total_fail=$((UNIT_FAIL + INTEG_FAIL))
    local total_skip=$((UNIT_SKIP + INTEG_SKIP))
    local total_tests=$((UNIT_TOTAL + INTEG_TOTAL))

    local status_color="#22c55e"
    local status_bg="rgba(34,197,94,0.1)"
    if [ "$overall_status" = "FAIL" ]; then
        status_color="#ef4444"
        status_bg="rgba(239,68,68,0.1)"
    fi

    local coverage_color="#22c55e"
    local cov_num
    cov_num=$(echo "$COVERAGE_PCT" | tr -d '%')
    if (( $(echo "$cov_num < 50" | bc -l 2>/dev/null || echo 1) )); then
        coverage_color="#ef4444"
    elif (( $(echo "$cov_num < 80" | bc -l 2>/dev/null || echo 0) )); then
        coverage_color="#f59e0b"
    fi

    local bench_rows unit_test_rows integ_test_rows
    bench_rows=$(parse_benchmark_rows "$BENCH_OUTPUT")
    unit_test_rows=$(parse_test_rows "$UNIT_RAW")
    integ_test_rows=$(parse_test_rows "$INTEG_RAW")

    local load_color="#94a3b8"
    if [ "$LOAD_STATUS" = "PASS" ]; then load_color="#22c55e";
    elif echo "$LOAD_STATUS" | grep -q "FAIL"; then load_color="#ef4444"; fi

    cat > "$HTML_REPORT_FILE" <<REPORTEOF
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Event Platform — Test Report</title>
<style>
  :root { --bg: #0f172a; --surface: #1e293b; --border: #334155; --text: #e2e8f0; --muted: #94a3b8; --accent: #3b82f6; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; background: var(--bg); color: var(--text); line-height: 1.6; }
  .container { max-width: 1200px; margin: 0 auto; padding: 2rem; }
  header { text-align: center; margin-bottom: 3rem; padding-bottom: 2rem; border-bottom: 1px solid var(--border); }
  header h1 { font-size: 2rem; font-weight: 700; margin-bottom: 0.5rem; }
  header .subtitle { color: var(--muted); font-size: 0.9rem; }
  .status-badge { display: inline-block; padding: 0.5rem 1.5rem; border-radius: 9999px; font-weight: 700; font-size: 1.1rem; margin: 1rem 0; background: ${status_bg}; color: ${status_color}; border: 2px solid ${status_color}; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; margin-bottom: 3rem; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 1.5rem; text-align: center; }
  .card .value { font-size: 2.5rem; font-weight: 700; }
  .card .label { color: var(--muted); font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 0.25rem; }
  section { margin-bottom: 3rem; }
  section h2 { font-size: 1.4rem; font-weight: 600; margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid var(--border); }
  table { width: 100%; border-collapse: collapse; background: var(--surface); border-radius: 8px; overflow: hidden; }
  th { background: #0f172a; padding: 0.75rem 1rem; text-align: left; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }
  td { padding: 0.75rem 1rem; border-top: 1px solid var(--border); font-size: 0.9rem; }
  tr:hover td { background: rgba(59,130,246,0.05); }
  footer { text-align: center; color: var(--muted); font-size: 0.8rem; padding-top: 2rem; border-top: 1px solid var(--border); }
  .section-status { float: right; font-size: 0.85rem; font-weight: 600; }
  .coverage-bar { height: 8px; background: var(--border); border-radius: 4px; margin-top: 0.5rem; overflow: hidden; }
  .coverage-fill { height: 100%; border-radius: 4px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Event Platform — Test Report</h1>
    <div class="subtitle">Generated: $DATE_DISPLAY</div>
    <div class="status-badge">$overall_status</div>
  </header>

  <div class="grid">
    <div class="card"><div class="value" style="color:#22c55e">$total_pass</div><div class="label">Passed</div></div>
    <div class="card"><div class="value" style="color:#ef4444">$total_fail</div><div class="label">Failed</div></div>
    <div class="card"><div class="value" style="color:#f59e0b">$total_skip</div><div class="label">Skipped</div></div>
    <div class="card"><div class="value" style="color:var(--accent)">$total_tests</div><div class="label">Total Tests</div></div>
    <div class="card">
      <div class="value" style="color:$coverage_color">$COVERAGE_PCT</div>
      <div class="label">Code Coverage</div>
      <div class="coverage-bar"><div class="coverage-fill" style="width:$COVERAGE_PCT;background:$coverage_color"></div></div>
    </div>
    <div class="card">
      <div class="value" style="color:${load_color};font-size:1rem;padding-top:0.5rem">$LOAD_STATUS</div>
      <div class="label">Load Test</div>
      <div style="font-size:0.75rem;color:var(--muted);margin-top:0.25rem">$LOAD_SUMMARY</div>
    </div>
  </div>

  <section>
    <h2>Unit Tests <span class="section-status" style="color:$([ "$UNIT_STATUS" = "PASS" ] && echo "#22c55e" || echo "#ef4444")">$UNIT_STATUS — $UNIT_DURATION</span></h2>
    <table><thead><tr><th>Test Name</th><th>Status</th><th>Duration</th></tr></thead><tbody>$unit_test_rows</tbody></table>
  </section>

  <section>
    <h2>Integration Tests <span class="section-status" style="color:$([ "$INTEG_STATUS" = "PASS" ] && echo "#22c55e" || echo "#ef4444")">$INTEG_STATUS — $INTEG_DURATION</span></h2>
    <table><thead><tr><th>Test Name</th><th>Status</th><th>Duration</th></tr></thead><tbody>$integ_test_rows</tbody></table>
  </section>

  <section>
    <h2>Benchmark Results</h2>
    <table><thead><tr><th>Benchmark</th><th>Iterations</th><th>ns/op</th><th>B/op</th><th>allocs/op</th></tr></thead><tbody>$bench_rows</tbody></table>
  </section>

  <section>
    <h2>Artifacts</h2>
    <table>
      <thead><tr><th>Report</th><th>Path</th></tr></thead>
      <tbody>
        <tr><td>Unit Test Results</td><td>reports/unit/results.txt</td></tr>
        <tr><td>Code Coverage (HTML)</td><td>reports/unit/coverage.html</td></tr>
        <tr><td>Integration Test Results</td><td>reports/integration/results.txt</td></tr>
        <tr><td>Benchmark Results</td><td>reports/benchmark/results.txt</td></tr>
        <tr><td>Load Test Output</td><td>reports/loadtest/</td></tr>
        <tr><td>Consolidated Report</td><td>reports/report.html</td></tr>
      </tbody>
    </table>
  </section>

  <footer>Event Platform Test Suite &middot; Ingestion API (Go) + Traefik v3.7.10 &middot; $TIMESTAMP</footer>
</div>
</body>
</html>
REPORTEOF

    echo "═══════════════════════════════════════════════"
    echo " REPORT GENERATED"
    echo "═══════════════════════════════════════════════"
    echo "  Status:      $overall_status"
    echo "  Tests:       $total_pass passed / $total_fail failed / $total_skip skipped"
    echo "  Coverage:    $COVERAGE_PCT"
    echo "  Load Test:   $LOAD_STATUS"
    echo "  HTML Report: $HTML_REPORT_FILE"
    echo ""
}

usage() {
    echo "Usage: $0 [--unit] [--integration] [--bench] [--load [scenario]] [--all] [--report]"
    echo ""
    echo "  --unit          Run unit tests only"
    echo "  --integration   Run integration tests only"
    echo "  --bench         Run benchmark tests only"
    echo "  --load          Run load test (default scenario: rampup)"
    echo "  --load sustained  Run sustained 1667 RPS load test"
    echo "  --load burst      Run burst load test"
    echo "  --all           Run all tests including load test"
    echo "  --report        Run all tests and generate HTML report (default)"
    echo ""
}

main() {
    echo ""
    echo "╔═══════════════════════════════════════════════╗"
    echo "║   EVENT PLATFORM — TEST SUITE                ║"
    echo "║   $(date -u +"%Y-%m-%d %H:%M UTC")                       ║"
    echo "╚═══════════════════════════════════════════════╝"
    echo ""

    local run_unit=false
    local run_integ=false
    local run_bench=false
    local run_load=false
    local run_report=false
    local load_scenario="rampup"

    if [ "$#" -eq 0 ]; then
        run_unit=true; run_integ=true; run_bench=true; run_report=true
    fi

    while [ "$#" -gt 0 ]; do
        case "$1" in
            --unit)        run_unit=true ;;
            --integration) run_integ=true ;;
            --bench)       run_bench=true ;;
            --load)
                run_load=true
                if [ "${2:-}" != "" ] && [[ "${2}" != --* ]]; then
                    load_scenario="$2"; shift
                fi
                ;;
            --all)         run_unit=true; run_integ=true; run_bench=true; run_load=true; run_report=true ;;
            --report)      run_unit=true; run_integ=true; run_bench=true; run_report=true ;;
            --help|-h)     usage; exit 0 ;;
            *)             echo "Unknown option: $1"; usage; exit 1 ;;
        esac
        shift
    done

    init_report_dirs
    check_infrastructure

    [ "$run_unit" = true ]  && run_go_test_suite "UNIT"        "$UNIT_PKG_PATH"  "unit"
    [ "$run_integ" = true ] && run_go_test_suite "INTEGRATION" "$INTEG_PKG_PATH" "integration"
    [ "$run_bench" = true ] && run_benchmarks
    [ "$run_load" = true ]  && run_load_test "$load_scenario"
    [ "$run_report" = true ] && generate_html_report
}

main "$@"
