#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GO_SERVICE="$PROJECT_ROOT/services/ingestion-api"
REPORTS_DIR="$PROJECT_ROOT/reports"
ALLURE_RESULTS_DIR="$REPORTS_DIR/allure-results"
ALLURE_REPORT_DIR="$REPORTS_DIR/allure-report"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
DATE_DISPLAY=$(date -u +"%B %d, %Y %H:%M UTC")

mkdir -p "$REPORTS_DIR/unit"
mkdir -p "$REPORTS_DIR/integration"
mkdir -p "$REPORTS_DIR/benchmark"
mkdir -p "$ALLURE_RESULTS_DIR"
mkdir -p "$ALLURE_REPORT_DIR"

UNIT_PASS=0
UNIT_FAIL=0
UNIT_SKIP=0
UNIT_TOTAL=0
UNIT_DURATION="0s"
INTEG_PASS=0
INTEG_FAIL=0
INTEG_SKIP=0
INTEG_TOTAL=0
INTEG_DURATION="0s"
BENCH_OUTPUT=""
COVERAGE_PCT="0.0"
UNIT_STATUS="PASS"
INTEG_STATUS="PASS"
UNIT_RAW=""
INTEG_RAW=""

run_unit_tests() {
    echo "═══════════════════════════════════════════════"
    echo " UNIT TESTS"
    echo "═══════════════════════════════════════════════"
    cd "$GO_SERVICE"

    local start_time=$(date +%s%N)
    set +e
    UNIT_RAW=$(go test ./test/unit/... -v -count=1 -coverprofile="$REPORTS_DIR/unit/coverage.out" -covermode=atomic 2>&1)
    local exit_code=$?
    set -e
    local end_time=$(date +%s%N)
    local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    UNIT_DURATION="${elapsed_ms}ms"

    echo "$UNIT_RAW"
    echo "$UNIT_RAW" > "$REPORTS_DIR/unit/results.txt"

    UNIT_PASS=$(grep -c "--- PASS:" "$REPORTS_DIR/unit/results.txt" || echo "0")
    UNIT_FAIL=$(grep -c "--- FAIL:" "$REPORTS_DIR/unit/results.txt" || echo "0")
    UNIT_SKIP=$(grep -c "--- SKIP:" "$REPORTS_DIR/unit/results.txt" || echo "0")
    UNIT_TOTAL=$((UNIT_PASS + UNIT_FAIL + UNIT_SKIP))

    if [ -f "$REPORTS_DIR/unit/coverage.out" ]; then
        COVERAGE_PCT=$(go tool cover -func="$REPORTS_DIR/unit/coverage.out" 2>/dev/null | grep total: | awk '{print $3}' || echo "0.0%")
        go tool cover -html="$REPORTS_DIR/unit/coverage.out" -o "$REPORTS_DIR/unit/coverage.html" 2>/dev/null || true
    fi

    if [ "$exit_code" -ne 0 ]; then
        UNIT_STATUS="FAIL"
    fi

    echo ""
    echo "Unit: $UNIT_PASS passed, $UNIT_FAIL failed, $UNIT_SKIP skipped ($UNIT_DURATION)"
    echo ""
}

run_integration_tests() {
    echo "═══════════════════════════════════════════════"
    echo " INTEGRATION TESTS"
    echo "═══════════════════════════════════════════════"
    cd "$GO_SERVICE"

    local start_time=$(date +%s%N)
    set +e
    INTEG_RAW=$(go test ./test/integration/... -v -count=1 2>&1)
    local exit_code=$?
    set -e
    local end_time=$(date +%s%N)
    local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    INTEG_DURATION="${elapsed_ms}ms"

    echo "$INTEG_RAW"
    echo "$INTEG_RAW" > "$REPORTS_DIR/integration/results.txt"

    INTEG_PASS=$(grep -c "--- PASS:" "$REPORTS_DIR/integration/results.txt" || echo "0")
    INTEG_FAIL=$(grep -c "--- FAIL:" "$REPORTS_DIR/integration/results.txt" || echo "0")
    INTEG_SKIP=$(grep -c "--- SKIP:" "$REPORTS_DIR/integration/results.txt" || echo "0")
    INTEG_TOTAL=$((INTEG_PASS + INTEG_FAIL + INTEG_SKIP))

    if [ "$exit_code" -ne 0 ]; then
        INTEG_STATUS="FAIL"
    fi

    echo ""
    echo "Integration: $INTEG_PASS passed, $INTEG_FAIL failed, $INTEG_SKIP skipped ($INTEG_DURATION)"
    echo ""
}

run_benchmarks() {
    echo "═══════════════════════════════════════════════"
    echo " BENCHMARK TESTS"
    echo "═══════════════════════════════════════════════"
    cd "$GO_SERVICE"

    set +e
    BENCH_OUTPUT=$(go test ./test/benchmark/... -bench=. -benchmem -benchtime=3s -count=1 2>&1)
    set -e

    echo "$BENCH_OUTPUT"
    echo "$BENCH_OUTPUT" > "$REPORTS_DIR/benchmark/results.txt"
    echo ""
}

generate_html_report() {
    local OVERALL_STATUS="PASS"
    if [ "$UNIT_STATUS" = "FAIL" ] || [ "$INTEG_STATUS" = "FAIL" ]; then
        OVERALL_STATUS="FAIL"
    fi

    local TOTAL_PASS=$((UNIT_PASS + INTEG_PASS))
    local TOTAL_FAIL=$((UNIT_FAIL + INTEG_FAIL))
    local TOTAL_SKIP=$((UNIT_SKIP + INTEG_SKIP))
    local TOTAL_TESTS=$((UNIT_TOTAL + INTEG_TOTAL))

    local STATUS_COLOR="#22c55e"
    local STATUS_BG="rgba(34,197,94,0.1)"
    if [ "$OVERALL_STATUS" = "FAIL" ]; then
        STATUS_COLOR="#ef4444"
        STATUS_BG="rgba(239,68,68,0.1)"
    fi

    local COVERAGE_COLOR="#22c55e"
    local cov_num=$(echo "$COVERAGE_PCT" | tr -d '%')
    if (( $(echo "$cov_num < 50" | bc -l 2>/dev/null || echo 1) )); then
        COVERAGE_COLOR="#ef4444"
    elif (( $(echo "$cov_num < 80" | bc -l 2>/dev/null || echo 0) )); then
        COVERAGE_COLOR="#f59e0b"
    fi

    local BENCH_ROWS=""
    while IFS= read -r line; do
        if echo "$line" | grep -qE '^Benchmark'; then
            local bench_name=$(echo "$line" | awk '{print $1}')
            local iterations=$(echo "$line" | awk '{print $2}')
            local ns_op=$(echo "$line" | awk '{print $3}')
            local bytes_op=$(echo "$line" | awk '{print $5}' || echo "N/A")
            local allocs_op=$(echo "$line" | awk '{print $7}' || echo "N/A")
            BENCH_ROWS="$BENCH_ROWS<tr><td>$bench_name</td><td>$iterations</td><td>$ns_op</td><td>$bytes_op</td><td>$allocs_op</td></tr>"
        fi
    done <<< "$BENCH_OUTPUT"

    local UNIT_TEST_ROWS=""
    while IFS= read -r line; do
        if echo "$line" | grep -q '--- PASS:\|--- FAIL:\|--- SKIP:'; then
            local status=""
            if echo "$line" | grep -q '--- PASS:'; then status="PASS"; fi
            if echo "$line" | grep -q '--- FAIL:'; then status="FAIL"; fi
            if echo "$line" | grep -q '--- SKIP:'; then status="SKIP"; fi
            local test_name=$(echo "$line" | sed 's/.*--- [A-Z]*: //' | awk '{print $1}')
            local duration=$(echo "$line" | grep -o '([0-9.]*s)' || echo "(0.00s)")
            local row_color="#22c55e"
            if [ "$status" = "FAIL" ]; then row_color="#ef4444"; fi
            if [ "$status" = "SKIP" ]; then row_color="#f59e0b"; fi
            UNIT_TEST_ROWS="$UNIT_TEST_ROWS<tr><td>$test_name</td><td style=\"color:$row_color;font-weight:600\">$status</td><td>$duration</td></tr>"
        fi
    done <<< "$UNIT_RAW"

    local INTEG_TEST_ROWS=""
    while IFS= read -r line; do
        if echo "$line" | grep -q '--- PASS:\|--- FAIL:\|--- SKIP:'; then
            local status=""
            if echo "$line" | grep -q '--- PASS:'; then status="PASS"; fi
            if echo "$line" | grep -q '--- FAIL:'; then status="FAIL"; fi
            if echo "$line" | grep -q '--- SKIP:'; then status="SKIP"; fi
            local test_name=$(echo "$line" | sed 's/.*--- [A-Z]*: //' | awk '{print $1}')
            local duration=$(echo "$line" | grep -o '([0-9.]*s)' || echo "(0.00s)")
            local row_color="#22c55e"
            if [ "$status" = "FAIL" ]; then row_color="#ef4444"; fi
            if [ "$status" = "SKIP" ]; then row_color="#f59e0b"; fi
            INTEG_TEST_ROWS="$INTEG_TEST_ROWS<tr><td>$test_name</td><td style=\"color:$row_color;font-weight:600\">$status</td><td>$duration</td></tr>"
        fi
    done <<< "$INTEG_RAW"

    cat > "$REPORTS_DIR/report.html" <<REPORTEOF
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
  .status-badge { display: inline-block; padding: 0.5rem 1.5rem; border-radius: 9999px; font-weight: 700; font-size: 1.1rem; margin: 1rem 0; background: ${STATUS_BG}; color: ${STATUS_COLOR}; border: 2px solid ${STATUS_COLOR}; }
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
  .pass { color: #22c55e; }
  .fail { color: #ef4444; }
  .skip { color: #f59e0b; }
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
    <div class="status-badge">$OVERALL_STATUS</div>
  </header>

  <div class="grid">
    <div class="card">
      <div class="value" style="color:#22c55e">$TOTAL_PASS</div>
      <div class="label">Passed</div>
    </div>
    <div class="card">
      <div class="value" style="color:#ef4444">$TOTAL_FAIL</div>
      <div class="label">Failed</div>
    </div>
    <div class="card">
      <div class="value" style="color:#f59e0b">$TOTAL_SKIP</div>
      <div class="label">Skipped</div>
    </div>
    <div class="card">
      <div class="value" style="color:var(--accent)">$TOTAL_TESTS</div>
      <div class="label">Total Tests</div>
    </div>
    <div class="card">
      <div class="value" style="color:$COVERAGE_COLOR">$COVERAGE_PCT</div>
      <div class="label">Code Coverage</div>
      <div class="coverage-bar"><div class="coverage-fill" style="width:$COVERAGE_PCT;background:$COVERAGE_COLOR"></div></div>
    </div>
  </div>

  <section>
    <h2>Unit Tests <span class="section-status" style="color:$([ "$UNIT_STATUS" = "PASS" ] && echo "#22c55e" || echo "#ef4444")">$UNIT_STATUS — $UNIT_DURATION</span></h2>
    <table>
      <thead><tr><th>Test Name</th><th>Status</th><th>Duration</th></tr></thead>
      <tbody>$UNIT_TEST_ROWS</tbody>
    </table>
  </section>

  <section>
    <h2>Integration Tests <span class="section-status" style="color:$([ "$INTEG_STATUS" = "PASS" ] && echo "#22c55e" || echo "#ef4444")">$INTEG_STATUS — $INTEG_DURATION</span></h2>
    <table>
      <thead><tr><th>Test Name</th><th>Status</th><th>Duration</th></tr></thead>
      <tbody>$INTEG_TEST_ROWS</tbody>
    </table>
  </section>

  <section>
    <h2>Benchmark Results</h2>
    <table>
      <thead><tr><th>Benchmark</th><th>Iterations</th><th>ns/op</th><th>B/op</th><th>allocs/op</th></tr></thead>
      <tbody>$BENCH_ROWS</tbody>
    </table>
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
        <tr><td>Consolidated Report</td><td>reports/report.html</td></tr>
      </tbody>
    </table>
  </section>

  <footer>
    Event Platform Test Suite &middot; Ingestion API (Go) &middot; $TIMESTAMP
  </footer>
</div>
</body>
</html>
REPORTEOF

    echo "═══════════════════════════════════════════════"
    echo " REPORT GENERATED"
    echo "═══════════════════════════════════════════════"
    echo ""
    echo "  Status:      $OVERALL_STATUS"
    echo "  Tests:       $TOTAL_PASS passed / $TOTAL_FAIL failed / $TOTAL_SKIP skipped"
    echo "  Coverage:    $COVERAGE_PCT"
    echo ""
    echo "  HTML Report: $REPORTS_DIR/report.html"
    echo "  Coverage:    $REPORTS_DIR/unit/coverage.html"
    echo ""
}

echo ""
echo "╔═══════════════════════════════════════════════╗"
echo "║   EVENT PLATFORM — TEST SUITE                ║"
echo "║   $(date -u +"%Y-%m-%d %H:%M UTC")                       ║"
echo "╚═══════════════════════════════════════════════╝"
echo ""

run_unit_tests
run_integration_tests
run_benchmarks
generate_html_report
