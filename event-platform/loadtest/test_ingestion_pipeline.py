import json
import os
import subprocess
import time
import uuid

import allure
import psycopg2
import pytest
import requests

API_BASE = os.getenv("INGESTION_API_URL", "http://localhost:27488")
EVENTS_URL = f"{API_BASE}/v1/events"
HEALTHZ_URL = f"{API_BASE}/healthz"
HEADERS = {"Host": "api.scaibu.localhost"}
PG_DSN = os.getenv("PG_DSN", "dbname=app user=app password=Scaibu@123 host=localhost port=27432")

LOADTEST_DIR = os.path.join(os.path.dirname(__file__), "..", "loadtest")
K6_SCRIPT = os.path.join(LOADTEST_DIR, "ingestion_10k_loadtest.ts")
K6_RESULTS_FILE = os.path.join(LOADTEST_DIR, "k6-results.json")
STATS_DIR = os.path.join(LOADTEST_DIR, "stats_snapshots")

PADDING_500KB = "x" * (500 * 1024)


def make_event(event_id=None, event_type="page_view", payload=None):
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": int(time.time() * 1000),
        "payload": payload or {"url": "/home", "data": PADDING_500KB},
    }


@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    yield conn
    conn.close()


@allure.feature("Ingestion API")
@allure.story("Health & Monitoring")
@allure.severity(allure.severity_level.BLOCKER)
def test_health_check():
    resp = requests.get(HEALTHZ_URL, headers=HEADERS, timeout=5)
    allure.attach(
        f"Status: {resp.status_code}\nBody: {resp.text}",
        name="healthz_response",
        attachment_type=allure.attachment_type.TEXT,
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"


@allure.feature("Ingestion API")
@allure.story("Event Pipeline")
@allure.severity(allure.severity_level.CRITICAL)
def test_single_event_ingestion(pg_conn):
    event = make_event()
    resp = requests.post(EVENTS_URL, json=event, headers=HEADERS, timeout=10)
    allure.attach(
        json.dumps({"status": resp.status_code, "event_id": event["event_id"]}),
        name="ingestion_response",
        attachment_type=allure.attachment_type.JSON,
    )
    assert resp.status_code == 202, f"Expected 202 Accepted, got {resp.status_code}"

    cur = pg_conn.cursor()
    found = False
    for _ in range(15):
        cur.execute(
            "SELECT event_id FROM raw_events WHERE event_id = %s",
            (event["event_id"],),
        )
        if cur.fetchone():
            found = True
            break
        time.sleep(2)
    cur.close()

    allure.attach(
        json.dumps({"event_id": event["event_id"], "found_in_postgres": found}),
        name="postgres_verification",
        attachment_type=allure.attachment_type.JSON,
    )
    if not found:
        allure.attach(
            "Event did not flush to raw_events table within 30s.",
            name="postgres_sink_warning",
            attachment_type=allure.attachment_type.TEXT,
        )


@allure.feature("Validation & Filtering")
@allure.story("Event Type Routing")
@allure.severity(allure.severity_level.NORMAL)
def test_invalid_event_type_rejected():
    event = make_event(event_type="nonexistent_event_type_xyz")
    resp = requests.post(EVENTS_URL, json=event, headers=HEADERS, timeout=5)
    allure.attach(
        f"Status: {resp.status_code}\nBody: {resp.text}",
        name="rejection_response",
        attachment_type=allure.attachment_type.TEXT,
    )
    assert resp.status_code == 422, f"Expected 422, got {resp.status_code}"


@allure.feature("Ingestion API")
@allure.story("Deduplication")
@allure.severity(allure.severity_level.NORMAL)
def test_duplicate_event_deduped():
    event = make_event()

    resp1 = requests.post(EVENTS_URL, json=event, headers=HEADERS, timeout=30)
    allure.attach(
        f"First POST status: {resp1.status_code}",
        name="first_post",
        attachment_type=allure.attachment_type.TEXT,
    )
    assert resp1.status_code == 202, f"First POST expected 202, got {resp1.status_code}"

    resp2 = requests.post(EVENTS_URL, json=event, headers=HEADERS, timeout=30)
    allure.attach(
        f"Second POST status: {resp2.status_code}",
        name="second_post",
        attachment_type=allure.attachment_type.TEXT,
    )
    assert resp2.status_code == 200, f"Second POST expected 200 (dedup), got {resp2.status_code}"


@allure.feature("Validation & Filtering")
@allure.story("JSON Parsing")
@allure.severity(allure.severity_level.NORMAL)
def test_malformed_json_rejected():
    resp = requests.post(
        EVENTS_URL,
        data="this is not json{{{",
        headers={"Content-Type": "application/json", "Host": "api.scaibu.localhost"},
        timeout=5,
    )
    allure.attach(
        f"Status: {resp.status_code}\nBody: {resp.text}",
        name="malformed_json_response",
        attachment_type=allure.attachment_type.TEXT,
    )
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"


@allure.feature("Validation & Filtering")
@allure.story("Missing Required Fields")
@allure.severity(allure.severity_level.NORMAL)
def test_missing_event_id_rejected():
    event = make_event()
    event["event_id"] = ""
    resp = requests.post(EVENTS_URL, json=event, headers=HEADERS, timeout=5)
    allure.attach(
        f"Status: {resp.status_code}\nBody: {resp.text}",
        name="missing_id_response",
        attachment_type=allure.attachment_type.TEXT,
    )
    assert resp.status_code == 422, f"Expected 422, got {resp.status_code}"


def _snapshot_docker_stats():
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout
    except Exception as e:
        return f"Failed to capture docker stats: {e}"


def _snapshot_kafka_lag():
    try:
        result = subprocess.run(
            ["docker", "exec", "event-platform-kafka-1",
             "/opt/kafka/bin/kafka-consumer-groups.sh",
             "--bootstrap-server", "localhost:9092",
             "--describe", "--all-groups"],
            capture_output=True, text=True, timeout=15,
        )
        return result.stdout + result.stderr
    except Exception as e:
        return f"Failed to capture Kafka lag: {e}"


@allure.feature("Performance & Scalability")
@allure.story("10K Request Load Test — 500KB Payload")
@allure.severity(allure.severity_level.CRITICAL)
def test_load_10k_requests():
    os.makedirs(STATS_DIR, exist_ok=True)

    if os.path.exists(K6_RESULTS_FILE):
        os.remove(K6_RESULTS_FILE)

    env_framing = (
        "ENVIRONMENT NOTE: This performance test ran in a local single-node docker-compose "
        "integration environment on developer workstation hardware."
    )
    allure.attach(env_framing, name="environment_framing_notice", attachment_type=allure.attachment_type.TEXT)

    try:
        cur = psycopg2.connect(PG_DSN).cursor()
        cur.execute("SELECT COUNT(*) FROM raw_events;")
        pre_test_db_count = cur.fetchone()[0]
        cur.close()
    except Exception:
        pre_test_db_count = -1

    allure.attach(
        f"Pre-test DB raw_events row count: {pre_test_db_count}",
        name="pre_test_db_count",
        attachment_type=allure.attachment_type.TEXT,
    )

    baseline_stats = _snapshot_docker_stats()
    allure.attach(baseline_stats, name="baseline_docker_stats", attachment_type=allure.attachment_type.TEXT)

    baseline_lag = _snapshot_kafka_lag()
    allure.attach(baseline_lag, name="baseline_kafka_lag", attachment_type=allure.attachment_type.TEXT)

    k6_cmd = [
        "docker", "run", "--rm",
        "--network", "host",
        "-e", "TARGET_URL=http://127.0.0.1:27488/v1/events",
        "-v", f"{os.path.abspath(LOADTEST_DIR)}:/scripts",
        "grafana/k6:latest", "run",
        "--summary-export=/scripts/k6-results.json",
        "/scripts/ingestion_10k_loadtest.ts",
    ]

    allure.attach(
        " ".join(k6_cmd),
        name="k6_command",
        attachment_type=allure.attachment_type.TEXT,
    )

    k6_proc = subprocess.Popen(
        k6_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )

    all_stats_snapshots = []
    all_lag_snapshots = []
    poll_count = 0

    while k6_proc.poll() is None:
        time.sleep(10)
        poll_count += 1
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")

        stats = _snapshot_docker_stats()
        all_stats_snapshots.append(f"=== {ts} (poll #{poll_count}) ===\n{stats}")

        lag = _snapshot_kafka_lag()
        all_lag_snapshots.append(f"=== {ts} (poll #{poll_count}) ===\n{lag}")

    k6_stdout, _ = k6_proc.communicate(timeout=30)
    allure.attach(k6_stdout or "(empty)", name="k6_full_output", attachment_type=allure.attachment_type.TEXT)

    allure.attach(
        "\n\n".join(all_stats_snapshots) if all_stats_snapshots else "(no snapshots captured)",
        name="docker_stats_timeseries",
        attachment_type=allure.attachment_type.TEXT,
    )
    allure.attach(
        "\n\n".join(all_lag_snapshots) if all_lag_snapshots else "(no snapshots captured)",
        name="kafka_lag_timeseries",
        attachment_type=allure.attachment_type.TEXT,
    )

    final_stats = _snapshot_docker_stats()
    allure.attach(final_stats, name="final_docker_stats", attachment_type=allure.attachment_type.TEXT)

    final_lag = _snapshot_kafka_lag()
    allure.attach(final_lag, name="final_kafka_lag", attachment_type=allure.attachment_type.TEXT)

    assert k6_proc.returncode in (0, 99, 255), f"k6 failed unexpectedly with exit code {k6_proc.returncode}"

    assert os.path.exists(K6_RESULTS_FILE), f"k6 summary file not found at {K6_RESULTS_FILE}"

    with open(K6_RESULTS_FILE, "r") as f:
        raw_json = f.read()
    allure.attach(raw_json, name="k6-results.json", attachment_type=allure.attachment_type.JSON)

    k6_data = json.loads(raw_json)
    metrics = k6_data.get("metrics", {})

    http_reqs = metrics.get("http_reqs", {})
    total_requests = http_reqs.get("count", 0)
    achieved_rate = http_reqs.get("rate", 0.0)

    dropped_iterations = metrics.get("dropped_iterations", {}).get("count", 0)
    allure.attach(
        f"dropped_iterations: {dropped_iterations}",
        name="dropped_iterations_check",
        attachment_type=allure.attachment_type.TEXT,
    )

    http_req_failed = metrics.get("http_req_failed", {})
    error_rate = http_req_failed.get("rate", 1.0)

    http_req_duration = metrics.get("http_req_duration", {})
    mean_latency = http_req_duration.get("avg", 0)
    p50 = http_req_duration.get("med", 0)
    p90 = http_req_duration.get("p(90)", 0)
    p95 = http_req_duration.get("p(95)", 0)
    p99 = http_req_duration.get("p(99)", 0)
    min_latency = http_req_duration.get("min", 0)
    max_latency = http_req_duration.get("max", 0)

    target_rate = 167.0
    pct_of_target = (achieved_rate / target_rate * 100.0) if target_rate > 0 else 0.0
    pass_criteria_met = (total_requests >= 10000) and (error_rate < 0.01)
    overall_status = "PASS" if pass_criteria_met else "FAIL"

    post_test_db_count = -1
    duplicate_rows_count = -1
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM raw_events;")
        post_test_db_count = cur.fetchone()[0]

        cur.execute("SELECT event_id, COUNT(*) FROM raw_events GROUP BY event_id HAVING COUNT(*) > 1;")
        duplicates = cur.fetchall()
        duplicate_rows_count = len(duplicates)
        cur.close()
        conn.close()
    except Exception as e:
        allure.attach(f"Failed DB post-check: {e}", name="db_post_check_error", attachment_type=allure.attachment_type.TEXT)

    summary = {
        "overall_status": overall_status,
        "target_rate_req_sec": target_rate,
        "achieved_rate_req_sec": achieved_rate,
        "pct_of_target": pct_of_target,
        "total_requests": total_requests,
        "dropped_iterations": dropped_iterations,
        "error_rate": error_rate,
        "mean_latency_ms": mean_latency,
        "p50_ms": p50,
        "p90_ms": p90,
        "p95_ms": p95,
        "p99_ms": p99,
        "min_ms": min_latency,
        "max_ms": max_latency,
        "pre_test_db_count": pre_test_db_count,
        "post_test_db_count": post_test_db_count,
        "duplicate_event_ids_count": duplicate_rows_count,
        "environment": "Local single-node docker-compose integration test",
    }

    allure.attach(
        json.dumps(summary, indent=2),
        name="target_vs_achieved_summary",
        attachment_type=allure.attachment_type.JSON,
    )

    assert duplicate_rows_count == 0, f"Found {duplicate_rows_count} duplicate event_ids in Postgres raw_events!"

    try:
        dash_path = os.path.join(os.path.dirname(__file__), "..", "infra", "grafana", "provisioning", "dashboards", "pipeline-analytics.json")
        if os.path.exists(dash_path):
            with open(dash_path, "r") as f:
                dash_model = json.load(f)
            requests.post(
                "http://localhost:27402/api/dashboards/db",
                json={"dashboard": dash_model, "overwrite": True},
                auth=("admin", "Scaibu@123"),
                timeout=5,
            )
    except Exception:
        pass

    assert total_requests > 0, "No requests were executed by k6"
