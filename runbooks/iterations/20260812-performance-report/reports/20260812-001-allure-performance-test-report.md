# 20260812-001: Allure Test Suite & Empirical Ingestion Performance Report

**Date**: 2026-08-12  
**Target Environment**: Local Dev Platform (Traefik v3.7.10 + 4 Ingestion API Replicas + Kafka + Redis + Postgres)  
**Single-File Allure HTML Report**: [runbooks/reports/20260812-001-allure-performance-test-report.html](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/reports/20260812-001-allure-performance-test-report.html)  
**Associated Test Scripts**: [runbooks/reports/scripts/](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/reports/scripts/)  
**Runbook Registry**: [runbooks/registry.yaml](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/registry.yaml)  

---

## 📄 Standalone Single-Page Allure Report Notice

> [!IMPORTANT]
> The single-page report below is packaged as a **self-contained single-file HTML document**:
> **[runbooks/reports/20260812-001-allure-performance-test-report.html](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/reports/20260812-001-allure-performance-test-report.html)**
>
> You can open or share this `.html` file directly in any web browser (`file://`) without running an Allure server.

---

## 📊 Summary of Executed Pytest Allure Test Cases (6 Passed / 0 Failed)

| Test Case Name | Feature Category | Status | Execution Time | Assertions & Evidence Captured |
| :--- | :--- | :--- | :--- | :--- |
| `test_health_check` | Health & Monitoring | **PASSED** | 0.12s | Verified `HTTP 200 OK` healthz response from Traefik |
| `test_single_event_ingestion` | Event Pipeline & Database | **PASSED** | 2.15s | Verified `HTTP 202 Accepted` + SQL query poll matching `event_id` in Postgres `raw_events` table |
| `test_invalid_event_type_rejected` | Validation & Filtering | **PASSED** | 0.18s | Verified unregistered event type returns `HTTP 422 Unprocessable Entity` |
| `test_duplicate_event_deduped` | Deduplication Semantics | **PASSED** | 0.42s | Verified first request returns `HTTP 202` and second request returns `HTTP 200 (Deduped)` |
| `test_malformed_json_rejected` | Validation & Filtering | **PASSED** | 0.15s | Verified non-JSON malformed string payload returns `HTTP 422` error |
| `test_missing_event_id_rejected` | Validation & Filtering | **PASSED** | 0.14s | Verified payload missing required `event_id` is rejected with `HTTP 422` |

**Overall Test Suite Status**: **6 PASSED / 0 FAILED (100.0% Pass Rate)**

---

## ⚡ Empirical Load Test Metrics

| Metric | Heavy 200 KB Payload Test | Standard 1 KB Telemetry Test |
| :--- | :--- | :--- |
| **Total Executed Requests** | **1,000** | **1,000** |
| **Successful Requests (HTTP 202)** | **1,000 / 1,000 (100.0%)** | **1,000 / 1,000 (100.0%)** |
| **Failed Requests** | **0** | **0** |
| **Parallel Worker Threads** | 100 workers | 100 workers |
| **Duration** | **10.616s** | **8.902s** |
| **Throughput (RPS)** | **94.20 req/sec** | **112.33 req/sec** |
