# Official Allure Single-File HTML Test & Performance Report

**Date**: 2026-08-12  
**Target Environment**: Local Dev Platform (Traefik v3.7.10 + 4 Ingestion API Replicas + Kafka + Redis + Postgres)  
**Official Allure Single-File HTML Report**: [event-platform/loadtest/allure-report-single/index.html](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/loadtest/allure-report-single/index.html)  
**Runbook Registry**: [runbooks/registry.yaml](file:///home/btpl-lap-22/live/messaging-pipeline/runbooks/registry.yaml)  

---

## ⚡ Official Allure Single-File Report

> [!IMPORTANT]
> The report below was generated using official **Allure 2 `--single-file` bundle engine**:
> **[event-platform/loadtest/allure-report-single/index.html](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/loadtest/allure-report-single/index.html)**
>
> All CSS, JS scripts, JSON test results, and evidence attachments are compiled into this **2.8 MB standalone HTML file**. It can be opened directly in any browser (`file://`) without running an Allure server.

---

## 📊 Executed Pytest Allure Test Cases (6 Passed / 0 Failed)

| Test Case Name | Feature Area | Status | Evidence Captured |
| :--- | :--- | :--- | :--- |
| `test_health_check` | Health & Monitoring | **PASSED** | HTTP 200 healthz response |
| `test_single_event_ingestion` | Event Pipeline & Database | **PASSED** | HTTP 202 + Postgres `raw_events` row verification |
| `test_invalid_event_type_rejected` | Validation | **PASSED** | HTTP 422 response |
| `test_duplicate_event_deduped` | Redis Deduplication | **PASSED** | 1st HTTP 202, 2nd HTTP 200 (deduped) |
| `test_malformed_json_rejected` | Validation | **PASSED** | HTTP 422 response |
| `test_missing_event_id_rejected` | Schema Boundary | **PASSED** | HTTP 422 response |

---

## 📈 Empirical Load Test Metrics

| Metric | Heavy 200 KB Payload Test | Standard 1 KB Telemetry Test |
| :--- | :--- | :--- |
| **Total Executed Requests** | **1,000** | **1,000** |
| **Successful Requests (HTTP 202)** | **1,000 / 1,000 (100.0%)** | **1,000 / 1,000 (100.0%)** |
| **Failed Requests** | **0** | **0** |
| **Parallel Worker Threads** | 100 workers | 100 workers |
| **Duration** | **10.616s** | **8.902s** |
| **Throughput (RPS)** | **94.20 req/sec** | **112.33 req/sec** |
