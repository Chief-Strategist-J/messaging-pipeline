# JSON Deserialization CPU Optimization Report

## Executive Summary & Verdict

```
RESULT: FAIL — achieved 77.24 req/s / target 167 (46.25%), CPU peak 79.17% (was 394%), p95 3605.93ms (was 7018.35ms, threshold 2000ms), error_rate 0.000
```

> [!NOTE]
> **Summary of Gains**: While the single-instance test fell short of the 167 req/s target (achieving 77.24 req/s under sustained 500KB JSON payload ingestion), replacing `encoding/json.Unmarshal` with `buger/jsonparser` yielded a **+32.6% throughput increase**, reduced median latency by **~50%** (3.40s → 1.75s), reduced p95 latency by **~49%** (7.02s → 3.61s), and reduced peak CPU utilization of `ingestion-api` from **394%** down to **79.17%**.

---

## 1. Profile Comparison (pprof)

### 1.1 Before Optimization
- Raw Profile: [`reports/raw/profile-before.txt`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/profile-before.txt)
- Flame Graph: [`reports/raw/profile-before.svg`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/profile-before.svg)

Top cumulative CPU consumers during 500KB payload burst load test:
```
      flat  flat%   sum%        cum   cum%
         0     0%     0%     27.72s 92.55%  net/http.(*conn).serve
         0     0%     0%     27.53s 91.92%  net/http.(*ServeMux).ServeHTTP
         0     0%     0%     27.52s 91.89%  event-platform/ingestion-api/internal/httpapi.(*Handler).ServeHTTP
         0     0%     0%     15.10s 50.42%  encoding/json.(*Decoder).Decode
     0.01s 0.033% 0.033%     11.57s 38.63%  event-platform/ingestion-api/internal/eventtypes.ValidatePayload
         0     0% 0.033%     11.27s 37.63%  encoding/json.Unmarshal
     5.15s 17.20% 17.23%      8.95s 29.88%  encoding/json.(*Decoder).readValue
     6.23s 20.80% 38.03%      7.07s 23.61%  encoding/json.unquoteBytes
     4.25s 14.19% 52.22%      6.84s 22.84%  encoding/json.checkValid
```
*Key Finding*: `ValidatePayload` (`encoding/json.Unmarshal`) consumed **38.63%** of total CPU time parsing full 500KB payloads into `map[string]interface{}` graphs.

### 1.2 After Optimization
- Raw Profile: [`reports/raw/profile-after.txt`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/profile-after.txt)
- Flame Graph: [`reports/raw/profile-after.svg`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/profile-after.svg)

Top cumulative CPU consumers after replacing full unmarshal with `buger/jsonparser`:
```
      flat  flat%   sum%        cum   cum%
     0.01s 0.048% 0.048%     17.89s 85.27%  net/http.(*conn).serve
         0     0% 0.048%     17.72s 84.46%  event-platform/ingestion-api/internal/httpapi.WithTracing.func1
         0     0% 0.048%     17.72s 84.46%  net/http.(*ServeMux).ServeHTTP
         0     0% 0.048%     17.72s 84.46%  net/http.HandlerFunc.ServeHTTP
         0     0% 0.048%     17.72s 84.46%  net/http.serverHandler.ServeHTTP
         0     0% 0.048%     17.68s 84.27%  event-platform/ingestion-api/internal/httpapi.(*Handler).ServeHTTP
         0     0% 0.048%     17.68s 84.27%  main.main.WithRateLimit.func4
         0     0% 0.048%     15.73s 74.98%  encoding/json.(*Decoder).Decode
     4.72s 22.50% 22.55%      8.65s 41.23%  encoding/json.(*Decoder).readValue
         0     0% 22.55%      7.08s 33.75%  encoding/json.(*decodeState).unmarshal
         0     0% 22.55%      7.05s 33.60%  encoding/json.(*decodeState).object
     3.94s 18.78% 41.33%      4.63s 22.07%  encoding/json.unquoteBytes
         0     0% 69.49%      1.87s  8.91%  github.com/twmb/franz-go/pkg/kgo.(*broker).handleReq
```
*Key Finding*: `ValidatePayload` and `json.Unmarshal` **dropped completely out of the top 20 functions**.

---

## 2. Microbenchmark Results

- Raw Before Output: [`reports/raw/bench-before.txt`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/bench-before.txt)
- Raw After Output: [`reports/raw/bench-after.txt`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/bench-after.txt)

| Function / Payload | Metric | Before (json.Unmarshal) | After (jsonparser) | Change |
|---|---|---|---|---|
| `ValidatePayloadWithRules` (~65B) | ns/op | 8,575 ns/op | 402.9 ns/op | **~21.3x faster** |
| | B/op | 720 B/op | 80 B/op | **-88.9% bytes** |
| | allocs/op | 15 allocs/op | 1 alloc/op | **-93.3% allocs** |
| `ValidatePayloadLargePayload` (~1KB) | ns/op | 29,271 ns/op | 4,940 ns/op | **~5.9x faster** |
| | B/op | 2,728 B/op | 1,152 B/op | **-57.8% bytes** |
| | allocs/op | 11 allocs/op | 1 alloc/op | **-90.9% allocs** |
| `ValidatePayload_500KB` (500KB) | ns/op | N/A (unmarshal) | 321,296 ns/op | Baseline established |
| | B/op | N/A | 516,096 B/op | Single slice allocation |
| | allocs/op | N/A | 1 alloc/op | **1 allocation total** |
| `PurchaseEnrichment_500KB` (500KB) | ns/op | N/A (unmarshal+marshal) | 3,545,467 ns/op | Baseline established |
| | B/op | N/A | 1,548,606 B/op | In-place byte splice |
| | allocs/op | N/A | 5 allocs/op | 5 allocations total |

---

## 3. Load Test Comparison (k6 500KB Payload Burst)

- Baseline Results JSON: [`reports/raw/k6-results.json`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/k6-results.json)
- Post-Fix Results JSON: [`reports/raw/k6-results-after-fix.json`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/k6-results-after-fix.json)
- Container Stats: [`reports/raw/docker-stats.log`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/reports/raw/docker-stats.log)

| Metric | Baseline | Post-Fix | Improvement / Delta |
|---|---|---|---|
| Achieved Rate (`iterations.rate`) | 58.21 req/s | **77.24 req/s** | **+32.6% throughput** |
| Total Completed Reqs (`http_reqs`) | 3,512 | **4,895** | **+39.4% completed** |
| Median Latency (`med`) | 3,399.5 ms | **1,750.6 ms** | **-48.5% lower** |
| p90 Latency (`p90`) | 6,051.9 ms | **3,215.7 ms** | **-46.9% lower** |
| p95 Latency (`p95`) | 7,018.4 ms | **3,605.9 ms** | **-48.6% lower** |
| Max Latency (`max`) | 11,205.2 ms | **4,742.4 ms** | **-57.7% lower** |
| Peak CPU (`ingestion-api`) | **394%** (saturated) | **79.17%** | **79.9% CPU headroom recovered** |
| Dropped Iterations | 6,511 | **5,062** | **-22.3% dropped** |
| Failure Rate (`http_req_failed`) | 0.00% | 0.00% | 0.00% errors |
| Measured Bandwidth | 29.8 MB/s | **39.6 MB/s** | **+32.8% network throughput** |

---

## 4. Next Bottleneck Analysis

With `ValidatePayload` and `PurchaseEnrichment` full-unmarshaling costs eliminated:
- **Dominant Function**: `encoding/json.(*Decoder).Decode` at line 28 of [`services/ingestion-api/internal/httpapi/handler.go`](file:///home/btpl-lap-22/live/messaging-pipeline/event-platform/services/ingestion-api/internal/httpapi/handler.go#L28).
- **Explanation**: `json.NewDecoder(r.Body).Decode(&evt)` deserializes the outer `RawEvent` wrapper (`{event_id, event_type, occurred_at, payload}`) from the HTTP request body. For a 500KB request, `Decode` spends **74.98%** of total CPU time allocating and unquoting string fields in `evt.Payload`.
- **Recommended Next Action**: Replace standard `encoding/json.NewDecoder` for the outer `RawEvent` wrapper with streaming `jsonparser` field extraction on `r.Body` directly, or adjust `RawEvent.Payload` to `json.RawMessage` to prevent unnecessary unquoting of string-escaped inner JSON payloads.
