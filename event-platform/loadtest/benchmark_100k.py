import json
import os
import random
import string
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

API_URL = "http://localhost:27488/v1/events"
HEADERS = {
    "Content-Type": "application/json",
    "Host": "api.scaibu.localhost",
}

TARGET_REQUESTS = 100000
CONCURRENCY_WORKERS = 100  # 100 parallel worker threads


def send_request(session):
    event_id = str(uuid.uuid4())
    payload = {
        "event_id": event_id,
        "event_type": "page_view",
        "occurred_at": int(time.time() * 1000),
        "payload": {
            "url": f"/benchmark/{event_id}",
            "user_id": f"user_{random.randint(1, 10000)}"
        }
    }
    try:
        resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=10)
        return resp.status_code in (200, 202)
    except Exception:
        return False


def run_benchmark():
    print(f"🚀 Starting 100,000 High-Throughput Load Test across {CONCURRENCY_WORKERS} worker threads...")
    
    success_count = 0
    fail_count = 0
    start_time = time.perf_counter()
    
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=CONCURRENCY_WORKERS, pool_maxsize=CONCURRENCY_WORKERS)
    session.mount("http://", adapter)
    
    with ThreadPoolExecutor(max_workers=CONCURRENCY_WORKERS) as executor:
        futures = [executor.submit(send_request, session) for _ in range(TARGET_REQUESTS)]
        
        for i, future in enumerate(as_completed(futures), 1):
            if future.result():
                success_count += 1
            else:
                fail_count += 1
            
            if i % 10000 == 0:
                elapsed = time.perf_counter() - start_time
                current_rps = i / elapsed if elapsed > 0 else 0
                print(f"  Progress: {i:,}/{TARGET_REQUESTS:,} requests sent | Elapsed: {elapsed:.2f}s | Current RPS: {current_rps:.2f}")

    end_time = time.perf_counter()
    total_duration = end_time - start_time
    rps = TARGET_REQUESTS / total_duration if total_duration > 0 else 0

    print("\n" + "═" * 65)
    print("           100K LOAD TEST RESULTS SUMMARY                     ")
    print("═" * 65)
    print(f"Total Requests Executed  : {TARGET_REQUESTS:,}")
    print(f"Parallel Worker Threads  : {CONCURRENCY_WORKERS}")
    print(f"Successful Requests (202): {success_count:,}")
    print(f"Failed Requests          : {fail_count:,}")
    print(f"Total Time Taken         : {total_duration:.3f} seconds")
    print(f"Achieved Throughput (RPS): {rps:.2f} requests/sec")
    print("═" * 65)


if __name__ == "__main__":
    run_benchmark()
