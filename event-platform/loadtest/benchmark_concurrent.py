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

TARGET_REQUESTS = 1000
PAYLOAD_SIZE_KB = 200
PAYLOAD_SIZE_BYTES = PAYLOAD_SIZE_KB * 1024
CONCURRENCY_WORKERS = 100  # 100 parallel threads


def generate_random_payload(size_bytes: int) -> dict:
    random_str = "".join(random.choices(string.ascii_letters + string.digits, k=size_bytes))
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "page_view",
        "occurred_at": int(time.time() * 1000),
        "payload": {
            "url": "/benchmark",
            "random_data": random_str
        }
    }


def send_request(session, payload):
    try:
        resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=10)
        return resp.status_code in (200, 202)
    except Exception:
        return False


def run_benchmark():
    print(f"🚀 Starting Concurrent Benchmark: {TARGET_REQUESTS} requests with {CONCURRENCY_WORKERS} workers...")
    
    payloads = [generate_random_payload(PAYLOAD_SIZE_BYTES) for _ in range(TARGET_REQUESTS)]
    sample_size = len(json.dumps(payloads[0]).encode('utf-8'))
    print(f"Exact JSON payload size: {sample_size / 1024:.2f} KB")

    print(f"\nSending {TARGET_REQUESTS} requests concurrently across {CONCURRENCY_WORKERS} worker threads...")
    
    success_count = 0
    fail_count = 0
    start_time = time.perf_counter()
    
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=CONCURRENCY_WORKERS, pool_maxsize=CONCURRENCY_WORKERS)
    session.mount("http://", adapter)
    
    with ThreadPoolExecutor(max_workers=CONCURRENCY_WORKERS) as executor:
        futures = [executor.submit(send_request, session, p) for p in payloads]
        for future in as_completed(futures):
            if future.result():
                success_count += 1
            else:
                fail_count += 1

    end_time = time.perf_counter()
    total_duration = end_time - start_time
    rps = TARGET_REQUESTS / total_duration if total_duration > 0 else 0

    print("\n" + "═" * 60)
    print("           CONCURRENT BENCHMARK RESULTS SUMMARY               ")
    print("═" * 60)
    print(f"Total Requests Executed : {TARGET_REQUESTS}")
    print(f"Exact Payload Size      : ~{sample_size / 1024:.2f} KB (Random)")
    print(f"Parallel Worker Threads : {CONCURRENCY_WORKERS}")
    print(f"Successful Requests (202): {success_count}")
    print(f"Failed Requests         : {fail_count}")
    print(f"Total Time Taken        : {total_duration:.3f} seconds")
    print(f"Achieved Throughput (RPS): {rps:.2f} requests/sec")
    print("═" * 60)


if __name__ == "__main__":
    run_benchmark()
