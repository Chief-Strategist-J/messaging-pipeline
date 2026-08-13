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

TARGET_REQUESTS = 10000
PAYLOAD_SIZE_KB = 200
PAYLOAD_SIZE_BYTES = PAYLOAD_SIZE_KB * 1024
CONCURRENCY_WORKERS = 50


def generate_base_payload(size_bytes: int) -> tuple[dict, str]:
    random_str = "".join(random.choices(string.ascii_letters + string.digits, k=size_bytes))
    return random_str


def make_payload(random_str: str) -> dict:
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
        resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=15)
        return resp.status_code in (200, 202)
    except Exception:
        return False


def run_benchmark():
    print(f"🚀 Starting Benchmark: {TARGET_REQUESTS:,} requests with {PAYLOAD_SIZE_KB} KB payload across {CONCURRENCY_WORKERS} workers...")
    
    print("Pre-generating random data payload...")
    base_random_str = generate_base_payload(PAYLOAD_SIZE_BYTES)
    payloads = [make_payload(base_random_str) for _ in range(TARGET_REQUESTS)]
    
    sample_size = len(json.dumps(payloads[0]).encode('utf-8'))
    print(f"Exact JSON payload size: {sample_size / 1024:.2f} KB")

    print(f"\nSending {TARGET_REQUESTS:,} requests to {API_URL}...")
    success_count = 0
    fail_count = 0
    
    start_time = time.perf_counter()
    
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=CONCURRENCY_WORKERS, pool_maxsize=CONCURRENCY_WORKERS)
    session.mount("http://", adapter)
    
    with ThreadPoolExecutor(max_workers=CONCURRENCY_WORKERS) as executor:
        futures = [executor.submit(send_request, session, p) for p in payloads]
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if future.result():
                success_count += 1
            else:
                fail_count += 1

            if completed % 1000 == 0:
                elapsed = time.perf_counter() - start_time
                current_rps = completed / elapsed if elapsed > 0 else 0
                print(f"  Processed {completed:,}/{TARGET_REQUESTS:,} requests ({elapsed:.2f}s elapsed | {current_rps:.1f} RPS)...", flush=True)

    end_time = time.perf_counter()
    total_duration = end_time - start_time
    rps = TARGET_REQUESTS / total_duration if total_duration > 0 else 0

    print("\n" + "═" * 60)
    print("                10K BENCHMARK RESULTS SUMMARY                 ")
    print("═" * 60)
    print(f"Total Requests Executed  : {TARGET_REQUESTS:,}")
    print(f"Exact Payload Size       : ~{sample_size / 1024:.2f} KB")
    print(f"Parallel Worker Threads  : {CONCURRENCY_WORKERS}")
    print(f"Successful Requests (202): {success_count:,}")
    print(f"Failed Requests          : {fail_count:,}")
    print(f"Total Time Taken         : {total_duration:.3f} seconds")
    print(f"Throughput (RPS)         : {rps:.2f} requests/sec")
    print("═" * 60)


if __name__ == "__main__":
    run_benchmark()

