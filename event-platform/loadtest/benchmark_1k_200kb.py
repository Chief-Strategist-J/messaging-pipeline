import json
import os
import random
import string
import time
import uuid
import requests

API_URL = "http://localhost:27488/v1/events"
HEADERS = {
    "Content-Type": "application/json",
    "Host": "api.scaibu.localhost",
}

TARGET_REQUESTS = 1000
PAYLOAD_SIZE_KB = 200
PAYLOAD_SIZE_BYTES = PAYLOAD_SIZE_KB * 1024


def generate_random_payload(size_bytes: int) -> dict:
    # Generate random printable alphanumeric string of exact length
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


def run_benchmark():
    print(f"🚀 Starting Benchmark: {TARGET_REQUESTS} requests with strictly random {PAYLOAD_SIZE_KB} KB payload...")
    
    # Pre-generate random payloads to measure exact network/server HTTP latency without generator overhead
    print("Generating random payloads...")
    payloads = [generate_random_payload(PAYLOAD_SIZE_BYTES) for _ in range(TARGET_REQUESTS)]
    
    # Calculate exact serialized JSON byte size of first sample
    sample_size = len(json.dumps(payloads[0]).encode('utf-8'))
    print(f"Exact JSON payload size: {sample_size / 1024:.2f} KB")

    print(f"\nSending {TARGET_REQUESTS} requests to {API_URL}...")
    success_count = 0
    fail_count = 0
    
    start_time = time.perf_counter()
    
    with requests.Session() as session:
        for i, payload in enumerate(payloads, start=1):
            try:
                resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=10)
                if resp.status_code in (200, 202):
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1

            if i % 100 == 0:
                elapsed = time.perf_counter() - start_time
                print(f"  Processed {i}/{TARGET_REQUESTS} requests ({elapsed:.2f}s elapsed)...")

    end_time = time.perf_counter()
    total_duration = end_time - start_time
    rps = TARGET_REQUESTS / total_duration if total_duration > 0 else 0

    print("\n" + "═" * 60)
    print("                BENCHMARK RESULTS SUMMARY                     ")
    print("═" * 60)
    print(f"Total Requests Executed : {TARGET_REQUESTS}")
    print(f"Exact Payload Size      : ~{sample_size / 1024:.2f} KB (Random)")
    print(f"Successful Requests (202): {success_count}")
    print(f"Failed Requests         : {fail_count}")
    print(f"Total Time Taken        : {total_duration:.3f} seconds")
    print(f"Throughput (RPS)        : {rps:.2f} requests/sec")
    print("═" * 60)


if __name__ == "__main__":
    run_benchmark()
