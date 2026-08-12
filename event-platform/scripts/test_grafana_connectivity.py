#!/usr/bin/env python3
import json
import requests
import sys

GRAFANA_URL = "http://localhost:27402"
AUTH = ("admin", "Scaibu@123")

def test_grafana_health():
    print("Testing Grafana Service Connection...")
    try:
        res = requests.get(f"{GRAFANA_URL}/api/health", auth=AUTH, timeout=5)
        if res.status_code == 200:
            print("  ✅ Grafana UI API: Healthy (200 OK)")
        else:
            print(f"  ❌ Grafana UI API: Unhealthy (Status {res.status_code})")
            return False
    except Exception as e:
        print(f"  ❌ Grafana UI API: Connection Failed ({e})")
        return False
    return True

def test_grafana_datasources():
    print("\nTesting Grafana Configured Datasources...")
    required_datasources = {"prometheus": "Prometheus", "tempo": "Tempo"}
    
    try:
        res = requests.get(f"{GRAFANA_URL}/api/datasources", auth=AUTH, timeout=5)
        if res.status_code != 200:
            print(f"  ❌ Failed to fetch datasources: {res.status_code}")
            return False
        
        ds_list = res.json()
        ds_map = {ds["uid"]: ds for ds in ds_list}
        
        all_ok = True
        for uid, name in required_datasources.items():
            if uid in ds_map:
                print(f"  ✅ Datasource Registered: '{ds_map[uid]['name']}' (type: {ds_map[uid]['type']})")
                
                proxy_res = requests.get(f"{GRAFANA_URL}/api/datasources/uid/{uid}/health", auth=AUTH, timeout=5)
                if proxy_res.status_code == 200:
                    print(f"     ✅ Connection to target service ({name}): Verified & Connected!")
                else:
                    print(f"     ❌ Connection to target service ({name}): Failed! Status {proxy_res.status_code}")
                    all_ok = False
            else:
                print(f"  ❌ Missing Datasource: '{name}' (uid: {uid})")
                all_ok = False
        return all_ok
    except Exception as e:
        print(f"  ❌ Error verifying datasources: {e}")
        return False

def test_tempo_service_traces():
    print("\nTesting Live Traces in Tempo Datasource...")
    try:
        query_url = f"{GRAFANA_URL}/api/datasources/proxy/uid/tempo/api/search?limit=10"
        res = requests.get(query_url, auth=AUTH, timeout=5)
        if res.status_code == 200:
            traces = res.json().get("traces", [])
            print(f"  ✅ Tempo Trace Storage: Connected! ({len(traces)} traces available)")
            return True
        else:
            print(f"  ❌ Tempo Trace Storage: Query returned status {res.status_code}")
            return False
    except Exception as e:
        print(f"  ❌ Error querying Tempo traces: {e}")
        return False

if __name__ == "__main__":
    print("==================================================")
    print(" GRAFANA & SERVICE CONNECTIVITY VERIFICATION TEST ")
    print("==================================================")
    
    g_ok = test_grafana_health()
    ds_ok = test_grafana_datasources()
    t_ok = test_tempo_service_traces()
    
    print("\n==================================================")
    if g_ok and ds_ok and t_ok:
        print("🎉 SUMMARY: ALL SERVICES ARE 100% CONNECTED TO GRAFANA!")
        sys.exit(0)
    else:
        print("❌ SUMMARY: SERVICE CONNECTIVITY ERRORS DETECTED!")
        sys.exit(1)
