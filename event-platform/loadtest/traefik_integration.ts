/**
 * event-platform — Traefik Integration Load Test Suite
 * Target: api.scaibu.localhost (or configured API_HOST)
 *
 * Tests:
 *   1. Ramp-up: 100 → 500 → 1000 → 1667 → 2500 → 5000 → 10000 RPS
 *   2. Sustained: 1667 RPS for 5 minutes
 *   3. Burst: 3x normal for 30 seconds
 *   4. Failure scenarios (see SCENARIO env var)
 *
 * Usage:
 *   # Normal progressive ramp:
 *   k6 run loadtest/traefik_integration.ts
 *
 *   # Sustained 1667 RPS:
 *   SCENARIO=sustained k6 run loadtest/traefik_integration.ts
 *
 *   # Burst test:
 *   SCENARIO=burst k6 run loadtest/traefik_integration.ts
 *
 *   # Against production:
 *   API_HOST=api.scaibu.com k6 run loadtest/traefik_integration.ts
 *
 * Run k6 inside Docker to use Docker DNS for *.localhost resolution:
 *   docker run --rm -it --network event-platform_backbone \
 *     -v $(pwd)/loadtest:/loadtest \
 *     grafana/k6 run /loadtest/traefik_integration.ts \
 *     -e API_HOST=ingestion-api:8080   # direct, bypass Traefik for baseline
 *
 *   Or point at host port via host.docker.internal for Traefik-integrated test:
 *   docker run --rm -it \
 *     -v $(pwd)/loadtest:/loadtest \
 *     --add-host host.docker.internal:host-gateway \
 *     grafana/k6 run /loadtest/traefik_integration.ts \
 *     -e API_HOST=host.docker.internal
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { Options } from 'k6/options';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const API_HOST: string = __ENV.API_HOST || 'host.docker.internal';
const API_PORT: string = __ENV.API_PORT || '27488';
const PROTOCOL: string = __ENV.PROTOCOL || 'http';
const BASE_URL: string = `${PROTOCOL}://${API_HOST}${API_PORT === '80' ? '' : ':' + API_PORT}`;
const ENDPOINT: string = `${BASE_URL}/v1/events`;
const SCENARIO: string = __ENV.SCENARIO || 'rampup';

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const successCounter   = new Counter('successful_ingestions');
const failCounter      = new Counter('failed_ingestions');
const rateLimitCounter = new Counter('rate_limit_429');
const cbCounter        = new Counter('circuit_breaker_503');
const ingestLatency    = new Trend('ingest_latency_ms', true);  // percentile-capable

// ---------------------------------------------------------------------------
// Shared payload factory
// Realistic payload size ~1KB (not the 500KB used in prior tests).
// 500KB per request * 1667 RPS = ~800 MB/s — Traefik/Kafka would saturate.
// Use realistic payload sizes for throughput tests.
// ---------------------------------------------------------------------------
function makePayload(vu: number, iter: number): string {
  return JSON.stringify({
    event_id: `traefik-lt-${vu}-${iter}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
    event_type: 'page_view',
    occurred_at: Date.now(),
    payload: {
      url: '/home',
      session_id: `sess-${vu}`,
      user_agent: 'k6-load-test/1.0',
      referrer: 'https://scaibu.com',
    },
  });
}

const HEADERS = { 'Content-Type': 'application/json' };

// ---------------------------------------------------------------------------
// Scenario definitions
// ---------------------------------------------------------------------------

const rampupOptions: Options = {
  scenarios: {
    // Progressive ramp tests the gateway's scaling behaviour at each tier.
    // Each stage is long enough to reach steady-state (≥ 60s).
    rampup: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 5000,
      stages: [
        { duration: '60s',  target: 100   },  // baseline
        { duration: '60s',  target: 500   },  // moderate load
        { duration: '60s',  target: 1000  },  // approaching target
        { duration: '120s', target: 1667  },  // TARGET: 1M req / 10 min
        { duration: '60s',  target: 2500  },  // 50% above target
        { duration: '60s',  target: 5000  },  // 3× target
        { duration: '60s',  target: 10000 },  // 6× target — find the ceiling
        { duration: '60s',  target: 100   },  // cool-down
      ],
    },
  },
  thresholds: {
    // Success criteria — adjust after baseline benchmarking
    http_req_failed:           ['rate<0.01'],   // <1% errors
    http_req_duration:         ['p(95)<500', 'p(99)<2000'],
    'http_req_duration{route:api}': ['p(95)<500'],
    rate_limit_429:            ['count<100'],   // track but don't fail on 429
  },
};

const sustainedOptions: Options = {
  scenarios: {
    sustained: {
      executor: 'constant-arrival-rate',
      rate: 1667,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 3000,
      maxVUs: 5000,
    },
  },
  thresholds: {
    http_req_failed:   ['rate<0.005'],  // tighter threshold for sustained
    http_req_duration: ['p(95)<300', 'p(99)<1000'],
  },
};

const burstOptions: Options = {
  scenarios: {
    // Normal baseline runs concurrently with sudden spike
    baseline: {
      executor: 'constant-arrival-rate',
      rate: 1667,
      timeUnit: '1s',
      duration: '3m',
      preAllocatedVUs: 3000,
      maxVUs: 3500,
    },
    burst: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 5000,
      maxVUs: 8000,
      stages: [
        { duration: '60s',  target: 0    },  // wait during baseline warmup
        { duration: '5s',   target: 5000 },  // sudden 3× burst
        { duration: '30s',  target: 5000 },  // sustain burst
        { duration: '5s',   target: 0    },  // drop back
        { duration: '60s',  target: 0    },  // observe recovery
      ],
      startTime: '0s',
    },
  },
  thresholds: {
    http_req_failed:   ['rate<0.05'],   // 5% error budget during burst
    http_req_duration: ['p(95)<2000'],
  },
};

export const options: Options = (() => {
  switch (SCENARIO) {
    case 'sustained': return sustainedOptions;
    case 'burst':     return burstOptions;
    default:          return rampupOptions;
  }
})();

// ---------------------------------------------------------------------------
// Default test function — called once per VU per iteration
// ---------------------------------------------------------------------------
export default function (): void {
  const payload = makePayload(__VU, __ITER);
  const startTs = Date.now();

  const res = http.post(ENDPOINT, payload, {
    headers: HEADERS,
    timeout: '15s',
    tags: { route: 'api' },
  });

  const durationMs = Date.now() - startTs;
  ingestLatency.add(durationMs);

  const ok = check(res, {
    'status is 202 (accepted)':     (r) => r.status === 202,
    'status is 200 (dedup replay)': (r) => r.status === 200,
    'not a server error':           (r) => r.status < 500,
    'not rate limited':             (r) => r.status !== 429,
  });

  if (res.status === 202 || res.status === 200) {
    successCounter.add(1);
  } else {
    failCounter.add(1);
    if (res.status === 429) {
      rateLimitCounter.add(1);
    }
    if (res.status === 503) {
      cbCounter.add(1);
    }
  }
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------
export function setup(): void {
  // Verify the API is reachable before starting
  const probe = http.get(`${BASE_URL}/healthz`, { timeout: '5s' });
  if (probe.status !== 200) {
    throw new Error(`API not healthy at ${BASE_URL}/healthz — got ${probe.status}. Is Traefik running?`);
  }
  console.log(`Load test target: ${ENDPOINT} | Scenario: ${SCENARIO}`);
}

export function teardown(): void {
  console.log('Load test complete. Review Grafana → Traefik dashboard for gateway metrics.');
}
