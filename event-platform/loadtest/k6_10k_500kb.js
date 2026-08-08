import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

export const options = {
  scenarios: {
    constant_rate_load: {
      executor: 'constant-arrival-rate',
      rate: 35,                   // Sustained target rate: 35 req/s (~17.5 MB/s payload bandwidth)
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 50,
      maxVUs: 100,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<2000'],
  },
};

const successCounter = new Counter('successful_ingestions');
const failCounter = new Counter('failed_ingestions');

// FIX 2: Pre-built 500KB payload reference outside default function
const PADDING_500KB = 'x'.repeat(500 * 1024);
const PRE_BUILT_PAYLOAD_BODY = JSON.stringify({ url: '/home', data: PADDING_500KB });

export default function () {
  const payload = JSON.stringify({
    event_id: `load-${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
    event_type: 'page_view',
    occurred_at: Date.now(),
    payload: PRE_BUILT_PAYLOAD_BODY,
  });

  const res = http.post('http://host.docker.internal:8080/v1/events', payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '15s',
  });

  if (check(res, { 'status is 202': (r) => r.status === 202 })) {
    successCounter.add(1);
  } else {
    failCounter.add(1);
  }
}
