import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

export const options = {
  scenarios: {
    load_test: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 10000,
      maxDuration: '10m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const successCounter = new Counter('successful_ingestions');
const failCounter = new Counter('failed_ingestions');

const padding500KB = 'x'.repeat(500 * 1024);

export default function () {
  const payload = JSON.stringify({
    event_id: `load-${__VU}-${__ITER}-${Date.now()}`,
    event_type: 'page_view',
    occurred_at: Date.now(),
    payload: JSON.stringify({ url: '/home', data: padding500KB }),
  });

  const res = http.post('http://host.docker.internal:8080/v1/events', payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
  });

  if (check(res, { 'status is 202': (r) => r.status === 202 })) {
    successCounter.add(1);
  } else {
    failCounter.add(1);
  }
}
