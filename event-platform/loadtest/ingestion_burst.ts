import http from 'k6/http';
import { check } from 'k6';
import { Options } from 'k6/options';

export const options: Options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 167,
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 200,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<2000'],
  },
};

const padding500KB = 'x'.repeat(500 * 1024);
const PRE_BUILT_PAYLOAD_BODY = JSON.stringify({ url: '/home', data: padding500KB });

export default function (): void {
  const payload: string = JSON.stringify({
    event_id: `${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
    event_type: 'page_view',
    occurred_at: Date.now(),
    payload: PRE_BUILT_PAYLOAD_BODY,
  });
  const res = http.post('http://host.docker.internal:8080/v1/events', payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '15s',
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
}
