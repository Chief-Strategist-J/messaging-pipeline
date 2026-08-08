import http from 'k6/http';
import { check } from 'k6';
import { Options } from 'k6/options';

export const options: Options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 1667,
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 200,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200'],
  },
};

export default function (): void {
  const payload: string = JSON.stringify({
    event_id: `${__VU}-${__ITER}-${Date.now()}`,
    event_type: 'page_view',
    occurred_at: Date.now(),
    payload: '{"url":"/home"}',
  });
  const res = http.post('http://ingestion-api:8080/v1/events', payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
}
