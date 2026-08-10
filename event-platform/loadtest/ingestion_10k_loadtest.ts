import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { Options } from 'k6/options';

export const options: Options = {
  scenarios: {
    ten_k_load_test: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 10000,
      maxDuration: '5m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<2000'],
  },
};

const successCounter = new Counter('successful_ingestions');
const failCounter = new Counter('failed_ingestions');

const PADDING_500KB = 'x'.repeat(500 * 1024);

export default function (): void {
  const payload: string = JSON.stringify({
    event_id: `load10k-${__VU}-${__ITER}-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
    event_type: 'page_view',
    occurred_at: Date.now(),
    payload: {
      url: '/home',
      data: PADDING_500KB,
    },
  });

  const targetUrl = __ENV.TARGET_URL || 'http://127.0.0.1:27488/v1/events';
  const res = http.post(targetUrl, payload, {
    headers: { 'Content-Type': 'application/json', 'Host': 'api.scaibu.localhost' },
    timeout: '15s',
  });

  if (check(res, { 'status is 202': (r) => r.status === 202 })) {
    successCounter.add(1);
  } else {
    failCounter.add(1);
  }
}
