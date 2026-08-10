import http from 'k6/http';
import { check } from 'k6';
import { config, options, requestParams, MODE_HEALTH } from './probe-config.js';

export { options };

const STATUS_HEALTH = 200;
const STATUS_INGEST = 202;

const padding = config.payloadKilobytes > 0
  ? 'x'.repeat(config.payloadKilobytes * 1024)
  : 'small';
const seed = Math.random().toString(36).substring(2, 8);
const bodyPrefix = '{"event_id":"';
const bodySuffix = '","event_type":"page_view","occurred_at":' + Date.now()
  + ',"payload":{"url":"/home","data":"' + padding + '"}}';

function sendHealth() {
  return http.get(config.url, requestParams);
}

function sendIngest() {
  return http.post(config.url, bodyPrefix + __VU + '-' + __ITER + '-' + seed + bodySuffix, requestParams);
}

const senders = {
  [MODE_HEALTH]: { send: sendHealth, expected: STATUS_HEALTH },
};

export default function () {
  const sender = senders[config.mode] || { send: sendIngest, expected: STATUS_INGEST };
  const response = sender.send();
  check(response, { [`status ${sender.expected}`]: (r) => r.status === sender.expected });
}
