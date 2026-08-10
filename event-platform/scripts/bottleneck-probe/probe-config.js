export const MODE_INGEST = 'ingest';
export const MODE_HEALTH = 'health';

export const config = {
  mode: __ENV.MODE || MODE_INGEST,
  url: __ENV.URL,
  hostHeader: __ENV.HOST_HEADER || 'api.scaibu.localhost',
  vus: parseInt(__ENV.VUS || '50'),
  duration: __ENV.DUR || '30s',
  payloadKilobytes: parseInt(__ENV.PAYLOAD_KB || '0'),
};

export const options = {
  scenarios: {
    probe: {
      executor: 'constant-vus',
      vus: config.vus,
      duration: config.duration,
      gracefulStop: '5s',
    },
  },
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export const requestParams = {
  headers: { 'Content-Type': 'application/json', Host: config.hostHeader },
  timeout: '30s',
};
