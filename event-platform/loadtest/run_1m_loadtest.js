import http from 'http';

const totalRequests = 1_000_000;
const concurrency = 200;
const targetUrl = 'http://localhost:8080/v1/events';

const payload = JSON.stringify({
  event_id: 'test-evt',
  event_type: 'page_view',
  occurred_at: Date.now(),
  payload: JSON.stringify({ url: '/home', data: 'load-test-sample' })
});

const options = {
  hostname: 'localhost',
  port: 8080,
  path: '/v1/events',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(payload)
  },
  agent: new http.Agent({ keepAlive: true, maxSockets: concurrency })
};

let completed = 0;
let success = 0;
let errors = 0;
const startTime = Date.now();

function sendRequest() {
  if (completed >= totalRequests) return;

  const req = http.request(options, (res) => {
    if (res.statusCode === 202) success++;
    else errors++;

    completed++;
    if (completed % 100000 === 0) {
      const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(2);
      const rps = (completed / elapsedSec).toFixed(0);
      console.log(`Progress: ${completed}/${totalRequests} (${((completed/totalRequests)*100).toFixed(0)}%) - Elapsed: ${elapsedSec}s - RPS: ${rps}`);
    }

    if (completed === totalRequests) {
      const totalTimeSec = (Date.now() - startTime) / 1000;
      const throughput = (totalRequests / totalTimeSec).toFixed(2);
      console.log("\n==================================================");
      console.log(`🎉 1 MILLION REQUEST INGESTION BENCHMARK COMPLETE`);
      console.log("==================================================");
      console.log(`Total Requests: ${totalRequests}`);
      console.log(`Successful (202 Accepted): ${success}`);
      console.log(`Failed: ${errors}`);
      console.log(`Total Duration: ${totalTimeSec.toFixed(2)} seconds`);
      console.log(`Throughput: ${throughput} req/sec`);
      console.log("==================================================");
    } else {
      sendRequest();
    }
  });

  req.on('error', (err) => {
    errors++;
    completed++;
    sendRequest();
  });

  req.write(payload);
  req.end();
}

console.log(`Starting 1,000,000 requests load test to Ingestion API (Kafka pipeline) with concurrency ${concurrency}...`);
for (let i = 0; i < concurrency; i++) {
  sendRequest();
}
