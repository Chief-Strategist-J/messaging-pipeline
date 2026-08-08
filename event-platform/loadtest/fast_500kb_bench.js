import http from 'http';

const totalRequests = 10_000;
const concurrency = 20;

// 500KB payload constructed directly in memory
const padding500KB = 'x'.repeat(500 * 1024);
const payloadData = JSON.stringify({
  event_id: 'test-evt-500kb',
  event_type: 'page_view',
  occurred_at: Date.now(),
  payload: JSON.stringify({ url: '/home', data: padding500KB })
});
const payloadLength = Buffer.byteLength(payloadData);

console.log(`==================================================`);
console.log(`🚀 500KB PAYLOAD KAFKA INGESTION BENCHMARK`);
console.log(`==================================================`);
console.log(`Target:         http://localhost:8080/v1/events`);
console.log(`Sample Size:    ${totalRequests.toLocaleString()} requests`);
console.log(`Concurrency:    ${concurrency}`);
console.log(`Payload Size:   ${(payloadLength / 1024).toFixed(2)} KB (${payloadLength.toLocaleString()} bytes)`);
console.log(`Total Volume:   ${((totalRequests * payloadLength) / (1024 * 1024)).toFixed(2)} MB`);
console.log(`==================================================\n`);

const options = {
  hostname: 'localhost',
  port: 8080,
  path: '/v1/events',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Content-Length': payloadLength,
    'Connection': 'keep-alive'
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
    if (completed % 2500 === 0 || completed === totalRequests) {
      const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(2);
      const rps = (completed / (Date.now() - startTime) * 1000).toFixed(2);
      const mbps = ((completed * payloadLength / (1024 * 1024)) / (Date.now() - startTime) * 1000).toFixed(2);
      console.log(`[PROGRESS] ${completed.toLocaleString()}/${totalRequests.toLocaleString()} (${((completed/totalRequests)*100).toFixed(1)}%) | Elapsed: ${elapsedSec}s | Throughput: ${rps} req/sec (${mbps} MB/sec)`);
    }

    if (completed === totalRequests) {
      const totalTimeSec = (Date.now() - startTime) / 1000;
      const rps = (totalRequests / totalTimeSec).toFixed(2);
      const mbps = ((totalRequests * payloadLength / (1024 * 1024)) / totalTimeSec).toFixed(2);
      
      const timeFor1M_sec = (1000000 / rps).toFixed(2);
      const timeFor1M_min = (timeFor1M_sec / 60).toFixed(2);
      const volume1M_GB = ((1000000 * payloadLength) / (1024 * 1024 * 1024)).toFixed(2);

      console.log("\n==================================================");
      console.log(`📊 BENCHMARK RESULTS (500KB PAYLOAD)`);
      console.log("==================================================");
      console.log(`Sample Requests:         ${totalRequests.toLocaleString()}`);
      console.log(`Successful (202):        ${success.toLocaleString()}`);
      console.log(`Failed:                  ${errors.toLocaleString()}`);
      console.log(`Execution Duration:      ${totalTimeSec.toFixed(2)} seconds`);
      console.log(`Throughput Rate:         ${rps} req/sec`);
      console.log(`Network Bandwidth:       ${mbps} MB/sec`);
      console.log("--------------------------------------------------");
      console.log(`⏱️ CALCULATED TIME FOR 1,000,000 REQUESTS (500KB):`);
      console.log(`   Total Time (Seconds): ${timeFor1M_sec} s`);
      console.log(`   Total Time (Minutes): ${timeFor1M_min} mins`);
      console.log(`   Total Data Volume:    ${volume1M_GB} GB`);
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

  req.write(payloadData);
  req.end();
}

for (let i = 0; i < concurrency; i++) {
  sendRequest();
}
