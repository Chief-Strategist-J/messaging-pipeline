import http from 'http';

const totalRequests = 1_000_000;
const concurrency = 100;

// Generate exact 500KB string payload directly in code
const padding500KB = 'x'.repeat(500 * 1024);

const payloadData = JSON.stringify({
  event_id: 'test-evt-500kb',
  event_type: 'page_view',
  occurred_at: Date.now(),
  payload: JSON.stringify({ url: '/home', data: padding500KB })
});

const payloadLength = Buffer.byteLength(payloadData);

console.log(`==================================================`);
console.log(`🚀 KAFKA INGESTION PIPELINE 1M LOAD TEST (500KB PAYLOAD)`);
console.log(`==================================================`);
console.log(`Target Endpoint: http://localhost:8080/v1/events`);
console.log(`Total Requests:  ${totalRequests.toLocaleString()}`);
console.log(`Concurrency:     ${concurrency}`);
console.log(`Payload Size:    ${(payloadLength / 1024).toFixed(2)} KB (${payloadLength.toLocaleString()} bytes)`);
console.log(`Total Data Volume: ${((totalRequests * payloadLength) / (1024 * 1024 * 1024)).toFixed(2)} GB`);
console.log(`==================================================\n`);

const options = {
  hostname: 'localhost',
  port: 8080,
  path: '/v1/events',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Content-Length': payloadLength
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
    if (completed % 10000 === 0 || completed === totalRequests) {
      const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(2);
      const rps = (completed / (Date.now() - startTime) * 1000).toFixed(2);
      const mbps = ((completed * payloadLength / (1024 * 1024)) / (Date.now() - startTime) * 1000).toFixed(2);
      console.log(`[PROGRESS] ${completed.toLocaleString()}/${totalRequests.toLocaleString()} (${((completed/totalRequests)*100).toFixed(1)}%) | Elapsed: ${elapsedSec}s | Throughput: ${rps} req/sec (${mbps} MB/sec) | Success: ${success} | Errors: ${errors}`);
    }

    if (completed === totalRequests) {
      const totalTimeSec = (Date.now() - startTime) / 1000;
      const throughput = (totalRequests / totalTimeSec).toFixed(2);
      const dataVolumeGB = ((totalRequests * payloadLength) / (1024 * 1024 * 1024)).toFixed(2);
      const avgBandwidthMBps = ((totalRequests * payloadLength / (1024 * 1024)) / totalTimeSec).toFixed(2);
      
      console.log("\n==================================================");
      console.log(`🎉 1 MILLION REQUEST LOAD TEST COMPLETE (500KB PAYLOAD)`);
      console.log("==================================================");
      console.log(`Total Requests:         ${totalRequests.toLocaleString()}`);
      console.log(`Payload Size per req:   ${(payloadLength / 1024).toFixed(2)} KB`);
      console.log(`Total Data Ingested:    ${dataVolumeGB} GB`);
      console.log(`Successful (202):       ${success.toLocaleString()}`);
      console.log(`Failed:                 ${errors.toLocaleString()}`);
      console.log(`Total Duration:         ${totalTimeSec.toFixed(2)} seconds (${(totalTimeSec / 60).toFixed(2)} minutes)`);
      console.log(`Average Throughput:     ${throughput} req/sec`);
      console.log(`Average Network Bandwidth: ${avgBandwidthMBps} MB/sec`);
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
