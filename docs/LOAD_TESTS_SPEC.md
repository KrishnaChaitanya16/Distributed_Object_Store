# MiniDFS Load Test Specification

This document specifies the load tests for MiniDFS. The goal is to show how the
system behaves under realistic concurrent load — not just that it works for one
request at a time, but that it holds up under production-like traffic patterns.

**Owner:** Vignesh (teammate)
**Output:** Results table in `LOAD_TEST_RESULTS.md` + k6 scripts in `test/load/`

---

## Prerequisites

### Start MiniDFS locally
```bash
cd deploy
docker compose up --build
```
Controller at `http://localhost:8080`, Web UI at `http://localhost:8081`.

### Install k6
```bash
brew install k6          # macOS
# or
docker pull grafana/k6
```

---

## Test 1: Upload Throughput Ramp

### Goal
Show how upload throughput scales with concurrency. Find the saturation point
where adding more clients stops helping.

### Script: `test/load/upload_ramp.js`

```javascript
import http from 'k6/http';
import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const uploadErrors = new Rate('upload_errors');
const uploadDuration = new Trend('upload_duration_ms');

export const options = {
  stages: [
    { duration: '30s', target: 1  },   // baseline: 1 client
    { duration: '30s', target: 10 },   // ramp to 10
    { duration: '30s', target: 25 },   // ramp to 25
    { duration: '30s', target: 50 },   // ramp to 50
    { duration: '30s', target: 50 },   // hold at 50
    { duration: '30s', target: 0  },   // ramp down
  ],
};

// Generate ~100KB of random-ish content per upload
function makePayload(size) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i = 0; i < size; i++) {
    s += chars[Math.floor(Math.random() * chars.length)];
  }
  return s;
}

export default function () {
  const payload = makePayload(100 * 1024); // 100KB
  const form = {
    file: http.file(payload, `load-test-${__VU}-${__ITER}.txt`, 'text/plain'),
  };

  const start = Date.now();
  const res = http.post('http://localhost:8080/api/v1/objects', form);
  uploadDuration.add(Date.now() - start);

  const ok = check(res, {
    'status is 201': (r) => r.status === 201,
    'has object_id': (r) => JSON.parse(r.body).object_id !== undefined,
  });
  uploadErrors.add(!ok);
}
```

### Metrics to record
- Throughput (requests/sec) at each concurrency level
- P50 / P95 / P99 upload latency
- Error rate at each level
- Point where error rate exceeds 1% (saturation point)

### Expected shape
Throughput increases linearly up to ~10-20 clients, then flattens as bbolt
serializes metadata writes. Report the saturation point honestly.

---

## Test 2: Download Throughput (Zipf Distribution)

### Goal
Show download performance under realistic access patterns. Real object stores
have hot objects — 20% of objects get 80% of requests (Zipf distribution).

### Script: `test/load/download_zipf.js`

```javascript
import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const downloadDuration = new Trend('download_duration_ms');

// Pre-upload 100 objects and store their IDs in a shared array.
// Run this setup script first: node test/load/setup_objects.js
const objectIDs = JSON.parse(open('./object_ids.json'));

export const options = {
  scenarios: {
    zipf_reads: {
      executor: 'constant-vus',
      vus: 50,
      duration: '2m',
    },
  },
};

// Zipf-like: first objects get hit exponentially more often
function zipfIndex(n) {
  // Simple approximation: weight by 1/rank
  const r = Math.random();
  return Math.floor(n * (1 - Math.sqrt(1 - r)));
}

export default function () {
  const idx = zipfIndex(objectIDs.length);
  const objectID = objectIDs[idx];

  const start = Date.now();
  const res = http.get(`http://localhost:8080/api/v1/objects/${objectID}`);
  downloadDuration.add(Date.now() - start);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'body not empty': (r) => r.body.length > 0,
  });
}
```

### Setup script: `test/load/setup_objects.js`
Pre-upload 100 objects and save their IDs to `object_ids.json`:
```javascript
// Run with: node test/load/setup_objects.js
const http = require('http');
const fs = require('fs');
// ... upload 100 objects, save IDs to object_ids.json
```

### Metrics to record
- P50 / P95 / P99 download latency at 50 concurrent clients
- Throughput (requests/sec)
- Compare hot object (top 10%) vs cold object (bottom 50%) latency

---

## Test 3: Mixed Read/Write (Realistic Production Load)

### Goal
Simulate realistic production traffic: mostly reads with periodic writes.
70% downloads, 30% uploads — matches typical object store usage.

### Script: `test/load/mixed_load.js`

```javascript
import http from 'k6/http';
import { check } from 'k6';

const objectIDs = JSON.parse(open('./object_ids.json'));

export const options = {
  scenarios: {
    readers: {
      executor: 'constant-vus',
      vus: 35,          // 70% of load
      duration: '3m',
      exec: 'doDownload',
    },
    writers: {
      executor: 'constant-vus',
      vus: 15,          // 30% of load
      duration: '3m',
      exec: 'doUpload',
    },
  },
};

export function doDownload() {
  const id = objectIDs[Math.floor(Math.random() * objectIDs.length)];
  const res = http.get(`http://localhost:8080/api/v1/objects/${id}`);
  check(res, { 'download ok': (r) => r.status === 200 });
}

export function doUpload() {
  const data = Array(50 * 1024).fill('x').join(''); // 50KB
  const form = { file: http.file(data, 'mixed-load.txt', 'text/plain') };
  const res = http.post('http://localhost:8080/api/v1/objects', form);
  check(res, { 'upload ok': (r) => r.status === 201 });
}
```

### Metrics to record
- Upload P99 and download P99 measured simultaneously
- Error rate for both operations
- Does upload contention affect download latency? (bbolt write lock)

---

## Test 4: Degraded Mode — 1 Worker Down Under Load

### Goal
Show system behaviour under load when one worker is offline. All uploads and
downloads should still succeed (RF=2 means one failure is tolerable). Show
latency impact of fallback reads.

### Procedure
1. Start the full 4-worker cluster
2. Run `docker compose stop worker1`
3. Immediately start the mixed load test (Test 3) — do NOT wait for health check
4. Record error rate and latency during the 6-second health lease timeout window
5. Record error rate and latency after worker is marked offline and ring updated

### Metrics to record
- Error rate during health timeout window (6 seconds) — expect some failures
- Error rate after worker marked offline — should drop to 0%
- Download P99 before vs after worker failure (fallback adds latency)
- Upload P99 before vs after (fewer workers = more load on remaining 3)

### Expected behaviour
- During health timeout: some uploads may fail (worker selected but unreachable)
- After 6s: system routes around dead worker, errors stop
- Downloads: 100% success via RF=2 secondary replicas
- Uploads: succeed on remaining 3 workers

---

## Test 5: Race Detector Pass

### Goal
Confirm no data races exist in the codebase under concurrent load.

### Command
```bash
go test -race -p 1 ./... -timeout 300s -count=1
```

### What to do if races are found
1. Note the exact goroutine stacks reported
2. Find the shared variable and the two goroutines accessing it unsynchronized
3. Fix with mutex, atomic, or channel as appropriate
4. Re-run until clean

### Definition of done
Clean output — no `DATA RACE` lines in output. Paste the final clean run output
into `LOAD_TEST_RESULTS.md`.

---

## Reporting Results

Create `LOAD_TEST_RESULTS.md` with this structure:

```markdown
# MiniDFS Load Test Results

## Environment
- MiniDFS version: <git sha>
- Machine: <CPU, RAM>
- Workers: 4 (docker compose)
- Date: <date>

## Test 1: Upload Throughput Ramp
| Concurrency | P50 (ms) | P95 (ms) | P99 (ms) | Throughput (req/s) | Error rate |
|---|---|---|---|---|---|
| 1 | | | | | |
| 10 | | | | | |
| 25 | | | | | |
| 50 | | | | | |

Saturation point: X concurrent clients

## Test 2: Download Throughput (Zipf)
...

## Test 3: Mixed Load
...

## Test 4: Degraded Mode
...

## Test 5: Race Detector
Result: CLEAN / X races found and fixed
```

---

## Commit Structure (suggested ~12-15 commits)

```
feat(load): add k6 upload ramp test script
feat(load): add k6 download zipf test script  
feat(load): add k6 mixed read/write test script
feat(load): add degraded mode test procedure
feat(load): add setup_objects helper script
fix(race): fix data race in <component> found by -race
fix(race): fix data race in <component> found by -race
docs(load): add LOAD_TEST_RESULTS.md with Test 1 results
docs(load): add Test 2 results
docs(load): add Test 3 results
docs(load): add Test 4 degraded mode results
docs(load): add race detector clean run result
```

Each script commit + each results commit = natural split, easy to review.
