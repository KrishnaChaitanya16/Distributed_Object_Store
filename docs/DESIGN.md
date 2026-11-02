# MiniDFS — Design Document

> **Living document.** Update this file whenever a design decision changes, is finalized, or is reversed.
> Do not let this drift from the actual implementation.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Finalized Design Decisions](#2-finalized-design-decisions)
3. [Pending Design Decisions](#3-pending-design-decisions)
4. [Explicitly Cut Features](#4-explicitly-cut-features)
5. [Change Log](#5-change-log)

---

## 1. System Overview

MiniDFS is a distributed, content-aware, fault-tolerant object storage engine written in Go.

| Parameter | Value |
|---|---|
| Language | Go 1.23+ |
| Controller nodes | 1 |
| Storage worker nodes | 4 |
| Chunk size | 1 MB (1,048,576 bytes) |
| Replication factor | 2 (across distinct physical workers) |
| Deployment | Docker Compose only — no Kubernetes |
| Metadata store | bbolt (embedded B+Tree, ACID) |
| Observability | Prometheus + Grafana (pre-provisioned) |

---

## 2. Finalized Design Decisions

---

### 2.1 Consistent Hashing with Virtual Nodes

**Decision:** Workers are mapped onto a 32-bit consistent hash ring using 50 virtual nodes per physical worker (200 total). Chunk placement uses SHA-256 truncated to 32 bits.

**Why:**
Round-robin and random placement require the controller to maintain explicit mapping state to locate chunks later. Consistent hashing makes placement deterministic — the same chunk_id always resolves to the same workers without any extra lookup. Virtual nodes ensure load stays statistically balanced across all 4 workers even though SHA-256 output is not perfectly uniform. When a worker fails, only 1/N of chunks need re-replication, not a full reshuffle.

**Vnode count justification — 50 per worker:**
For a uniform random hash ring, the coefficient of variation (load imbalance across workers) is approximately `CV ≈ 1 / sqrt(V)` where V is vnodes per worker.

| Vnodes per worker | Load imbalance (CV) |
|---|---|
| 10 | ~31% |
| 25 | ~20% |
| **50** | **~14%** ← our choice |
| 100 | ~10% |
| 200 | ~7% |

At V=50 with N=4 workers, the total ring has 200 points and load imbalance is ~14% — well within acceptable bounds. Going beyond 100 vnodes yields diminishing returns (4% improvement per doubling). Chunks are SHA-256 derived and already uniformly distributed, so 50 is more than sufficient. The choice also aligns with Amazon Dynamo's cited range (100–200 total ring points), making it defensible at any level of scrutiny.

**Replica disjointness rule:** Primary = first clockwise vnode from hash(chunk_id). Secondary = next clockwise vnode belonging to a *different* physical worker. This guarantees a single worker failure never takes both replicas of any chunk.

**Adding/removing a worker remaps at most 1/N of chunks** — verified against theoretical bounds (FR-4.3).

---

### 2.2 Content-Addressable Storage (CAS) + Cross-File Deduplication

**Decision:** Every chunk's identifier is `chunk_id = hex(SHA256(chunk_bytes))`. Before streaming a chunk to workers, the controller checks if chunk_id already exists in the manifest index. If it does, the network write is skipped and only the reference counter is incremented.

**Why:**
Two files that share identical 1 MB blocks (e.g., two versions of a dataset differing in one section) store zero redundant bytes. This is the same model used by S3, ZFS, and content delivery networks. The SHA-256 hash is both the dedup key and the integrity checksum — one computation serves two purposes.

**Reference counting:** Each chunk tracks a `ref_count`. On object deletion, ref_count is decremented. Workers only receive a DELETE when ref_count reaches zero, preventing premature deletion of shared chunks.

**Gap fix (Gap 1 — delete vs dedup race):** The ref_count decrement and the decision to dispatch a worker DELETE must happen inside the same bbolt transaction. If they are separate, a concurrent upload that dedup-hits a chunk while a delete is in flight can commit a manifest pointing to a chunk that no longer exists on any worker. To eliminate this race entirely, chunks with ref_count reaching zero are not deleted immediately. Instead they are marked `PENDING_DELETE` in bbolt, and a background GC pass executes the actual worker DELETE only after a grace period (30s), re-checking ref_count inside a read transaction first. If ref_count has risen above zero by then (a new upload deduped against it), the deletion is cancelled.

**Gap fix (Gap 4 — dedup against un-replicated chunk):** ChunkRecord carries a `state` field: `PENDING` (fan-out in progress, not yet fully replicated) or `DURABLE` (both replicas ACKed). The CAS dedup check only matches against `DURABLE` chunks. A `PENDING` chunk is invisible to dedup — a concurrent upload treats it as a cache miss and writes its own copy. This prevents a commit pointing to a chunk that only has R=1 because the original upload failed mid-fan-out.

---

### 2.3 Two-Phase Commit (WAL — STAGED → COMMITTED)

**Decision:** When an upload begins, an object manifest is written to bbolt with `status = STAGED`. The status only transitions to `COMMITTED` after every chunk is durably written and ACKed by both replicas. The `GET /api/v1/objects` endpoint only surfaces COMMITTED objects.

**Why:**
Without this, a controller crash mid-upload leaves a partial object visible to readers — chunks may be present on workers but the manifest is incomplete. The STAGED state makes partial uploads invisible. On controller boot, the crash reconciler scans for STAGED records older than 5 minutes and dispatches orphan chunk deletes to workers, keeping disk space clean.

**Crash reconciler:** Runs at boot AND periodically every 5 minutes while the controller is running. Scans bbolt for STAGED objects older than the timeout window (5 min).

**Gap fix (Gap 2 — aborted STAGED uploads deleting shared chunks):** The reconciler must not dispatch raw worker DELETEs for orphaned chunks. Instead it must decrement each chunk's ref_count inside a bbolt transaction and route chunks that reach zero through the same `PENDING_DELETE` GC path described in section 2.2. This prevents aborting an upload from deleting a chunk that a concurrent upload has already deduped against.

**Gap fix (Gap 3 — chunk intent not recorded before write):** Before sending any chunk to workers, the controller first records the chunk_id in the STAGED manifest inside bbolt (with state `PENDING`). If the controller crashes after a worker write but before bbolt records it, the reconciler has no record of that chunk and it leaks on disk. By writing intent first, the reconciler always knows which chunks to clean up. Additionally, a periodic worker-inventory GC runs every 10 minutes: any chunk present on a worker with no corresponding bbolt record and older than a grace period (15 min) is deleted. The existing `GET /internal/workers/{id}/chunks` endpoint makes this cheap.

**Gap fix (Gap 6 — reconciler runs only at boot):** In-flight uploads that fail while the controller stays running also leave STAGED records. The reconciler is now a periodic ticker (every 5 min), not a one-shot boot task.

---

### 2.4 bbolt as Metadata Store

**Decision:** Use `go.etcd.io/bbolt` as the embedded transactional key-value store for all metadata. No external database.

**Why:**
bbolt is a pure-Go embedded B+Tree with full ACID semantics. It runs in-process, requires no separate container, and persists to a single file on the controller's volume. For 4 workers and academic-scale object counts (hundreds to thousands of objects), bbolt is more than sufficient. Adding PostgreSQL or Redis would introduce operational overhead (connection pooling, separate service health checks) with no benefit at this scale. bbolt's single-writer model also gives us linearizable metadata operations for free.

---

### 2.5 Bloom Filter for CAS Lookup

**Decision:** An in-memory bloom filter sits in front of the bbolt CAS check. On controller startup, the bloom filter is rebuilt by scanning all chunk_ids in bbolt.

**Why:**
On every chunk ingested, the controller must check if the chunk already exists. Without the filter, this is a bbolt disk read on the hot upload path. The bloom filter gives an O(1) in-memory answer. A false negative is impossible — if the filter says "absent," the chunk is definitely new, and we skip bbolt entirely. A false positive just triggers one unnecessary bbolt read (which confirms the chunk does not exist). This trades a small amount of memory for significant reduction in disk I/O on the upload path.

**Library:** `github.com/bits-and-blooms/bloom/v3`
**False positive rate target:** 1% (configurable via env var)

---

### 2.6 Concurrent Fan-Out with 2-of-2 ACK

**Decision:** For each non-deduplicated chunk, the controller launches two goroutines concurrently — one PUT to the primary worker, one PUT to the secondary. The manifest only advances to the next chunk after both ACK (201 Created). If either times out or errors, the upload is marked FAILED and orphan cleanup is dispatched.

**Why:**
Sequential replication (primary then secondary) doubles upload latency unnecessarily. Concurrent fan-out halves it. 2-of-2 ACK (rather than 1-of-2 quorum) is correct here because N=2 makes W+R > N only achievable with W=2, R=1 or W=1, R=2. We choose W=2, R=1 — strong write durability, fast reads. A quorum of 1-of-2 on writes would mean data can be lost if the written replica fails before the second copy is made.

---

### 2.7 Zero-Copy Egress via sendfile(2)

**Decision:** Worker GET handlers serve chunks by using `io.Copy` from `*os.File` to `*net.TCPConn`. On Linux, this triggers the `sendfile(2)` system call, which transfers bytes directly from the OS Page Cache to the NIC via DMA without copying into user-space buffers.

**Why:**
The conventional path (`read()` into a Go `[]byte`, then `write()` to the socket) copies data twice through user space and burns CPU. `sendfile(2)` does it in one kernel operation. On a gigabit network under line-rate egress, this keeps worker CPU under 5% (NFR-2). The key implementation requirement: the `net.Conn` must not be wrapped in a `bufio.Writer` — that would break the zero-copy path.

**Gap fix (Gap 5 — worker write not atomic or fsynced):** Writing `.bin` and `.crc` as two separate files is not atomic — a crash between the two writes leaves a chunk with a missing or stale sidecar. The correct write sequence is:
1. Write chunk bytes to a temp file `<chunk_id>.tmp` in the same directory.
2. `fsync` the temp file (flush to disk).
3. `rename(<chunk_id>.tmp, <chunk_id>.bin)` — atomic on POSIX filesystems.
4. `fsync` the directory entry.
5. Write CRC32C as a footer appended to `.bin` (same file, one rename) — eliminates the two-file atomicity problem entirely.

The same temp-and-rename approach applies to read-repair overwrites, ensuring a crash during repair never replaces a good chunk with a partial write. ACKing the PUT to the controller only happens after the fsync+rename sequence completes — this is what "durably written" means in FR-6.2.

---

### 2.8 Hedged Reads at 25ms

**Decision:** When the controller issues `GET /internal/chunks/{chunk_id}` to the primary worker, a 25ms timer starts. If no first byte arrives within 25ms, the controller simultaneously issues the same request to the secondary replica. Whichever responds first provides the data; the slower is cancelled via `context.WithCancel()`.

**Why:**
Tail latency in distributed systems is dominated by rare slow responses (GC pause, disk seek, network jitter) rather than average latency. Hedging bounds worst-case download latency without requiring the primary to completely fail. 25ms is the default threshold — configurable via env var. The losing goroutine's response body must be fully drained and closed to avoid goroutine leaks.

---

### 2.9 Transparent Read-Repair

**Decision:** When a chunk is retrieved and its SHA-256 hash does not match the stored manifest hash, the controller immediately falls back to the secondary replica to serve the client request (no error exposed). It then asynchronously dispatches a repair task to overwrite the corrupted chunk on the failing worker with the clean bytes from the secondary.

**Why:**
The client request must not fail due to a storage-layer corruption that has a valid replica available. Serving from the secondary is transparent — the client receives correct data with no retry and no error. The async repair restores R=2 in the background. This is the standard read-repair pattern from Cassandra and Dynamo.

---

### 2.10 CRC32C Scrubber + SHA-256 Inline Verification (Two Paths)

**Decision:** Two independent corruption detection paths:
- **Path 1 (proactive):** Each worker runs a low-priority background goroutine sweeping all chunks every 30 seconds, computing CRC32C and comparing against a `.crc` sidecar file written at chunk store time.
- **Path 2 (reactive):** The controller verifies SHA-256 of every retrieved chunk inline on every GET, before streaming to the client.

**Why:**
CRC32C is hardware-accelerated (Intel SSE4.2 / ARM crypto extensions) and cheap to compute repeatedly. It catches silent disk bitrot before a client ever requests the corrupted data. SHA-256 on the read path catches anything that slips through (a corruption that occurred between scrub cycles). Two independent paths with different algorithms give the zero-undetected-bitrot guarantee (NFR-4). The `.crc` sidecar is written atomically with the chunk — if the sidecar is missing, the scrubber treats the chunk as suspect.

---

### 2.11 P2P Self-Healing (Controller Out of Data Path)

**Decision:** When a chunk needs re-replication, the controller sends `POST /internal/workers/replicate-peer` to the surviving replica with `{chunk_id, target_worker_address}`. The surviving worker streams the chunk directly to the target worker via `PUT /internal/chunks/{chunk_id}`. The controller sends only the instruction, never the bytes.

**Why:**
If the controller proxied repair data, its network bandwidth would become the bottleneck during mass recovery (e.g., worker failure with thousands of chunks to repair). P2P replication distributes the bandwidth load across surviving workers. This is the same pattern used by HDFS DataNode-to-DataNode replication and Ceph OSD recovery.

---

### 2.12 Heartbeat Leases + Failure Detection

**Decision:** Workers emit `POST /internal/workers/heartbeat` every 2 seconds. The controller maintains a lease table with the last-seen timestamp per worker. A worker missing 3 consecutive heartbeats (6 seconds) is marked OFFLINE. Its virtual nodes are removed from the hash ring. New writes are routed to remaining healthy workers.

**Why:**
6 seconds is long enough to tolerate transient network hiccups (one missed heartbeat = 2s), short enough to start healing before significant new data accumulates on only one replica. The 3-miss threshold prevents false positives from a single dropped heartbeat packet.

**Gap fix (Gap 7 — lease table lost on controller restart):** The in-memory lease table is wiped on controller restart. The controller must not immediately declare all workers OFFLINE and trigger a mass healing storm on boot. On startup, all previously registered workers (read from the `workers` bbolt bucket) are placed in an `UNKNOWN` state. The controller waits for a heartbeat or re-registration from each before marking it ONLINE or OFFLINE. No healing is dispatched for an `UNKNOWN` worker — only for one confirmed OFFLINE after the grace period (one full heartbeat cycle = 6s). Workers must implement retry logic on their registration and heartbeat loops so they reconnect automatically if the controller restarts.

**Gap fix (Gap 9 — healing storms on flapping workers):** A worker that bounces rapidly triggers repeated ring changes and re-replication waves. Two mitigations:
1. **Healing delay:** The self-healing loop waits 30 seconds after a worker is declared OFFLINE before dispatching any repair tasks. If the worker re-registers within that window, healing is cancelled.
2. **bbolt as source of truth for replica locations:** After any ring change, the computed ring placement no longer matches where chunks actually live (chunks written before the change are still on the old workers). The controller always uses the `replicas` list stored in the `chunks` bbolt bucket to locate chunk copies — never recomputes placement from the current ring for existing chunks. The ring is used only for placing *new* chunks.

---

### 2.13 Worker Restart Reconciliation

**Decision:** When a worker restarts and calls `POST /internal/workers/register`, the controller:
1. Re-adds its virtual nodes to the hash ring (for new writes).
2. Calls `GET /internal/workers/{id}/chunks` to get the worker's current chunk inventory.
3. Diffs the inventory against bbolt to find chunks still needing repair.
4. Only dispatches healing for chunks that are still at R < 2 after accounting for what the worker already has.

**Why:**
A naive approach would re-replicate all chunks that were degraded when the worker was down, even if the worker's persistent volume still has them. The reconcile step avoids redundant replication work and correctly handles the case where the volume survived (docker stop/start without data loss).

---

### 2.14 Circuit Breaker Per Worker

**Decision:** Each worker client in the controller has an independent circuit breaker with three states:
- **CLOSED:** Normal operation.
- **OPEN:** After 3 consecutive failures, fail fast for 10 seconds — no requests sent to worker.
- **HALF-OPEN:** After 10 seconds, send one probe request. Success → CLOSED. Failure → OPEN again.

**Why:**
Without a circuit breaker, a flapping worker (returning errors intermittently) causes every chunk request to wait for a full HTTP timeout before falling back to the secondary. This serializes failure handling and makes every download slow during a partial outage. The circuit breaker fails fast, letting the fallback path kick in immediately. Thresholds (3 failures, 10s cooldown) are configurable via env var.

---

### 2.15 Exponential Backoff + Jitter on Retries

**Decision:** All retry loops (chunk writes to workers, peer replication commands) use: `sleep = min(base * 2^attempt, maxDelay) + rand(0, jitter)` where `base = 100ms`, `maxDelay = 5s`, `jitter = 100ms`.

**Why:**
Pure exponential backoff without jitter causes a retry storm: if N clients all fail at the same moment (e.g., a worker briefly drops), they all retry at the same intervals, causing synchronized bursts. Adding random jitter spreads retries across time, reducing the burst amplitude and giving the recovering worker room to recover.

---

### 2.16 Idempotency Keys on Upload

**Decision:** Clients may include an `Idempotency-Key` header on `POST /api/v1/objects`. The controller stores the key → response mapping in bbolt with a 24-hour TTL. A second request with the same key returns the cached 201 response without re-uploading.

**Why:**
If the network drops after the server commits an upload but before the client receives the 201 response, a naive retry would create a duplicate object. Idempotency keys make retries safe. The key is client-generated (UUID recommended). This is the same pattern used by Stripe and most payment APIs.

**Gap fix (Gap 8 — idempotency record and commit not one transaction):** The `STAGED → COMMITTED` flip and the idempotency key write must be a single bbolt transaction. If they are two separate transactions, a crash between them leaves the object committed but the idempotency key unwritten — a client retry then creates a duplicate. Both writes go inside one `db.Update()` call. Concurrent requests with the same idempotency key are handled by writing a `PROCESSING` sentinel into the idempotency bucket at the start of the upload (inside the same transaction that writes the STAGED manifest). A second concurrent request seeing `PROCESSING` returns 409 Conflict immediately rather than starting a duplicate upload.

---

### 2.17 Object Versioning with ETag + Conditional PATCH

**Decision:** Every object metadata record carries a `version` integer, incremented on every `PATCH`. `GET /api/v1/objects/{id}/metadata` returns `ETag: "<version>"`. `PATCH` accepts an optional `If-Match: "<version>"` header — if provided and the version doesn't match, the controller returns 409 Conflict.

**Why:**
Without optimistic locking, two concurrent PATCH requests on the same object can produce a lost update — one client's write silently overwrites the other's. The ETag/If-Match pattern (from HTTP RFC 7232) gives clients a way to detect and handle conflicts. The version counter is stored alongside the metadata in bbolt and incremented atomically within the same bbolt transaction as the metadata update.

---

### 2.18 Prometheus Metrics + Pre-Provisioned Grafana

**Decision:** Both Controller and Workers expose `/metrics` in Prometheus format. `docker-compose up` brings up Prometheus (port 9090) and Grafana (port 3000) with the dashboard pre-provisioned via volume-mounted JSON — no manual setup required.

**Why:**
A live Grafana dashboard during the demo lets the evaluator watch the system react to events in real time (worker failure → degraded chunk spike → healing → recovery). This is more compelling than curl output or log tailing. Pre-provisioning the dashboard as a JSON file in the repo means zero manual steps — the demo is reproducible.

**Key metrics (to be finalized in section 3.6):**

| Metric | Type | Description |
|---|---|---|
| `minidfs_objects_total` | Counter | Total objects in COMMITTED state |
| `minidfs_chunks_total` | Gauge | Total unique chunks stored |
| `minidfs_dedup_bytes_saved_total` | Counter | Bytes saved via CAS dedup |
| `minidfs_degraded_chunks` | Gauge | Chunks with replica_count < 2 |
| `minidfs_workers_online` | Gauge | Number of ONLINE workers |
| `minidfs_healing_repairs_total` | Counter | Peer replication repairs triggered |
| `minidfs_bitrot_detected_total` | Counter | CRC32C mismatches found |
| `minidfs_upload_duration_seconds` | Histogram | End-to-end upload latency |
| `minidfs_download_duration_seconds` | Histogram | End-to-end download latency |
| `minidfs_worker_chunks_stored` | Gauge | Chunks stored on a given worker (label: worker_id) |
| `minidfs_worker_disk_bytes_used` | Gauge | Disk bytes used on a given worker (label: worker_id) |

---

## 3. Pending Design Decisions

> These must be finalized before the relevant code is written. Update this section as each is decided.

---

### 3.1 bbolt Bucket Schema

**Status:** PENDING

**Decision needed:** Exact bucket names, key formats, and value serialization (JSON vs protobuf vs gob).

**Candidates under consideration:**

```
Bucket: "objects"
  Key:   object_id (UUID string)
  Value: ObjectManifest { id, name, size, content_type, status,
                          created_at, modified_at, checksum,
                          chunk_count, version, custom_metadata,
                          idempotency_key }

Bucket: "chunks"
  Key:   chunk_id (SHA256 hex string)
  Value: ChunkRecord { chunk_id, size, ref_count,
                       replicas: [{worker_id, worker_addr}] }

Bucket: "workers"
  Key:   worker_id (string)
  Value: WorkerRecord { worker_id, address, port, capacity,
                        status, last_seen }

Bucket: "idempotency"
  Key:   idempotency_key (string)
  Value: CachedResponse { status_code, body, expires_at }
```

**Open questions:**
- JSON (human-readable, slower) vs gob (Go-native, faster) for value encoding?
- Separate bucket for chunk-to-object reverse index, or scan objects bucket?

---

### 3.2 Disk Layout on Workers

**Status:** PENDING

**Decision needed:** Directory structure for chunk files and CRC sidecars on worker persistent volumes.

**Candidates:**

Option A — Flat:
```
/data/chunks/<chunk_id>.bin
/data/chunks/<chunk_id>.crc
```
Simple. Risk: inode exhaustion at very large chunk counts (millions). Fine for academic scale.

Option B — Sharded by prefix:
```
/data/chunks/<first2chars>/<chunk_id>.bin
/data/chunks/<first2chars>/<chunk_id>.crc
```
Scales to millions of chunks. Slight added complexity in path construction.

**Leaning toward:** Option A for now (academic scale). Document threshold at which Option B becomes necessary.

---

### 3.3 Concurrency Model

**Status:** PENDING

**Decision needed:** Goroutine ownership, shared state protection, channel vs mutex strategy.

**Draft decisions:**

| Component | Concurrency approach |
|---|---|
| Hash ring | `sync.RWMutex` — many concurrent reads, rare writes (node join/leave) |
| Heartbeat lease table | `sync.Map` or `sync.RWMutex`-protected map |
| Bloom filter | `sync.RWMutex` — reads during upload, writes on new chunk commit |
| Self-healing loop | Single goroutine, ticker-based, reads ring and bbolt |
| Circuit breaker per worker | Per-worker mutex, simple state field |
| Scrubber per worker | Single goroutine, ticker-based, low priority via `runtime.Gosched()` |
| Chunk fan-out per upload | `errgroup.Group` — two goroutines per chunk, wait for both |

**Open questions:**
- Use `errgroup` from `golang.org/x/sync` or raw goroutines + WaitGroup for fan-out?
- Should the self-healing loop hold a scan lock or process one chunk at a time?

---

### 3.4 Worker Registration Protocol

**Status:** PENDING

**Decision needed:** Exact request/response JSON and controller behavior on re-registration.

**Draft:**

```json
POST /internal/workers/register
{
  "worker_id": "worker1",
  "address":   "worker1",
  "port":      8001,
  "capacity":  10737418240
}

Response 200:
{
  "status": "registered",
  "vnode_count": 50
}
```

**Re-registration behavior (worker restart):**
1. Reset last_seen timestamp in lease table.
2. Set worker status back to ONLINE.
3. Re-insert virtual nodes into hash ring.
4. Trigger async inventory reconcile.

**Open question:** Should the controller reject registration from an unknown worker_id, or auto-provision any worker that connects?

---

### 3.5 Configuration Strategy

**Status:** PENDING

**Decision needed:** Which values are env vars, which are hardcoded defaults.

**Draft env var list:**

| Env Var | Default | Component |
|---|---|---|
| `CONTROLLER_PORT` | `8080` | Controller |
| `WORKER_PORT` | `8001`–`8004` | Workers |
| `WORKER_ID` | required | Workers |
| `CONTROLLER_ADDRESS` | `controller:8080` | Workers |
| `VNODE_COUNT` | `50` | Controller |
| `HEARTBEAT_INTERVAL_S` | `2` | Workers |
| `HEARTBEAT_TIMEOUT_S` | `6` | Controller |
| `HEDGE_THRESHOLD_MS` | `25` | Controller |
| `CIRCUIT_BREAKER_FAILURES` | `3` | Controller |
| `CIRCUIT_BREAKER_COOLDOWN_S` | `10` | Controller |
| `SCRUB_INTERVAL_S` | `30` | Workers |
| `BLOOM_FP_RATE` | `0.01` | Controller |
| `STAGED_TIMEOUT_MIN` | `5` | Controller |
| `IDEMPOTENCY_TTL_H` | `24` | Controller |
| `DATA_DIR` | `/data` | Workers |
| `DB_PATH` | `/data/meta.db` | Controller |

---

### 3.6 Prometheus Metrics Schema (Finalization)

**Status:** PARTIALLY DEFINED (see section 2.18 — labels and histogram buckets TBD)

**Open questions:**
- Histogram bucket boundaries for upload/download duration?
- Label cardinality — add `object_id` label or keep only `worker_id`?

---

### 3.7 Graceful Shutdown Sequence

**Status:** PENDING

**Decision needed:** What happens on SIGTERM for controller and workers.

**Draft controller shutdown:**
1. Stop accepting new upload requests (503 on POST).
2. Allow in-flight uploads to complete (or timeout after 30s → mark FAILED).
3. Flush bbolt write buffer.
4. Close HTTP server.

**Draft worker shutdown:**
1. Stop accepting new PUT chunk requests.
2. Finish serving any in-progress GET (streaming to controller).
3. Close HTTP server.

---

### 3.8 Error Response Schema

**Status:** PENDING

**Decision needed:** Consistent JSON error body format for all non-2xx responses.

**Draft:**
```json
{
  "error": "object not found",
  "code":  "OBJECT_NOT_FOUND",
  "request_id": "uuid"
}
```

**Code values to define:** `OBJECT_NOT_FOUND`, `CHUNK_UNAVAILABLE`, `WORKER_UNAVAILABLE`, `INVALID_REQUEST`, `VERSION_CONFLICT`, `INTERNAL_ERROR`, `UPLOAD_FAILED`.

---

### 3.9 Grafana Dashboard Layout

**Status:** PENDING

**Draft panel layout:**

```
Row 1 — Cluster Health
  [Workers Online]  [Degraded Chunks]  [Total Objects]  [Total Unique Chunks]

Row 2 — Throughput
  [Upload Rate (obj/s)]  [Download Rate (obj/s)]  [Upload Latency p50/p95/p99]

Row 3 — Data Integrity
  [Bitrot Detected Total]  [Read Repairs Total]  [Healing Repairs Total]

Row 4 — Deduplication
  [Dedup Bytes Saved]  [Dedup Ratio]  [Bloom Filter Hit Rate]

Row 5 — Per-Worker
  [Chunks per Worker (bar)]  [Disk Used per Worker (bar)]
```

---

### 3.10 Structured Log Format

**Status:** PENDING

**Draft JSON log line:**
```json
{
  "ts":         "2026-11-02T10:00:00.123Z",
  "level":      "info",
  "component":  "controller",
  "event":      "chunk_replicated",
  "object_id":  "c7a9b21e-...",
  "chunk_id":   "e3b0c442...",
  "worker_id":  "worker2",
  "duration_ms": 12,
  "error":      null
}
```

---

## 4. Explicitly Cut Features

| Feature | Reason |
|---|---|
| Resumable downloads (HTTP Range) | Not in project requirements. Deferred post-submission. |
| Worker draining (graceful evacuation) | Low ROI for Nov 20 deadline. |
| Custom web UI dashboard | Replaced by Grafana — more credible, less build time. |
| Kubernetes orchestration | Explicitly forbidden by project requirements. |
| Hedged reads on upload side | Upload is write-path; hedging applies only to reads. |
| Gossip protocol for worker discovery | Overkill for 4 workers. Heartbeat lease table is sufficient. |
| Raft / Paxos consensus | Single controller — no consensus needed. |
| Hinted handoff | R=2 self-healing covers the same failure scenario more simply. |

---

## 5. Change Log

| Date | Change | Reason |
|---|---|---|
| 2026-11-02 | Document created | Initial architecture design session |
| 2026-11-02 | Added CV formula and 14% imbalance justification for 50 vnodes | Confirmed choice is deliberate, not arbitrary |
| 2026-11-02 | Incorporated 9 correctness gaps into relevant sections | Gap analysis identified data-loss races, atomicity issues, and storm conditions |

