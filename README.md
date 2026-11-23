# MiniDFS — Fault-Tolerant Distributed Object Store

A distributed object storage system built from first principles in Go.
MiniDFS stores arbitrary binary objects by splitting them into 1 MiB chunks,
replicating each chunk across two independent workers, and tracking all
placement decisions in an embedded bbolt B+Tree database — no external
database server required.

**Authors:** K. Krishna Chaitanya (IMT2023039) · Pasupuleti T A N S V Vignesh (IMT2023003)

---

## Table of Contents

1. [What it does](#what-it-does)
2. [Architecture](#architecture)
3. [Design choices](#design-choices)
4. [Prerequisites](#prerequisites)
5. [Quick start (Docker Compose)](#quick-start-docker-compose)
6. [Running tests](#running-tests)
7. [Running benchmarks](#running-benchmarks)
8. [API reference](#api-reference)
9. [Configuration](#configuration)
10. [Project structure](#project-structure)

---

## What it does

| Feature | Detail |
|---|---|
| **Replication** | RF=2 — every chunk lives on two distinct workers; one worker can die with zero data loss |
| **Content-addressed storage** | Chunk ID = `SHA-256(bytes)` — identical content is stored exactly once |
| **Bloom filter dedup fast-path** | 46 ns lookup; 1 M chunks at 1% FP rate; rebuilt from bbolt on restart |
| **Parallel fan-out** | Both replicas written concurrently with goroutines + `sync.WaitGroup` |
| **Hedged reads** | If the primary misses its 25 ms window the secondary is raced; P99 stays ≈28 ms even with a 100 ms slow worker |
| **Inline SHA-256 verification** | Every chunk is verified before any byte is forwarded to the client |
| **CRC32C scrubber** | Workers scan every chunk file every 30 s; damaged copies are quarantined and the reconciler repairs them |
| **Self-healing reconciler** | Detects missing replicas, fans out up to 4 peer-to-peer replication RPCs concurrently, restores RF=2 |
| **Consistent hash ring** | 50 virtual nodes per worker (200 total); adding a worker remaps ≈1/N chunks (vs 97% for modulo) |
| **Circuit breaker** | Per-worker CLOSED → OPEN → HALF_OPEN state machine prevents goroutine pile-ups on a bad worker |
| **Two-phase commit** | Objects are STAGED before any chunk is sent; COMMITTED only after both replicas are durable |
| **Idempotent uploads** | Client-supplied request token prevents double-writes on retry |
| **Prometheus + Grafana** | Pre-provisioned dashboard; no manual setup after `docker compose up` |
| **Live Web UI** | Visualizes upload flow, hash ring, Bloom filter, healing, and cluster state via SSE |

---

## Architecture

The system is deployed as a Docker Compose project with seven containers: one controller (REST API on port 8080, Web UI on port 8081), four workers (ports 8001–8004), one Prometheus instance (port 9090), and one Grafana instance (port 3000). All containers share a private bridge network; only the controller's ports are published to the host. Workers never communicate with each other except for peer-replication RPCs commanded by the controller.

Every upload goes through a two-phase commit:

1. **STAGED** — object record written to bbolt; no chunk sent yet.
2. Chunks are split, SHA-256-hashed, Bloom-checked, and pushed to two workers in parallel.
3. **COMMITTED** — atomic state transition after every chunk has two durable replicas.

The GC Reconciler cleans up STAGED objects that stall past a configurable timeout.

---

## Design choices

### bbolt over PostgreSQL
bbolt is embedded — no extra container, no network hop, microsecond manifest
lookups via `mmap` directly against the OS page cache. The tradeoff is a
single write-lock at a time, which is fine for a 4-worker cluster but would
be a bottleneck at thousands of concurrent uploads.

### Content-addressed chunks (SHA-256)
Using `SHA-256(data)` as the chunk ID gives deduplication for free: identical
content always maps to the same record. A UUID or sequential scheme is simpler
but cannot deduplicate across objects. Measured savings: 2.95× storage
reduction for three Spring Boot services sharing a JRE base image.

### Parallel fan-out over chain replication
The controller pushes to both replicas concurrently, halving per-chunk latency.
Chain replication (worker 1 → worker 2) would halve controller bandwidth but
double the latency path. For a co-located controller the latency saving wins.

### Fixed 25 ms hedge threshold
Chosen to sit well above normal loopback latency (<1 ms) while bounding P99
below any realistic slow-worker delay. A rolling-window adaptive threshold
would be better for production; the fixed value is sufficient here and avoids
feedback-loop complexity.

### Consistent hash ring (50 vnodes / worker)
50 vnodes per worker produces near-ideal load balance. Adding a 5th worker
remaps 18.8% of chunks vs the theoretical 20% minimum and vs 79.9% for
modulo placement. The secondary replica is always picked from a **different**
physical worker, guaranteeing RF=2 on distinct machines.

---

## Prerequisites

| Tool | Minimum version |
|---|---|
| Go | 1.22 |
| Docker | 24 |
| Docker Compose | v2 (plugin, not standalone) |
| `mmdc` (optional) | For regenerating Mermaid diagrams |

---

## Quick start (Docker Compose)

### 1. Clone the repository

```bash
git clone https://github.com/KrishnaChaitanya16/Distributed_Object_Store.git
cd Distributed_Object_Store
```

### 2. Build and start the full stack

```bash
docker compose -f deploy/docker-compose.yml up --build
```

This starts seven containers:

| Container | Port | Purpose |
|---|---|---|
| `controller` | 8080 | REST API |
| `controller` | 8081 | Web UI |
| `worker1` | 8001 | Chunk storage |
| `worker2` | 8002 | Chunk storage |
| `worker3` | 8003 | Chunk storage |
| `worker4` | 8004 | Chunk storage |
| `prometheus` | 9090 | Metrics scraper |
| `grafana` | 3000 | Dashboard (admin / admin) |

### 3. Upload a file

```bash
curl -X POST http://localhost:8080/api/v1/objects \
  -F "file=@/path/to/your/file.txt" \
  -F "name=my-file"
```

### 4. Download it back

```bash
# replace <object_id> with the id returned by the upload
curl http://localhost:8080/api/v1/objects/<object_id> -o downloaded.txt
```

### 5. Open the Web UI

Navigate to **http://localhost:8081** to see the live cluster visualization —
upload flow animation, hash ring, Bloom filter bit-cell view, worker health,
and GC healing history.

### 6. Open Grafana

Navigate to **http://localhost:3000** (admin / admin).
The **MiniDFS Overview** dashboard is pre-provisioned — no setup required.

### 7. Tear down

```bash
docker compose -f deploy/docker-compose.yml down -v
```

The `-v` flag removes the data volumes. Omit it to persist chunk data across restarts.

---

## Running tests

### Unit tests

```bash
go test ./...
```

### Unit tests with race detector

```bash
go test -race ./...
```

All 19 packages pass the race detector clean.

### End-to-end tests

```bash
go test ./test/e2e/... -v -timeout 120s
```

### Chaos and fault-injection tests

```bash
go test ./test/e2e/ -run TestChunk -v          # chunk boundary tests
go test ./test/e2e/ -run TestFuzz  -v          # fuzz / bad-input tests
go test ./test/e2e/ -run TestHealing -v        # worker flapping / mid-heal
go test ./test/e2e/ -run TestCorruption -v     # bit-rot, truncated chunks
go test ./test/e2e/ -run TestCircuitBreaker -v # per-worker circuit breaker
go test ./test/e2e/ -run TestInvariant -v      # RF=2 invariant checker
```

### Smoke test (Docker, full stack)

```bash
bash deploy/smoke_test.sh
```

Builds images, starts the stack on shifted ports, runs container-level checks,
and tears everything down.

---

## Running benchmarks

Benchmarks spin up a real in-process cluster (bbolt, 4 workers with fsync,
loopback HTTP). Results are reported as P50/P95/P99/max with a bootstrap 95%
confidence interval over 7 repetitions (1 warm-up discarded).

### Dedup storage savings

```bash
# Requires Docker images to be built first:
bash test/benchmarks/build_app_images.sh
bash test/benchmarks/build_spring_images.sh
bash test/benchmarks/fetch_layers.sh

go test ./test/benchmarks/ -run BenchmarkDedup -bench BenchmarkDedup \
  -benchtime 7x -v
```

### Hedged read latency

```bash
go test ./test/benchmarks/ -run BenchmarkHedge -bench BenchmarkHedge \
  -benchtime 7x -v
```

### Reconciler healing throughput

```bash
go test ./test/benchmarks/ -run BenchmarkReconcile \
  -bench BenchmarkReconcile -benchtime 7x -v
```

### Scrubber detection

```bash
go test ./test/benchmarks/ -run BenchmarkScrubber \
  -bench BenchmarkScrubber -benchtime 7x -v
```

### All benchmarks

```bash
go test ./test/benchmarks/ -bench . -benchtime 7x -v
```

See [`BENCHMARKS.md`](BENCHMARKS.md) for the full results table.

---

## API reference

| Method | Path | Success | Description |
|---|---|---|---|
| `POST` | `/api/v1/objects` | 201 | Upload an object (multipart) |
| `GET` | `/api/v1/objects` | 200 | List all objects |
| `GET` | `/api/v1/objects/{id}` | 200 | Download an object |
| `GET` | `/api/v1/objects/{id}/metadata` | 200 | Get object metadata only |
| `PATCH` | `/api/v1/objects/{id}/metadata` | 200 | Update object metadata |
| `DELETE` | `/api/v1/objects/{id}` | 204 | Delete an object |

**Range requests** are supported on downloads (`Range: bytes=a-b`, RFC 9110).
Only the overlapping chunks are fetched; each is SHA-256-verified before slicing.

**Error codes:** 404 missing object, 409 idempotency key conflict,
503 insufficient workers, 416 range not satisfiable.

**Idempotent uploads:** pass `X-Idempotency-Key: <token>` to get the same
response on retry without double-writing.

---

## Configuration

All settings are environment variables. The Docker Compose file sets sensible
defaults; override any of them with an `.env` file or by exporting before
running compose.

### Controller

| Variable | Default | Description |
|---|---|---|
| `CONTROLLER_PORT` | `8080` | REST API listen port |
| `WEBUI_PORT` | `8081` | Web UI listen port |
| `DB_PATH` | `/data/meta.db` | bbolt database file |
| `VNODE_COUNT` | `50` | Virtual nodes per worker on the hash ring |
| `HEARTBEAT_TIMEOUT_S` | `6` | Seconds before a silent worker is declared offline |
| `HEDGE_THRESHOLD_MS` | `25` | Milliseconds before a secondary read is fired |
| `GC_INTERVAL_S` | `30` | Reconciler run interval |
| `STAGED_TIMEOUT_MIN` | `5` | Minutes before a stalled upload is expired |
| `BLOOM_FP_RATE` | `0.01` | Bloom filter false-positive rate (1%) |
| `CIRCUIT_BREAKER_FAILURES` | `3` | Consecutive failures before OPEN |
| `CIRCUIT_BREAKER_COOLDOWN_S` | `10` | Seconds in OPEN before HALF_OPEN probe |

### Worker

| Variable | Default | Description |
|---|---|---|
| `WORKER_ID` | — | Unique worker identifier (required) |
| `WORKER_PORT` | `8001` | Listen port inside the container |
| `DATA_DIR` | `/data` | Directory for chunk files |
| `CONTROLLER_ADDRESS` | — | `host:port` of the controller (required) |
| `HEARTBEAT_INTERVAL_S` | `2` | Seconds between heartbeats |
| `SCRUB_INTERVAL_S` | `30` | Seconds between CRC32C scrub passes |

---

## Project structure

```
.
├── cmd/
│   ├── controller/        # Controller binary + Dockerfile
│   └── worker/            # Worker binary + Dockerfile
├── deploy/
│   ├── docker-compose.yml # Full stack (controller, 4 workers, Prometheus, Grafana)
│   ├── prometheus/        # Scrape config
│   ├── grafana/           # Pre-provisioned dashboard + datasource
│   └── smoke_test.sh      # End-to-end container smoke test
├── docs/
│   ├── report.tex         # Project report (LaTeX)
│   └── diagrams/          # Mermaid source for all architecture diagrams
├── internal/
│   ├── controller/
│   │   ├── api/           # HTTP handlers, upload/download pipelines
│   │   ├── dedup/         # Bloom filter
│   │   ├── gc/            # GC Reconciler
│   │   ├── health/        # Heartbeat monitor
│   │   ├── metadata/      # bbolt store (5 buckets)
│   │   ├── placement/     # Consistent hash ring
│   │   ├── rebalancer/    # Post-join rebalance
│   │   ├── web/           # Web UI + SSE event stream
│   │   └── workerclient/  # HTTP client for worker RPCs
│   └── worker/
│       ├── api/           # Chunk PUT/GET/DELETE + peer-replicate RPC
│       ├── registration/  # Heartbeat sender
│       ├── scrubber/      # CRC32C background scrubber
│       └── storage/       # Chunk file I/O with CRC footer
├── pkg/                   # Shared models and circuit breaker
├── test/
│   ├── e2e/               # End-to-end and chaos/fault-injection tests
│   └── benchmarks/        # All benchmark suites + Docker layer scripts
└── BENCHMARKS.md          # Full benchmark results
```
