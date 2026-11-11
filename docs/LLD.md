# MiniDFS — Low-Level Design (LLD)

> Companion to `DESIGN.md` (HLD). The HLD says *what* and *why*. This document says *how* — architecture style, module boundaries, interfaces, design patterns, and data structures.
> Update this file whenever a module boundary, interface, or pattern changes.

---

## Table of Contents

1. [Architecture Style](#1-architecture-style)
2. [Repository Layout](#2-repository-layout)
3. [Controller — Module Breakdown](#3-controller--module-breakdown)
4. [Worker — Module Breakdown](#4-worker--module-breakdown)
5. [Key Interfaces](#5-key-interfaces)
6. [Design Patterns](#6-design-patterns)
7. [Core Data Structures](#7-core-data-structures)
8. [Inter-Service Communication](#8-inter-service-communication)
9. [Dependency Graph](#9-dependency-graph)
10. [Change Log](#10-change-log)

---

## 1. Architecture Style

### Chosen Style: Layered Architecture (3-Tier)

Each binary (Controller, Worker) is organized in three strict layers. Dependencies only flow downward — upper layers call lower layers, never the reverse.

```
┌─────────────────────────────────────────┐
│           Transport Layer               │  HTTP handlers only — parse, call, serialize
├─────────────────────────────────────────┤
│           Service Layer                 │  All business logic — orchestrates everything
├─────────────────────────────────────────┤
│        Infrastructure Layer             │  bbolt, worker HTTP clients, disk I/O
└─────────────────────────────────────────┘
```

**Rules:**
- Transport layer never contains business logic. Handlers parse the request, call one service method, serialize the result.
- Service layer never imports `bbolt`, `net/http` (for outbound), or `os`. It talks to infrastructure only through Go interfaces.
- Infrastructure layer never makes business decisions. It executes I/O and returns data.
- Background goroutines (scrubber, healer, GC) live in the Service Layer — they call the same interfaces as HTTP handlers.

### What We Are NOT Using

| Style | Reason skipped |
|---|---|
| Microservices per feature | Controller is one binary — splitting adds network overhead with no benefit |
| Event-driven / message bus | No Kafka/NATS; Go channels between goroutines serve the same role at this scale |
| CQRS | Read/write volumes are similar; the split adds complexity without payoff |
| Hexagonal / Ports & Adapters | Layered achieves the same goal — interface-driven boundaries — without the jargon overhead |

---

## 2. Repository Layout

```
Distributed_Object_Store/
├── cmd/
│   ├── controller/
│   │   └── main.go              # Controller entry point
│   └── worker/
│       └── main.go              # Worker entry point
│
├── internal/
│   ├── controller/
│   │   ├── api/                 # HTTP handlers (transport layer)
│   │   ├── service/             # Business logic (service layer)
│   │   ├── metadata/            # bbolt repository (infrastructure)
│   │   ├── ring/                # Consistent hash ring (infrastructure)
│   │   ├── health/              # Heartbeat lease table (infrastructure)
│   │   ├── healing/             # Self-healing rebalancer (service)
│   │   ├── replication/         # Fan-out, hedged reads, read-repair (service)
│   │   ├── bloom/               # Bloom filter wrapper (infrastructure)
│   │   ├── gc/                  # Garbage collector — PENDING_DELETE + STAGED (service)
│   │   └── workerclient/        # HTTP client per worker + circuit breaker (infrastructure)
│   │
│   └── worker/
│       ├── api/                 # HTTP handlers (transport layer)
│       ├── storage/             # Chunk store — read/write/delete (infrastructure)
│       └── scrubber/            # CRC32C background scrubber (service)
│
├── pkg/
│   └── models/                  # Shared structs — ObjectManifest, ChunkRecord, etc.
│
├── docs/
│   ├── DESIGN.md                # High-Level Design
│   ├── LLD.md                   # This file
│   └── diagrams/                # Mermaid UML files
│
├── deploy/
│   ├── docker-compose.yml
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── datasources/
│       └── dashboards/
│
├── go.mod
├── go.sum
└── README.md
```

---

## 3. Controller — Module Breakdown

### 3.1 `cmd/controller/main.go`
**Responsibility:** Wire all dependencies together and start the server.
- Reads env vars, constructs all infrastructure adapters (bbolt, bloom filter, ring, worker clients).
- Injects adapters into service layer via interfaces.
- Starts background goroutines (healer, reconciler, GC).
- Registers OS signal handler for graceful shutdown.

---

### 3.2 `internal/controller/api/`
**Responsibility:** HTTP transport only. Parse → call service → serialize.

| File | Handler |
|---|---|
| `objects.go` | `POST /api/v1/objects`, `GET /api/v1/objects`, `GET /api/v1/objects/{id}` |
| `metadata.go` | `GET /api/v1/objects/{id}/metadata`, `PATCH /api/v1/objects/{id}/metadata` |
| `delete.go` | `DELETE /api/v1/objects/{id}` |
| `workers.go` | `POST /internal/workers/register`, `POST /internal/workers/heartbeat` |
| `middleware.go` | Request ID injection, structured logging, Prometheus request metrics |

**Rule:** Handlers never touch bbolt or worker HTTP clients directly. All calls go through service interfaces.

---

### 3.3 `internal/controller/service/`
**Responsibility:** All business logic. Orchestrates metadata, ring, replication, and GC.

| File | Responsibility |
|---|---|
| `upload.go` | Stream chunking, CAS check, fan-out, WAL commit |
| `download.go` | Manifest lookup, hedged read orchestration, SHA-256 verification, read-repair dispatch |
| `metadata.go` | Metadata reads, PATCH with ETag/If-Match, version increment |
| `delete.go` | Object deletion, ref_count decrement, PENDING_DELETE scheduling |
| `list.go` | Paginated object listing |
| `registration.go` | Worker registration, UNKNOWN→ONLINE transition, async inventory reconcile |

---

### 3.4 `internal/controller/metadata/`
**Responsibility:** All bbolt reads and writes. The only package that imports bbolt.

| File | Responsibility |
|---|---|
| `store.go` | `MetadataStore` struct wrapping `*bbolt.DB` |
| `objects.go` | Object manifest CRUD |
| `chunks.go` | ChunkRecord CRUD — ref_count, state transitions (PENDING→DURABLE→PENDING_DELETE) |
| `workers.go` | WorkerRecord CRUD |
| `idempotency.go` | Idempotency key read/write/expire |

**Transaction rule:** Any operation that touches more than one bucket (e.g., flip STAGED→COMMITTED + write idempotency key) uses a single `db.Update()` call. No cross-bucket logic is split across multiple transactions.

---

### 3.5 `internal/controller/ring/`
**Responsibility:** Consistent hash ring. Pure in-memory, no I/O.

| File | Responsibility |
|---|---|
| `ring.go` | `Ring` struct — sorted vnode array, `sync.RWMutex` |
| `placement.go` | `Primary(chunkID)`, `Secondary(chunkID)` — returns WorkerID |
| `vnode.go` | Vnode construction — `hash(workerID + ":" + strconv.Itoa(i))` for i in 0..49 |

**Key invariant:** The ring is used only for placing *new* chunks. Existing chunk locations are always read from the `replicas` field in bbolt ChunkRecord — never recomputed from the ring.

---

### 3.6 `internal/controller/health/`
**Responsibility:** Heartbeat lease table and worker state machine.

| File | Responsibility |
|---|---|
| `lease.go` | `LeaseTable` — `sync.RWMutex`-protected map of `workerID → lastSeen` |
| `monitor.go` | Ticker goroutine — checks for missed heartbeats, emits OFFLINE events |
| `states.go` | Worker state constants: `UNKNOWN`, `ONLINE`, `OFFLINE` |

**State transitions:**
```
Boot          → UNKNOWN  (all previously registered workers)
Heartbeat/Register received → ONLINE
3 missed heartbeats (6s)    → OFFLINE  (triggers healing after 30s delay)
Re-registration             → ONLINE   (cancels pending heal if within 30s window)
```

---

### 3.7 `internal/controller/healing/`
**Responsibility:** Detect DEGRADED chunks and orchestrate P2P repair.

| File | Responsibility |
|---|---|
| `healer.go` | Ticker goroutine — scans bbolt for chunks with replica_count < 2 |
| `scheduler.go` | 30s delay queue before dispatching repair — cancellable if worker recovers |
| `dispatch.go` | Calls `POST /internal/workers/replicate-peer` on surviving worker |

---

### 3.8 `internal/controller/replication/`
**Responsibility:** Chunk fan-out during upload, hedged reads, read-repair.

| File | Responsibility |
|---|---|
| `fanout.go` | Concurrent PUT to primary + secondary via `errgroup`; waits for 2-of-2 ACK |
| `reader.go` | Hedged read — 25ms timer, `context.WithCancel` on loser |
| `repair.go` | Async read-repair dispatch after SHA-256 mismatch |

---

### 3.9 `internal/controller/bloom/`
**Responsibility:** Bloom filter wrapper over chunk existence.

| File | Responsibility |
|---|---|
| `filter.go` | `BloomFilter` struct — wraps `bloom.BloomFilter`, `sync.RWMutex` |
| `rebuild.go` | Scans bbolt chunks bucket on startup to populate filter |

---

### 3.10 `internal/controller/gc/`
**Responsibility:** Two GC passes.

| File | Responsibility |
|---|---|
| `staged.go` | Periodic scanner (every 5 min) — aborts STAGED objects older than timeout |
| `chunks.go` | PENDING_DELETE pass (every 30s) — re-checks ref_count before deleting from workers |
| `inventory.go` | Periodic worker-inventory GC (every 10 min) — deletes orphan chunks on workers |

---

### 3.11 `internal/controller/workerclient/`
**Responsibility:** HTTP client per worker with circuit breaker.

| File | Responsibility |
|---|---|
| `client.go` | `WorkerClient` — wraps `http.Client`, exposes `PutChunk`, `GetChunk`, `DeleteChunk`, `ReplicatePeer`, `ListChunks` |
| `circuitbreaker.go` | Per-client circuit breaker — CLOSED/OPEN/HALF-OPEN state machine |
| `pool.go` | `WorkerPool` — map of `workerID → *WorkerClient`, protected by `sync.RWMutex` |

---

## 4. Worker — Module Breakdown

### 4.1 `cmd/worker/main.go`
- Reads env vars (WORKER_ID, CONTROLLER_ADDRESS, DATA_DIR, WORKER_PORT).
- Constructs storage and scrubber.
- Registers with controller (with retry loop).
- Starts heartbeat goroutine.
- Starts HTTP server.

---

### 4.2 `internal/worker/api/`

| File | Handler |
|---|---|
| `chunks.go` | `PUT /internal/chunks/{id}`, `GET /internal/chunks/{id}`, `DELETE /internal/chunks/{id}` |
| `inventory.go` | `GET /internal/workers/{id}/chunks` |
| `peer.go` | `POST /internal/workers/replicate-peer` |

---

### 4.3 `internal/worker/storage/`
**Responsibility:** All disk I/O for chunks. The only package that touches the filesystem.

| File | Responsibility |
|---|---|
| `store.go` | `ChunkStore` struct — base data directory |
| `write.go` | Atomic write: temp file → fsync → rename → fsync dir. CRC32C written as file footer. |
| `read.go` | Read chunk bytes — `io.Copy` from `*os.File` for sendfile path |
| `delete.go` | Hard unlink of chunk file |
| `inventory.go` | Walk data directory, return all chunk IDs present on disk |

**Atomic write sequence (enforced in `write.go`):**
```
1. os.CreateTemp(dataDir, chunkID+".tmp")
2. Write chunk bytes
3. Append CRC32C footer (4 bytes)
4. file.Sync()  ← fsync
5. os.Rename(tmpPath, finalPath)
6. fsync directory fd
7. Return 201 to caller
```

---

### 4.4 `internal/worker/scrubber/`

| File | Responsibility |
|---|---|
| `scrubber.go` | Ticker goroutine (30s) — walks data dir, reads CRC footer, recomputes CRC32C, alerts controller on mismatch |
| `alert.go` | `POST /internal/scrubber/alert` to controller (or direct repair trigger) |

---

## 5. Key Interfaces

The service layer depends only on these interfaces — never on concrete types. Each interface has exactly one implementation, injected at startup from `cmd/*/main.go`.

```go
// pkg/models/interfaces.go
// package models
// imports: context, io, time, errors

// --- Sentinel errors ---
var (
    ErrNotFound      = errors.New("not found")
    ErrAlreadyExists = errors.New("already exists")
    ErrNoWorkers     = errors.New("no healthy workers available")
    ErrLockHeld      = errors.New("idempotency key already processing")
)

// MetadataRepository — implemented by internal/controller/metadata
// Only package that imports bbolt. All methods are goroutine-safe (bbolt handles locking).
type MetadataRepository interface {
    // Objects
    SaveObject(ctx context.Context, obj *ObjectManifest) error
    GetObject(ctx context.Context, objectID string) (*ObjectManifest, error)
    UpdateObjectStatus(ctx context.Context, objectID string, status ObjectStatus) error
    ListObjects(ctx context.Context) ([]*ObjectManifest, error)
    ListObjectsByStatus(ctx context.Context, status ObjectStatus) ([]*ObjectManifest, error)
    DeleteObject(ctx context.Context, objectID string) error

    // ChunkRefs — ordered per-object chunk list, stored in separate obj_chunks bucket
    // Implementation uses bbolt cursor to avoid loading all refs into memory at once.
    SaveChunkRefs(ctx context.Context, objectID string, refs []ChunkRef) error
    GetChunkRefs(ctx context.Context, objectID string) ([]ChunkRef, error)

    // Chunks
    SaveChunk(ctx context.Context, chunk *ChunkRecord) error
    GetChunk(ctx context.Context, chunkID string) (*ChunkRecord, error)
    UpdateChunkState(ctx context.Context, chunkID string, state ChunkState) error
    AddChunkReplica(ctx context.Context, chunkID string, replica Replica) error
    RemoveChunkReplica(ctx context.Context, chunkID string, workerID string) error
    IncrementChunkRefCount(ctx context.Context, chunkID string) error
    // Returns new count — caller deletes the chunk if count == 0
    DecrementChunkRefCount(ctx context.Context, chunkID string) (newCount int, err error)
    DeleteChunk(ctx context.Context, chunkID string) error
    ListChunksByState(ctx context.Context, state ChunkState) ([]*ChunkRecord, error)
    ListUnderReplicatedChunks(ctx context.Context, minReplicas int) ([]*ChunkRecord, error)

    // Workers
    SaveWorker(ctx context.Context, worker *WorkerRecord) error
    GetWorker(ctx context.Context, workerID string) (*WorkerRecord, error)
    UpdateWorkerStatus(ctx context.Context, workerID string, status WorkerStatus) error
    ListWorkers(ctx context.Context) ([]*WorkerRecord, error)

    // Idempotency — must be called in this sequence for 2PC upload correctness:
    //   1. AcquireIdempotencyLock — returns existing record if key seen before
    //   2. ... do upload work ...
    //   3. CommitIdempotencyResult — atomically flip PROCESSING → COMMITTED
    // Returns (nil, nil) if lock was just acquired (proceed with upload).
    // Returns (record, nil) if key already exists — replay the cached response.
    AcquireIdempotencyLock(ctx context.Context, key string) (existing *IdempotencyRecord, err error)
    CommitIdempotencyResult(ctx context.Context, key string, statusCode int, body []byte) error
    PurgeExpiredIdempotencyKeys(ctx context.Context) error
}

// PlacementStrategy — implemented by internal/controller/ring
// Pure in-memory, no I/O. All methods are goroutine-safe (sync.RWMutex inside).
type PlacementStrategy interface {
    // Returns ErrNoWorkers if ring is empty.
    Primary(chunkID string) (workerID string, err error)
    // Returns ErrNoWorkers if no healthy worker other than excludeWorkerID exists.
    Secondary(chunkID string, excludeWorkerID string) (workerID string, err error)
    AddWorker(workerID string, address string)
    RemoveWorker(workerID string)
    Workers() []string
}

// ChunkStore — implemented by internal/worker/storage
// Only package that touches the filesystem. All writes are atomic (temp→fsync→rename→fsync dir).
type ChunkStore interface {
    // Write streams data to disk atomically, appends CRC32C footer.
    Write(ctx context.Context, chunkID string, data io.Reader, size int64) error
    // Read returns a ReadCloser — caller MUST close it. Enables zero-copy sendfile path.
    Read(ctx context.Context, chunkID string) (io.ReadCloser, error)
    Delete(ctx context.Context, chunkID string) error
    Exists(ctx context.Context, chunkID string) (bool, error)
    List(ctx context.Context) ([]string, error)
    // VerifyCRC reads stored footer and recomputes CRC32C — used by background scrubber.
    VerifyCRC(ctx context.Context, chunkID string) (ok bool, err error)
}

// WorkerClientPool — implemented by internal/controller/workerclient
// Wraps all outbound HTTP calls to workers. Each call goes through the worker's circuit breaker.
// Takes workerAddr (host:port) not workerID — the pool is stateless; callers resolve the address.
type WorkerClientPool interface {
    PutChunk(ctx context.Context, workerAddr string, chunkID string, data io.Reader, size int64) error
    // Returns ReadCloser — caller MUST close it.
    GetChunk(ctx context.Context, workerAddr string, chunkID string) (io.ReadCloser, error)
    DeleteChunk(ctx context.Context, workerAddr string, chunkID string) error
    // Tells sourceAddr to stream chunkID directly to targetAddr (P2P — controller not in data path).
    ReplicatePeer(ctx context.Context, sourceAddr string, chunkID string, targetAddr string) error
    ListChunks(ctx context.Context, workerAddr string) ([]string, error)
}

// HealthMonitor — implemented by internal/controller/health
// In-memory lease table. No context needed — all reads are map lookups under RWMutex.
type HealthMonitor interface {
    // RecordHeartbeat is called on every heartbeat and on worker registration.
    RecordHeartbeat(workerID string)
    GetWorkerStatus(workerID string) WorkerStatus
    ListOnlineWorkers() []string
    // Subscribe returns a channel that receives an event each time a worker goes OFFLINE.
    // The healing goroutine blocks on this channel — idiomatic Go Observer pattern.
    Subscribe() <-chan WorkerOfflineEvent
}

// ExistenceFilter — implemented by internal/controller/bloom
// Probabilistic: MightExist can return true for a chunk that doesn't exist (false positive).
// Never returns false for a chunk that does exist (no false negatives).
// No Remove method — standard bloom filters don't support deletion.
type ExistenceFilter interface {
    MightExist(chunkID string) bool
    Add(chunkID string)
}
```

---

## 6. Design Patterns

All patterns below are from the Gang of Four (GoF) catalog unless marked with `*` (enterprise pattern, universally adopted but not in the original 23).

---

### 6.1 Strategy — Chunk Placement
**GoF category:** Behavioural
**Where:** `internal/controller/ring/` implements `PlacementStrategy` interface
**What:** The chunk placement algorithm is defined as an interface with `Primary(chunkID)` and `Secondary(chunkID, exclude)` methods. The consistent hash ring is one concrete strategy injected at startup.
**Why:** If placement needs to change (e.g., swap consistent hashing for rendezvous hashing), only the concrete struct changes. The entire service layer is unaffected.

```go
type PlacementStrategy interface {
    Primary(chunkID string) (workerID string, err error)
    Secondary(chunkID string, excludeWorkerID string) (workerID string, err error)
    AddWorker(workerID string)
    RemoveWorker(workerID string)
}
// Concrete: HashRing implements PlacementStrategy
```

---

### 6.2 State — Circuit Breaker per Worker
**GoF category:** Behavioural
**Where:** `internal/controller/workerclient/circuitbreaker.go`
**What:** Each worker client owns a `CircuitBreaker` struct with a `state` field. The struct's behaviour changes based on which state it is in. This is the GoF State pattern: the same `Call()` method behaves differently depending on current state.
**Why:** Models the CLOSED/OPEN/HALF-OPEN lifecycle cleanly. Each state encapsulates its own transition logic.

```go
type cbState int
const (
    StateClosed   cbState = iota // normal — forward calls
    StateOpen                    // failing — reject immediately
    StateHalfOpen                // recovering — allow one probe
)

type CircuitBreaker struct {
    state        cbState
    failures     int
    lastFailure  time.Time
    mu           sync.Mutex
}

func (cb *CircuitBreaker) Allow() bool { ... }  // behaviour differs per state
func (cb *CircuitBreaker) OnSuccess()  { ... }
func (cb *CircuitBreaker) OnFailure()  { ... }
```

---

### 6.3 Decorator — Worker Client Cross-Cutting Concerns
**GoF category:** Structural
**Where:** `internal/controller/workerclient/client.go`
**What:** `WorkerClient.PutChunk()` layers three concerns around the raw HTTP call — circuit breaker check, actual HTTP call, Prometheus metric recording — each as a wrapper. The caller sees one `PutChunk()` call; the decoration is invisible.
**Why:** Metrics and circuit breaking are cross-cutting concerns. Decorating keeps the core HTTP call clean and each concern independently changeable.

```
PutChunk() call stack:
  → circuit breaker Allow()?
      → raw http.Put(...)
          → record duration metric
              → return
```

---

### 6.4 Observer — Health Monitor to Healer
**GoF category:** Behavioural
**Where:** `internal/controller/health/monitor.go` (subject) → `internal/controller/healing/healer.go` (observer)
**What:** The health monitor is the subject. When it declares a worker OFFLINE, it sends a `WorkerOfflineEvent` on a Go channel. The healer goroutine is the observer — it blocks on the channel and reacts when an event arrives. Go channels are the idiomatic implementation of Observer in Go.
**Why:** Decouples failure detection from repair logic. The monitor has no knowledge of the healer.

```go
// Subject (health monitor) sends to:
offlineEvents chan<- WorkerOfflineEvent

// Observer (healer) reads from:
offlineEvents <-chan WorkerOfflineEvent
```

---

### 6.5 Template Method — Atomic Chunk Write
**GoF category:** Behavioural
**Where:** `internal/worker/storage/write.go`
**What:** The atomic write sequence is a fixed algorithm skeleton: create temp file → write bytes → fsync → rename → fsync directory. The data source (new upload vs read-repair overwrite) is a parameter, but the sequence never varies. `writeChunk(chunkID string, src io.Reader)` is the template method.
**Why:** Ensures the fsync+rename atomicity guarantee can never be bypassed by a caller taking a shortcut. Any future write path reuses the same function.

```
writeChunk(chunkID, src io.Reader):
  1. os.CreateTemp  — hook: caller provides src
  2. io.Copy(tmp, src)
  3. tmp.Sync()
  4. os.Rename(tmp, final)
  5. syncDir()
  6. return
```

---

### 6.6 Facade — Worker Client Pool
**GoF category:** Structural
**Where:** `internal/controller/workerclient/pool.go`
**What:** `WorkerPool` presents a single, simple interface (`PutChunk`, `GetChunk`, `DeleteChunk`, `ReplicatePeer`, `ListChunks`) to the service layer. Internally it maintains a map of per-worker `WorkerClient` structs, each with its own circuit breaker. The service layer never addresses individual workers directly.
**Why:** Hides the complexity of per-worker connection management, circuit breaker state, and address resolution behind one simple interface.

---

### 6.7 Proxy — Individual Worker Client
**GoF category:** Structural
**Where:** `internal/controller/workerclient/client.go`
**What:** `WorkerClient` is a local proxy for a remote worker node. It has the same method signatures (`PutChunk`, `GetChunk`, etc.) that the worker's HTTP API exposes, but all the network I/O is encapsulated inside. The caller interacts with it as if calling the worker directly.
**Why:** Standard remote proxy — hides network communication behind a local object interface.

---

### 6.8 Command — Repair and GC Tasks
**GoF category:** Behavioural
**Where:** `internal/controller/gc/` and `internal/controller/healing/`
**What:** Each repair task and GC deletion is encapsulated as a `Command` struct with an `Execute()` method. The healing scheduler queues `RepairCommand{chunkID, targetWorkerID}` objects. The GC queues `DeleteChunkCommand{chunkID, workerIDs}` objects. Commands can be queued, delayed, cancelled, and retried independently.
**Why:** Encapsulating each operation as an object enables the 30-second healing delay (commands sit in a queue), cancellation when a worker recovers, and retry on failure — all without the scheduler knowing what the command does.

```go
type Command interface {
    Execute(ctx context.Context) error
    Cancel()
}

type RepairCommand struct {
    ChunkID        string
    SourceWorkerID string
    TargetWorkerID string
    cancelFn       context.CancelFunc
}

type DeleteChunkCommand struct {
    ChunkID   string
    WorkerIDs []string
}
```

---

### 6.9 Singleton — bbolt Database + Bloom Filter
**GoF category:** Creational
**Where:** `cmd/controller/main.go` wiring
**What:** Exactly one `*bbolt.DB` instance and one `BloomFilter` instance exist for the lifetime of the controller process. Both are constructed once in `main.go` and injected everywhere via interfaces. Go's `sync.Once` ensures no accidental second initialization.
**Why:** bbolt is a single-writer embedded database — opening it twice from the same process would deadlock. The bloom filter must be a single shared structure so all upload goroutines see the same state.

---

### 6.10 Factory Method — Worker Client Construction
**GoF category:** Creational
**Where:** `internal/controller/workerclient/pool.go`
**What:** `WorkerPool.AddWorker(record WorkerRecord) WorkerClient` is the factory method. It constructs a `WorkerClient` with the correct address, HTTP timeout settings, and a fresh `CircuitBreaker`. The caller (registration service) does not construct `WorkerClient` directly.
**Why:** Centralises construction logic. If client configuration changes (e.g., add TLS, change timeout), only the factory method changes.

---

### Summary Table

| Pattern | GoF Category | Where Used |
|---|---|---|
| Strategy | Behavioural | Consistent hash ring placement |
| State | Behavioural | Circuit breaker (CLOSED/OPEN/HALF-OPEN) |
| Decorator | Structural | Worker client — metrics + circuit breaker wrapping |
| Observer | Behavioural | Health monitor → healer via Go channel |
| Template Method | Behavioural | Atomic chunk write sequence |
| Facade | Structural | WorkerPool hides per-worker complexity |
| Proxy | Structural | WorkerClient as remote proxy for worker node |
| Command | Behavioural | Repair tasks and GC delete tasks |
| Singleton | Creational | bbolt DB instance, bloom filter |
| Factory Method | Creational | WorkerClient construction in WorkerPool |

---

## 7. Core Data Structures

```go
// pkg/models/models.go

type ObjectStatus string
const (
    StatusStaged    ObjectStatus = "STAGED"
    StatusCommitted ObjectStatus = "COMMITTED"
    StatusDeleted   ObjectStatus = "DELETED"
    StatusFailed    ObjectStatus = "FAILED"
)

type ObjectManifest struct {
    ID             string            `json:"object_id"`
    Name           string            `json:"name"`
    SizeBytes      int64             `json:"size_bytes"`
    ContentType    string            `json:"content_type"`
    Status         ObjectStatus      `json:"status"`
    Checksum       string            `json:"checksum"`        // SHA-256 of full object
    ChunkCount     int               `json:"chunk_count"`
    Chunks         []ChunkRef        `json:"chunks"`          // ordered
    CustomMetadata map[string]string `json:"custom_metadata"`
    Version        int64             `json:"version"`         // ETag value
    CreatedAt      time.Time         `json:"created_at"`
    ModifiedAt     time.Time         `json:"modified_at"`
}

type ChunkRef struct {
    ChunkID    string `json:"chunk_id"`
    Index      int    `json:"index"`
    Offset     int64  `json:"offset"`
    Size       int    `json:"size"`
    PrimaryID  string `json:"primary_worker_id"`
    SecondaryID string `json:"secondary_worker_id"`
}

type ChunkState string
const (
    ChunkPending       ChunkState = "PENDING"        // fan-out in progress
    ChunkDurable       ChunkState = "DURABLE"        // both replicas ACKed
    ChunkPendingDelete ChunkState = "PENDING_DELETE" // ref_count reached 0
)

type ChunkRecord struct {
    ChunkID      string     `json:"chunk_id"`
    SizeBytes    int        `json:"size_bytes"`
    RefCount     int        `json:"ref_count"`
    State        ChunkState `json:"state"`
    Replicas     []Replica  `json:"replicas"`
    CreatedAt    time.Time  `json:"created_at"`
}

type Replica struct {
    WorkerID   string `json:"worker_id"`
    WorkerAddr string `json:"worker_addr"`
}

type WorkerStatus string
const (
    WorkerUnknown WorkerStatus = "UNKNOWN"
    WorkerOnline  WorkerStatus = "ONLINE"
    WorkerOffline WorkerStatus = "OFFLINE"
)

type WorkerRecord struct {
    WorkerID     string       `json:"worker_id"`
    Address      string       `json:"address"`
    Port         int          `json:"port"`
    CapacityBytes int64       `json:"capacity_bytes"`
    Status       WorkerStatus `json:"status"`
    LastSeen     time.Time    `json:"last_seen"`
    VnodeCount   int          `json:"vnode_count"`
}

type IdempotencyRecord struct {
    Key        string    `json:"key"`
    Status     string    `json:"status"`  // "PROCESSING" | "COMPLETE"
    StatusCode int       `json:"status_code"`
    Body       []byte    `json:"body"`
    ExpiresAt  time.Time `json:"expires_at"`
}

type WorkerOfflineEvent struct {
    WorkerID  string
    Timestamp time.Time
}
```

---

## 8. Inter-Service Communication

### Controller → Worker (outbound HTTP)

All calls go through `WorkerClientPool`. The circuit breaker wraps each call.

| Call | Method | When |
|---|---|---|
| Store chunk | `PUT /internal/chunks/{id}` | Upload fan-out |
| Fetch chunk | `GET /internal/chunks/{id}` | Download, read-repair |
| Delete chunk | `DELETE /internal/chunks/{id}` | GC pass (ref_count = 0) |
| Replicate peer | `POST /internal/workers/replicate-peer` | Self-healing |
| List chunks | `GET /internal/workers/{id}/chunks` | Inventory reconcile on restart |

### Worker → Controller (outbound HTTP)

| Call | Method | When |
|---|---|---|
| Register | `POST /internal/workers/register` | Startup + retry loop |
| Heartbeat | `POST /internal/workers/heartbeat` | Every 2 seconds |
| Scrubber alert | Via repair trigger | CRC32C mismatch detected |

### Content-Type Detection

On upload, if the client does not provide a `Content-Type` header, the controller uses `http.DetectContentType` (Go stdlib) on the first 512 bytes of the file. Falls back to `application/octet-stream` if detection is inconclusive.

---

## 9. Dependency Graph

```
cmd/controller/main.go
  └── controller/api          (imports service interfaces)
  └── controller/service      (imports metadata, ring, replication, health, gc interfaces)
      └── controller/metadata (imports bbolt — only package that does)
      └── controller/ring     (pure in-memory, no I/O)
      └── controller/health   (in-memory lease table)
      └── controller/replication
          └── controller/workerclient (imports net/http)
      └── controller/bloom    (imports bloom library)
      └── controller/gc       (imports metadata + workerclient interfaces)
      └── controller/healing  (imports health events + workerclient interface)
  └── pkg/models              (no imports from internal — shared structs only)

cmd/worker/main.go
  └── worker/api              (imports storage interface)
  └── worker/storage          (imports os, io — only package that touches disk)
  └── worker/scrubber         (imports storage interface)
  └── pkg/models
```

**Key rule:** `pkg/models` has zero imports from `internal/`. It is the shared vocabulary. No circular dependencies allowed — enforced by the layering above.

---

## 10. Change Log

| Date | Change | Reason |
|---|---|---|
| 2026-11-02 | LLD created | Architecture style, modules, interfaces, and patterns finalized |
| 2026-11-02 | Replaced Hexagonal with Layered Architecture, all patterns mapped to GoF | Hexagonal is an architectural philosophy not a GoF pattern; replaced non-GoF patterns (Circuit Breaker→State, Saga→Command, Repository→Facade/Proxy) |
| 2026-11-11 | Finalized all 6 interface signatures in §5 with full method set, sentinel errors, and inline rationale comments | Ready for implementation — interfaces are the contract both tracks code against |
