package models

import (
	"context"
	"errors"
	"io"
	"time"
)

// --- Sentinel errors ---

var (
	ErrNotFound        = errors.New("not found")
	ErrAlreadyExists   = errors.New("already exists")
	ErrNoWorkers       = errors.New("no healthy workers available")
	ErrLockHeld        = errors.New("idempotency key already processing")
	ErrVersionConflict = errors.New("version conflict")
	ErrNotStaged       = errors.New("upload is no longer in progress")
	ErrChunkBusy       = errors.New("chunk is being garbage collected")
)

// MetadataRepository is implemented by internal/controller/metadata.
// It is the only package that imports bbolt. All methods are goroutine-safe.
type MetadataRepository interface {
	// Objects
	SaveObject(ctx context.Context, obj *ObjectManifest) error
	GetObject(ctx context.Context, objectID string) (*ObjectManifest, error)
	UpdateObjectStatus(ctx context.Context, objectID string, status ObjectStatus) error
	ListObjects(ctx context.Context) ([]*ObjectManifest, error)
	ListObjectsByStatus(ctx context.Context, status ObjectStatus) ([]*ObjectManifest, error)
	DeleteObject(ctx context.Context, objectID string) error
	// PatchCustomMetadata does the read-check-merge-bump atomically in one bbolt transaction.
	// ifMatch < 0 means no version check. Returns ErrNotFound if the object is absent or not
	// COMMITTED; returns ErrVersionConflict if ifMatch >= 0 and doesn't match the current version.
	PatchCustomMetadata(ctx context.Context, id string, ifMatch int64, patch map[string]string) (*ObjectManifest, error)

	// ChunkRefs — ordered per-object chunk list in a separate obj_chunks bucket.
	// Implementation uses a bbolt cursor; callers get a plain slice.
	SaveChunkRefs(ctx context.Context, objectID string, refs []ChunkRef) error
	GetChunkRefs(ctx context.Context, objectID string) ([]ChunkRef, error)

	// Chunks
	SaveChunk(ctx context.Context, chunk *ChunkRecord) error
	GetChunk(ctx context.Context, chunkID string) (*ChunkRecord, error)
	UpdateChunkState(ctx context.Context, chunkID string, state ChunkState) error
	AddChunkReplica(ctx context.Context, chunkID string, replica Replica) error
	RemoveChunkReplica(ctx context.Context, chunkID string, workerID string) error
	IncrementChunkRefCount(ctx context.Context, chunkID string) error
	// DecrementChunkRefCount returns the new count so the caller can decide
	// whether to schedule deletion (count == 0) in the same call.
	DecrementChunkRefCount(ctx context.Context, chunkID string) (newCount int, err error)
	DeleteChunk(ctx context.Context, chunkID string) error
	ListChunksByState(ctx context.Context, state ChunkState) ([]*ChunkRecord, error)
	ListUnderReplicatedChunks(ctx context.Context, minReplicas int) ([]*ChunkRecord, error)

	// Crash-safe upload and garbage collection; each method is a single bbolt transaction.
	AddChunkRef(ctx context.Context, objectID string, ref ChunkRef) (*ChunkRecord, error)
	PrepareChunk(ctx context.Context, chunk *ChunkRecord, replace bool) (*ChunkRecord, error)
	MarkChunkDurable(ctx context.Context, chunkID string) error
	CommitObject(ctx context.Context, obj *ObjectManifest) error
	MarkObjectDeleted(ctx context.Context, objectID string) error
	FailStaleUploads(ctx context.Context, idleBefore time.Time) ([]string, error)
	ListAllChunks(ctx context.Context) ([]*ChunkRecord, error)
	LiveChunkRefs(ctx context.Context) (map[string]int, error)
	ClaimChunksForDeletion(ctx context.Context, chunkIDs []string, holders map[string][]Replica) ([]*ChunkRecord, error)
	RemoveDeletedChunk(ctx context.Context, chunkID string) error
	AdoptChunks(ctx context.Context, holders map[string][]Replica) ([]string, error)
	PurgeDeadObjects(ctx context.Context) (int, error)
	SyncRefCounts(ctx context.Context) error

	// Workers
	SaveWorker(ctx context.Context, worker *WorkerRecord) error
	GetWorker(ctx context.Context, workerID string) (*WorkerRecord, error)
	UpdateWorkerStatus(ctx context.Context, workerID string, status WorkerStatus) error
	ListWorkers(ctx context.Context) ([]*WorkerRecord, error)

	// Idempotency — call in sequence for 2PC upload correctness:
	//   1. AcquireIdempotencyLock — returns existing record if key was seen before
	//   2. ... perform upload ...
	//   3. CommitIdempotencyResult — atomically writes COMMITTED + response body
	//
	// Returns (nil, nil) if the lock was just acquired — caller should proceed.
	// Returns (record, nil) if key already exists — caller replays the cached response.
	// Returns (nil, ErrLockHeld) if key is currently PROCESSING by another request.
	AcquireIdempotencyLock(ctx context.Context, key string) (existing *IdempotencyRecord, err error)
	CommitIdempotencyResult(ctx context.Context, key string, statusCode int, body []byte) error
	PurgeExpiredIdempotencyKeys(ctx context.Context) error
}

// PlacementStrategy is implemented by internal/controller/placement.
// Pure in-memory consistent hash ring. All methods are goroutine-safe.
type PlacementStrategy interface {
	// Primary returns the first clockwise worker for chunkID.
	// Returns ErrNoWorkers if the ring is empty.
	Primary(chunkID string) (workerID string, err error)
	// Secondary returns the next clockwise worker on a different physical node.
	// Returns ErrNoWorkers if fewer than 2 distinct workers are online.
	Secondary(chunkID string, excludeWorkerID string) (workerID string, err error)
	AddWorker(workerID string, address string)
	RemoveWorker(workerID string)
	Workers() []string
}

// ChunkStore is implemented by internal/worker/storage.
// The only package that touches the filesystem. All writes are atomic
// (temp file → fsync → rename → fsync directory).
type ChunkStore interface {
	// Write streams data to disk atomically and appends a CRC32C footer.
	Write(ctx context.Context, chunkID string, data io.Reader, size int64) error
	// Read returns a ReadCloser backed by an os.File — caller MUST close it.
	// This enables the zero-copy sendfile(2) path when copying to a TCP conn.
	Read(ctx context.Context, chunkID string) (io.ReadCloser, error)
	Delete(ctx context.Context, chunkID string) error
	Exists(ctx context.Context, chunkID string) (bool, error)
	List(ctx context.Context) ([]string, error)
	// VerifyCRC recomputes CRC32C and compares against the stored footer.
	// Used by the background scrubber every 30 seconds.
	VerifyCRC(ctx context.Context, chunkID string) (ok bool, err error)
}

// WorkerClientPool is implemented by internal/controller/workerclient.
// Wraps all outbound HTTP calls to workers. Every call passes through
// the per-worker circuit breaker.
// Takes workerAddr (host:port) on every call — the pool is stateless.
type WorkerClientPool interface {
	PutChunk(ctx context.Context, workerAddr string, chunkID string, data io.Reader, size int64) error
	// GetChunk returns a ReadCloser — caller MUST close it.
	GetChunk(ctx context.Context, workerAddr string, chunkID string) (io.ReadCloser, error)
	DeleteChunk(ctx context.Context, workerAddr string, chunkID string) error
	// ReplicatePeer instructs sourceAddr to stream chunkID directly to targetAddr.
	// The controller is not in the data path — this is a P2P transfer.
	ReplicatePeer(ctx context.Context, sourceAddr string, chunkID string, targetAddr string) error
	ListChunks(ctx context.Context, workerAddr string, workerID string) ([]string, error)
}

// HealthMonitor is implemented by internal/controller/health.
// In-memory lease table backed by a sync.RWMutex. No I/O.
type HealthMonitor interface {
	// RecordHeartbeat is called on every worker heartbeat and on registration.
	RecordHeartbeat(workerID string)
	GetWorkerStatus(workerID string) WorkerStatus
	ListOnlineWorkers() []string
	// Subscribe returns a channel that emits one event each time a worker goes OFFLINE.
	// The self-healing goroutine blocks on this channel — idiomatic Go Observer pattern.
	Subscribe() <-chan WorkerOfflineEvent
}

// ExistenceFilter is implemented by internal/controller/bloom.
// Probabilistic: MightExist may return true for a chunk that does not exist
// (false positive). It never returns false for a chunk that does exist.
// No Remove — standard bloom filters do not support deletion.
type ExistenceFilter interface {
	MightExist(chunkID string) bool
	Add(chunkID string)
}
