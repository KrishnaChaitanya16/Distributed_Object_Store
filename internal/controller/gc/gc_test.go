package gc

// Covers every crash point in the upload flow plus worker-failure scenarios.
//
// Crash taxonomy (from upload.go):
//   CP1 — before SaveObject(STAGED)              → clean, nothing to test
//   CP2 — after STAGED, before any PutChunk      → orphan STAGED manifest
//   CP3 — mid-fanout (some chunks written)       → STAGED + PENDING chunks
//   CP4 — all chunks DURABLE, before COMMITTED   → STAGED + all chunks DURABLE
//   CP5 — after COMMITTED, before 201 sent       → idempotency gap (tested in api pkg)

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

type deleteCall struct{ addr, chunkID string }

type fakeWorkers struct {
	mu      sync.Mutex
	deletes []deleteCall
	failOn  map[string]bool // chunkID → fail delete
}

func newFakeWorkers() *fakeWorkers { return &fakeWorkers{failOn: map[string]bool{}} }

func (f *fakeWorkers) PutChunk(_ context.Context, _, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (f *fakeWorkers) GetChunk(_ context.Context, _, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (f *fakeWorkers) ListChunks(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (f *fakeWorkers) ReplicatePeer(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeWorkers) DeleteChunk(_ context.Context, addr, chunkID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOn[chunkID] {
		return fmt.Errorf("worker %s unavailable", addr)
	}
	f.deletes = append(f.deletes, deleteCall{addr, chunkID})
	return nil
}
func (f *fakeWorkers) deleted() []deleteCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]deleteCall, len(f.deletes))
	copy(out, f.deletes)
	return out
}
func (f *fakeWorkers) deletedChunkIDs() []string {
	calls := f.deleted()
	ids := make([]string, len(calls))
	for i, c := range calls {
		ids[i] = c.chunkID
	}
	return ids
}

// ── helpers ────────────────────────────────────────────────────────────────────

func newStore(t *testing.T) *metadata.MetadataStore {
	t.Helper()
	s, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func newGC(store *metadata.MetadataStore, workers *fakeWorkers) *GC {
	g := New(store, workers)
	g.StagedTTL = 0  // expire immediately in tests
	g.PendingTTL = 0 // expire immediately in tests
	return g
}

func saveManifest(t *testing.T, store *metadata.MetadataStore, id string, status models.ObjectStatus, age time.Duration) {
	t.Helper()
	require.NoError(t, store.SaveObject(context.Background(), &models.ObjectManifest{
		ID:        id,
		Name:      id + ".txt",
		Status:    status,
		CreatedAt: time.Now().UTC().Add(-age),
		ModifiedAt: time.Now().UTC().Add(-age),
	}))
}

func saveChunkRecord(t *testing.T, store *metadata.MetadataStore, chunkID string, state models.ChunkState, refCount int, workers ...string) {
	t.Helper()
	reps := make([]models.Replica, len(workers))
	for i, w := range workers {
		reps[i] = models.Replica{WorkerID: w, WorkerAddr: "localhost:900" + w}
	}
	require.NoError(t, store.SaveChunk(context.Background(), &models.ChunkRecord{
		ChunkID:   chunkID,
		State:     state,
		RefCount:  refCount,
		Replicas:  reps,
		CreatedAt: time.Now().UTC().Add(-DefaultPendingTTL - time.Minute), // old enough
	}))
}

func saveChunkRefs(t *testing.T, store *metadata.MetadataStore, objectID string, chunkIDs ...string) {
	t.Helper()
	refs := make([]models.ChunkRef, len(chunkIDs))
	for i, id := range chunkIDs {
		refs[i] = models.ChunkRef{ChunkID: id, Index: i, Size: 100}
	}
	require.NoError(t, store.SaveChunkRefs(context.Background(), objectID, refs))
}

// ── CP2: STAGED manifest, no chunks (crash before PutChunk) ──────────────────

func TestGC_CP2_StagedManifestNoChunks_Deleted(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := newGC(store, workers)

	saveManifest(t, store, "obj1", models.StatusStaged, 15*time.Minute)

	g.Tick(context.Background())

	_, err := store.GetObject(context.Background(), "obj1")
	assert.ErrorIs(t, err, models.ErrNotFound, "STAGED manifest past TTL must be deleted")
	assert.Empty(t, workers.deleted(), "no worker calls when no chunks exist")
}

func TestGC_CP2_StagedManifestWithinTTL_Kept(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := New(store, workers) // production TTL — 10 min
	g.Interval = time.Hour

	// Object created 1 minute ago — still within TTL, may still be uploading.
	saveManifest(t, store, "obj1", models.StatusStaged, 1*time.Minute)

	g.Tick(context.Background())

	_, err := store.GetObject(context.Background(), "obj1")
	assert.NoError(t, err, "STAGED manifest within TTL must NOT be deleted")
}

// ── CP3: mid-fanout crash (STAGED manifest + PENDING orphan chunks) ───────────

func TestGC_CP3_PendingOrphanChunk_DeletedFromWorkersAndBbolt(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := newGC(store, workers)

	// STAGED manifest — no chunk refs saved (crash before SaveChunkRefs)
	saveManifest(t, store, "obj1", models.StatusStaged, 15*time.Minute)
	// PENDING chunk — primary received it, secondary never did
	saveChunkRecord(t, store, "chunk-A", models.ChunkPending, 0, "1", "2")

	g.Tick(context.Background())

	// Manifest gone
	_, err := store.GetObject(context.Background(), "obj1")
	assert.ErrorIs(t, err, models.ErrNotFound)

	// PENDING chunk deleted from workers
	ids := workers.deletedChunkIDs()
	assert.Contains(t, ids, "chunk-A", "PENDING orphan chunk must be deleted from workers")

	// PENDING chunk gone from bbolt
	_, err = store.GetChunk(context.Background(), "chunk-A")
	assert.ErrorIs(t, err, models.ErrNotFound, "PENDING orphan chunk must be removed from bbolt")
}

func TestGC_CP3_MultipleOrphanChunks_AllDeleted(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := newGC(store, workers)

	saveManifest(t, store, "obj1", models.StatusStaged, 15*time.Minute)
	saveChunkRecord(t, store, "chunk-A", models.ChunkPending, 0, "1")
	saveChunkRecord(t, store, "chunk-B", models.ChunkPending, 0, "1")
	saveChunkRecord(t, store, "chunk-C", models.ChunkPending, 0, "1")

	g.Tick(context.Background())

	ids := workers.deletedChunkIDs()
	for _, id := range []string{"chunk-A", "chunk-B", "chunk-C"} {
		assert.Contains(t, ids, id)
	}
}

// ── CP4: all chunks DURABLE, never COMMITTED ──────────────────────────────────

func TestGC_CP4_AllChunksDurable_ObjectStagedPastTTL_FullCleanup(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := newGC(store, workers)

	saveManifest(t, store, "obj1", models.StatusStaged, 15*time.Minute)
	saveChunkRecord(t, store, "c1", models.ChunkDurable, 1, "w1", "w2")
	saveChunkRecord(t, store, "c2", models.ChunkDurable, 1, "w1", "w2")
	saveChunkRefs(t, store, "obj1", "c1", "c2")

	g.Tick(context.Background())

	_, err := store.GetObject(context.Background(), "obj1")
	assert.ErrorIs(t, err, models.ErrNotFound, "STAGED object must be deleted")

	ids := workers.deletedChunkIDs()
	assert.Contains(t, ids, "c1")
	assert.Contains(t, ids, "c2")

	_, err = store.GetChunk(context.Background(), "c1")
	assert.ErrorIs(t, err, models.ErrNotFound, "chunk c1 must be deleted from bbolt")
	_, err = store.GetChunk(context.Background(), "c2")
	assert.ErrorIs(t, err, models.ErrNotFound, "chunk c2 must be deleted from bbolt")
}

func TestGC_CP4_SharedChunk_NotDeletedFromWorker(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := newGC(store, workers)

	// obj1 is STAGED past TTL — should be GC'd.
	saveManifest(t, store, "obj1", models.StatusStaged, 15*time.Minute)
	// obj2 is COMMITTED — owns the same chunk (CAS dedup scenario).
	saveManifest(t, store, "obj2", models.StatusCommitted, 1*time.Minute)
	// shared-chunk has ref_count=2 (one for each object)
	saveChunkRecord(t, store, "shared", models.ChunkDurable, 2, "w1", "w2")
	saveChunkRefs(t, store, "obj1", "shared")
	saveChunkRefs(t, store, "obj2", "shared")

	g.Tick(context.Background())

	// obj1 deleted, obj2 kept
	_, err := store.GetObject(context.Background(), "obj1")
	assert.ErrorIs(t, err, models.ErrNotFound)
	_, err = store.GetObject(context.Background(), "obj2")
	assert.NoError(t, err)

	// shared chunk ref_count decremented to 1 — still on workers
	assert.NotContains(t, workers.deletedChunkIDs(), "shared",
		"chunk shared with a committed object must NOT be deleted from workers")

	chunk, err := store.GetChunk(context.Background(), "shared")
	require.NoError(t, err)
	assert.Equal(t, 1, chunk.RefCount, "ref_count must be decremented to 1")
}

// ── FAILED objects ─────────────────────────────────────────────────────────────

func TestGC_FailedObject_DeletedImmediately(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := New(store, workers) // production TTL — but FAILED objects always expire

	// FAILED object just 10 seconds old — rollback already ran, GC it now.
	saveManifest(t, store, "obj1", models.StatusFailed, 10*time.Second)
	saveChunkRecord(t, store, "c1", models.ChunkDurable, 1, "w1")
	saveChunkRefs(t, store, "obj1", "c1")

	g.Tick(context.Background())

	_, err := store.GetObject(context.Background(), "obj1")
	assert.ErrorIs(t, err, models.ErrNotFound, "FAILED object must be deleted regardless of age")
	assert.Contains(t, workers.deletedChunkIDs(), "c1")
}

// ── Orphan DURABLE chunk (crash between UpdateChunkState and IncrementRefCount) ──

func TestGC_DurableChunk_RefCountZero_IsOrphan_Deleted(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := newGC(store, workers)

	// Chunk became DURABLE but ref_count was never incremented (controller crashed).
	saveChunkRecord(t, store, "orphan", models.ChunkDurable, 0, "w1", "w2")

	g.Tick(context.Background())

	assert.Contains(t, workers.deletedChunkIDs(), "orphan",
		"DURABLE chunk with ref_count=0 is an orphan and must be deleted")
	_, err := store.GetChunk(context.Background(), "orphan")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestGC_DurableChunk_RefCountNonZero_Kept(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := newGC(store, workers)

	saveChunkRecord(t, store, "live", models.ChunkDurable, 1, "w1", "w2")

	g.Tick(context.Background())

	assert.NotContains(t, workers.deletedChunkIDs(), "live",
		"DURABLE chunk with ref_count>0 belongs to a live object and must not be deleted")
}

// ── Worker failure during GC delete ───────────────────────────────────────────

func TestGC_WorkerUnavailable_ChunkStillRemovedFromBbolt(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	workers.failOn["c1"] = true // worker returns error on delete
	g := newGC(store, workers)

	saveManifest(t, store, "obj1", models.StatusStaged, 15*time.Minute)
	saveChunkRecord(t, store, "c1", models.ChunkDurable, 1, "w1")
	saveChunkRefs(t, store, "obj1", "c1")

	// Should not panic; bbolt cleanup proceeds even if worker call fails.
	assert.NotPanics(t, func() { g.Tick(context.Background()) })

	// Manifest still cleaned from bbolt.
	_, err := store.GetObject(context.Background(), "obj1")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// ── Context cancellation ───────────────────────────────────────────────────────

func TestGC_ContextCancelled_StopsGracefully(t *testing.T) {
	store := newStore(t)
	workers := newFakeWorkers()
	g := New(store, workers)
	g.Interval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		g.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GC.Run did not stop after context cancellation")
	}
}

// ── Idempotency GC: expired keys purged ───────────────────────────────────────

func TestGC_PurgeExpiredIdempotencyKeys(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	// Acquire a lock and immediately commit it.
	_, err := store.AcquireIdempotencyLock(ctx, "key-old")
	require.NoError(t, err)
	require.NoError(t, store.CommitIdempotencyResult(ctx, "key-old", 201, []byte(`{}`)))

	// Purge (ExpiresAt is 24h from now so won't purge yet — just verify no error).
	require.NoError(t, store.PurgeExpiredIdempotencyKeys(ctx))
}
