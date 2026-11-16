package rebalancer

import (
	"context"
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

// fakePlacement implements models.PlacementStrategy with a fixed preferred target.
type fakePlacement struct {
	preferred string // returned by Secondary() when not empty
	err       error  // returned instead of preferred when non-nil
}

func (f *fakePlacement) Primary(_ string) (string, error) { return "", nil }
func (f *fakePlacement) AddWorker(_, _ string)            {}
func (f *fakePlacement) RemoveWorker(_ string)            {}
func (f *fakePlacement) Workers() []string                { return nil }
func (f *fakePlacement) Secondary(_ string, _ string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.preferred, nil
}

type fakeHealth struct {
	mu     sync.Mutex
	online []string
}

func (f *fakeHealth) RecordHeartbeat(_ string)                     {}
func (f *fakeHealth) GetWorkerStatus(_ string) models.WorkerStatus { return models.WorkerOnline }
func (f *fakeHealth) Subscribe() <-chan models.WorkerOfflineEvent  { return nil }
func (f *fakeHealth) ListOnlineWorkers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.online))
	copy(out, f.online)
	return out
}
func (f *fakeHealth) setOnline(ids ...string) {
	f.mu.Lock()
	f.online = ids
	f.mu.Unlock()
}

type replicateCall struct{ source, chunkID, target string }

type fakeWorkers struct {
	mu    sync.Mutex
	calls []replicateCall
	fail  bool
}

func (f *fakeWorkers) PutChunk(_ context.Context, _, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (f *fakeWorkers) GetChunk(_ context.Context, _, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (f *fakeWorkers) DeleteChunk(_ context.Context, _, _ string) error { return nil }
func (f *fakeWorkers) ListChunks(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}
func (f *fakeWorkers) ReplicatePeer(_ context.Context, sourceAddr, chunkID, targetAddr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return assert.AnError
	}
	f.calls = append(f.calls, replicateCall{sourceAddr, chunkID, targetAddr})
	return nil
}
func (f *fakeWorkers) getCalls() []replicateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]replicateCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestStore(t *testing.T) *metadata.MetadataStore {
	t.Helper()
	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func saveWorkers(t *testing.T, store *metadata.MetadataStore, ids ...string) {
	t.Helper()
	ctx := context.Background()
	for i, id := range ids {
		require.NoError(t, store.SaveWorker(ctx, &models.WorkerRecord{
			WorkerID: id,
			Address:  "localhost",
			Port:     9000 + i + 1,
			Status:   models.WorkerOnline,
		}))
	}
}

func saveChunk(t *testing.T, store *metadata.MetadataStore, chunkID string, replicas ...string) {
	t.Helper()
	reps := make([]models.Replica, len(replicas))
	for i, id := range replicas {
		reps[i] = models.Replica{WorkerID: id, WorkerAddr: "localhost:" + id}
	}
	require.NoError(t, store.SaveChunk(context.Background(), &models.ChunkRecord{
		ChunkID:  chunkID,
		State:    models.ChunkDurable,
		RefCount: 1,
		Replicas: reps,
	}))
}

func newRebalancer(store *metadata.MetadataStore, workers *fakeWorkers, health *fakeHealth) *Rebalancer {
	rb := New(store, workers, health, nil) // nil placement → linear-scan fallback
	rb.interval = 10 * time.Millisecond
	rb.rateLimit = 0
	return rb
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestRebalancer_HealthyChunk_NoAction(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w2", "w3")
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	assert.Empty(t, workers.getCalls(), "healthy chunk must not trigger replication")
}

func TestRebalancer_OneReplicaOffline_ReplicatesToNewWorker(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")

	// w2 goes offline
	health := &fakeHealth{}
	health.setOnline("w1", "w3")
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	calls := workers.getCalls()
	require.Len(t, calls, 1, "must trigger exactly one replication")
	assert.Equal(t, "chunk1", calls[0].chunkID)
	assert.Contains(t, calls[0].source, "localhost:w1", "source must be the online replica")
	assert.Contains(t, calls[0].target, "localhost:", "target must be a worker address")
}

func TestRebalancer_ReplicaCountStaysAtTwo(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w3")
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	// bbolt must still show exactly 2 replicas (dead replaced, not added)
	chunk, err := store.GetChunk(context.Background(), "chunk1")
	require.NoError(t, err)
	assert.Len(t, chunk.Replicas, 2, "replica count must remain exactly 2 after rebalance")
}

func TestRebalancer_DeadReplicaRemovedFromBbolt(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w3")
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	chunk, err := store.GetChunk(context.Background(), "chunk1")
	require.NoError(t, err)

	workerIDs := make([]string, len(chunk.Replicas))
	for i, r := range chunk.Replicas {
		workerIDs[i] = r.WorkerID
	}
	assert.NotContains(t, workerIDs, "w2", "dead worker must be removed from bbolt replicas")
	assert.Contains(t, workerIDs, "w1", "surviving worker must remain")
	assert.Contains(t, workerIDs, "w3", "new target must be added")
}

func TestRebalancer_BothReplicasOffline_NoAction(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")

	// Both replica holders offline — can't fix
	health := &fakeHealth{}
	health.setOnline("w3")
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	assert.Empty(t, workers.getCalls(), "can't fix when no source available")
}

func TestRebalancer_ReplicateFails_BboltUnchanged(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w3")
	workers := &fakeWorkers{fail: true}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	// bbolt must be unchanged — dead replica NOT removed if replication failed
	chunk, err := store.GetChunk(context.Background(), "chunk1")
	require.NoError(t, err)
	assert.Len(t, chunk.Replicas, 2)

	workerIDs := make([]string, len(chunk.Replicas))
	for i, r := range chunk.Replicas {
		workerIDs[i] = r.WorkerID
	}
	assert.Contains(t, workerIDs, "w2", "dead replica entry must remain if replication failed")
}

func TestRebalancer_MultipleChunks_AllFixed(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")
	saveChunk(t, store, "chunk2", "w1", "w2")
	saveChunk(t, store, "chunk3", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w3") // w2 offline
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	assert.Len(t, workers.getCalls(), 3, "all 3 chunks must be re-replicated")

	for _, id := range []string{"chunk1", "chunk2", "chunk3"} {
		chunk, err := store.GetChunk(context.Background(), id)
		require.NoError(t, err)
		assert.Len(t, chunk.Replicas, 2, "%s must have exactly 2 replicas", id)
	}
}

func TestRebalancer_InsufficientWorkers_Skips(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2")
	saveChunk(t, store, "chunk1", "w1", "w2")

	// Only 1 worker online — rebalancer can't do anything
	health := &fakeHealth{}
	health.setOnline("w1")
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	assert.Empty(t, workers.getCalls())
}

// ── Fix 2: ring-preferred secondary target ────────────────────────────────────

func TestRebalancer_PrefersRingSecondaryTarget(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3", "w4")
	// w1 online, w2 offline (dead replica), w3/w4 online
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w3", "w4")
	workers := &fakeWorkers{}

	// Placement says the ring-preferred secondary for this chunk is w4.
	pl := &fakePlacement{preferred: "w4"}
	rb := New(store, workers, health, pl)
	rb.rateLimit = 0
	rb.tick(context.Background())

	calls := workers.getCalls()
	require.Len(t, calls, 1, "must trigger exactly one replication")
	assert.Equal(t, "chunk1", calls[0].chunkID)
	// saveWorkers assigns port 9000+(index+1): w4 is index 3 → port 9004
	assert.Equal(t, "localhost:9004", calls[0].target,
		"must replicate to ring-preferred worker w4 (localhost:9004)")

	chunk, err := store.GetChunk(context.Background(), "chunk1")
	require.NoError(t, err)
	workerIDs := make([]string, len(chunk.Replicas))
	for i, r := range chunk.Replicas {
		workerIDs[i] = r.WorkerID
	}
	assert.Contains(t, workerIDs, "w4", "bbolt must record the ring-preferred worker")
}

func TestRebalancer_FallsBackToLinearScanWhenRingFails(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w3")
	workers := &fakeWorkers{}

	// Placement returns an error — rebalancer must fall back to linear scan.
	pl := &fakePlacement{err: models.ErrNoWorkers}
	rb := New(store, workers, health, pl)
	rb.rateLimit = 0
	rb.tick(context.Background())

	calls := workers.getCalls()
	require.Len(t, calls, 1, "linear-scan fallback must still replicate the chunk")
	assert.Equal(t, "chunk1", calls[0].chunkID)
	// saveWorkers assigns port 9000+(index+1): w3 is index 2 → port 9003
	assert.Equal(t, "localhost:9003", calls[0].target,
		"fallback must replicate to the only remaining online non-holder (w3)")
}

// ── Edge: zero online workers → tick returns immediately, no panic ────────────

func TestRebalancer_ZeroOnlineWorkers_NoAction(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2")
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	// No workers online at all
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	assert.NotPanics(t, func() { rb.tick(context.Background()) },
		"tick with zero online workers must not panic")
	assert.Empty(t, workers.getCalls())
}

// ── Edge: ring-preferred target already holds the chunk → linear fallback ─────

func TestRebalancer_RingPreferredAlreadyHasChunk_FallsBackToLinear(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3", "w4")
	// w2 offline; chunk already on w1 and w2
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w3", "w4")
	workers := &fakeWorkers{}

	// Ring says preferred secondary is w1 — but w1 already holds the chunk!
	// Rebalancer must fall back to linear scan and pick w3.
	pl := &fakePlacement{preferred: "w1"}
	rb := New(store, workers, health, pl)
	rb.rateLimit = 0
	rb.tick(context.Background())

	calls := workers.getCalls()
	require.Len(t, calls, 1, "must still replicate even when ring preferred is already a holder")
	assert.Equal(t, "chunk1", calls[0].chunkID)
	// Linear scan: w1 excluded (already holder), w3 is first non-holder → port 9003
	assert.Equal(t, "localhost:9003", calls[0].target,
		"must fall back to linear-scan target when ring preferred already holds chunk")
}

// ── Edge: all online workers already hold the chunk → nothing to do ───────────

func TestRebalancer_AllOnlineWorkersHoldChunk_NoAction(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	// All 3 workers online; chunk already on all 3 (unusual but possible after
	// a manual repair followed by a failed cleanup).
	saveChunk(t, store, "chunk1", "w1", "w2")

	health := &fakeHealth{}
	health.setOnline("w1", "w2") // only 2 online; both already hold the chunk
	workers := &fakeWorkers{}

	rb := newRebalancer(store, workers, health)
	rb.tick(context.Background())

	// w1 and w2 are healthy — no dead slot → nothing to do.
	assert.Empty(t, workers.getCalls(),
		"chunk with all online workers healthy must not trigger replication")
}
