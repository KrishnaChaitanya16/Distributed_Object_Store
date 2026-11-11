package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func testChunk(id string) *models.ChunkRecord {
	return &models.ChunkRecord{
		ChunkID:   id,
		SizeBytes: 512,
		RefCount:  1,
		State:     models.ChunkPending,
		Replicas:  []models.Replica{},
		CreatedAt: time.Now().UTC().Truncate(time.Second),
	}
}

func testReplica(workerID, addr string) models.Replica {
	return models.Replica{WorkerID: workerID, WorkerAddr: addr}
}

// --- SaveChunk / GetChunk ---

func TestSaveAndGetChunk(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	chunk := testChunk("chunk-1")
	require.NoError(t, store.SaveChunk(ctx, chunk))

	got, err := store.GetChunk(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Equal(t, chunk.ChunkID, got.ChunkID)
	assert.Equal(t, chunk.SizeBytes, got.SizeBytes)
	assert.Equal(t, chunk.State, got.State)
}

func TestSaveChunk_AlreadyExists(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))
	err := store.SaveChunk(ctx, testChunk("chunk-1"))
	assert.ErrorIs(t, err, models.ErrAlreadyExists)
}

func TestGetChunk_NotFound(t *testing.T) {
	store := newTestStore(t)
	_, err := store.GetChunk(context.Background(), "ghost")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// --- UpdateChunkState ---

func TestUpdateChunkState_PendingToDurable(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))
	require.NoError(t, store.UpdateChunkState(ctx, "chunk-1", models.ChunkDurable))

	got, err := store.GetChunk(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Equal(t, models.ChunkDurable, got.State)
}

func TestUpdateChunkState_NotFound(t *testing.T) {
	store := newTestStore(t)
	err := store.UpdateChunkState(context.Background(), "ghost", models.ChunkDurable)
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// --- AddChunkReplica / RemoveChunkReplica ---

func TestAddChunkReplica(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))
	require.NoError(t, store.AddChunkReplica(ctx, "chunk-1", testReplica("w1", "localhost:9001")))
	require.NoError(t, store.AddChunkReplica(ctx, "chunk-1", testReplica("w2", "localhost:9002")))

	got, err := store.GetChunk(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Len(t, got.Replicas, 2)
	assert.Equal(t, "w1", got.Replicas[0].WorkerID)
	assert.Equal(t, "w2", got.Replicas[1].WorkerID)
}

func TestRemoveChunkReplica(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))
	require.NoError(t, store.AddChunkReplica(ctx, "chunk-1", testReplica("w1", "localhost:9001")))
	require.NoError(t, store.AddChunkReplica(ctx, "chunk-1", testReplica("w2", "localhost:9002")))

	require.NoError(t, store.RemoveChunkReplica(ctx, "chunk-1", "w1"))

	got, err := store.GetChunk(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Len(t, got.Replicas, 1)
	assert.Equal(t, "w2", got.Replicas[0].WorkerID)
}

func TestRemoveChunkReplica_NonExistentWorker(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))
	require.NoError(t, store.AddChunkReplica(ctx, "chunk-1", testReplica("w1", "localhost:9001")))

	// Removing a worker that isn't in the list is a no-op, not an error.
	require.NoError(t, store.RemoveChunkReplica(ctx, "chunk-1", "ghost-worker"))

	got, err := store.GetChunk(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Len(t, got.Replicas, 1)
}

// --- IncrementChunkRefCount / DecrementChunkRefCount ---

func TestIncrementChunkRefCount(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))
	require.NoError(t, store.IncrementChunkRefCount(ctx, "chunk-1"))

	got, err := store.GetChunk(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Equal(t, 2, got.RefCount)
}

func TestDecrementChunkRefCount_ReturnsNewCount(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))

	count, err := store.DecrementChunkRefCount(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestDecrementChunkRefCount_DoesNotGoBelowZero(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	chunk := testChunk("chunk-1")
	chunk.RefCount = 0
	require.NoError(t, store.SaveChunk(ctx, chunk))

	count, err := store.DecrementChunkRefCount(ctx, "chunk-1")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// --- DeleteChunk ---

func TestDeleteChunk(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunk(ctx, testChunk("chunk-1")))
	require.NoError(t, store.DeleteChunk(ctx, "chunk-1"))

	_, err := store.GetChunk(ctx, "chunk-1")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestDeleteChunk_NotFound(t *testing.T) {
	store := newTestStore(t)
	err := store.DeleteChunk(context.Background(), "ghost")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// --- ListChunksByState ---

func TestListChunksByState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	c1 := testChunk("chunk-1")
	c1.State = models.ChunkPending
	c2 := testChunk("chunk-2")
	c2.State = models.ChunkDurable
	c3 := testChunk("chunk-3")
	c3.State = models.ChunkDurable

	require.NoError(t, store.SaveChunk(ctx, c1))
	require.NoError(t, store.SaveChunk(ctx, c2))
	require.NoError(t, store.SaveChunk(ctx, c3))

	durable, err := store.ListChunksByState(ctx, models.ChunkDurable)
	require.NoError(t, err)
	assert.Len(t, durable, 2)

	pending, err := store.ListChunksByState(ctx, models.ChunkPending)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
}

// --- ListUnderReplicatedChunks ---

func TestListUnderReplicatedChunks(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// chunk-1: 2 replicas — healthy
	c1 := testChunk("chunk-1")
	c1.State = models.ChunkDurable
	c1.Replicas = []models.Replica{testReplica("w1", ""), testReplica("w2", "")}

	// chunk-2: 1 replica — under-replicated
	c2 := testChunk("chunk-2")
	c2.State = models.ChunkDurable
	c2.Replicas = []models.Replica{testReplica("w1", "")}

	// chunk-3: 1 replica but PENDING_DELETE — must NOT appear in results
	c3 := testChunk("chunk-3")
	c3.State = models.ChunkPendingDelete
	c3.Replicas = []models.Replica{testReplica("w1", "")}

	require.NoError(t, store.SaveChunk(ctx, c1))
	require.NoError(t, store.SaveChunk(ctx, c2))
	require.NoError(t, store.SaveChunk(ctx, c3))

	degraded, err := store.ListUnderReplicatedChunks(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, degraded, 1)
	assert.Equal(t, "chunk-2", degraded[0].ChunkID)
}

func TestListUnderReplicatedChunks_Empty(t *testing.T) {
	store := newTestStore(t)
	degraded, err := store.ListUnderReplicatedChunks(context.Background(), 2)
	require.NoError(t, err)
	assert.Empty(t, degraded)
}
