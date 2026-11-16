package rebalancer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/chunklock"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// tickWhileLocked starts a tick while the test holds the chunk lock, applies change, then releases the lock and waits for the tick.
func tickWhileLocked(t *testing.T, rb *Rebalancer, chunkID string, change func()) {
	t.Helper()
	unlock := chunklock.Lock(chunkID)
	done := make(chan struct{})
	go func() {
		rb.tick(context.Background())
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	change()
	unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tick did not finish")
	}
}

func TestRebalancer_SkipsChunkClaimedForDeletionWhileWaitingForLock(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")
	health := &fakeHealth{}
	health.setOnline("w1", "w3")
	workers := &fakeWorkers{}
	rb := newRebalancer(store, workers, health)

	tickWhileLocked(t, rb, "chunk1", func() {
		require.NoError(t, store.UpdateChunkState(context.Background(), "chunk1", models.ChunkPendingDelete))
	})
	assert.Empty(t, workers.getCalls(), "a chunk being garbage collected must not be copied")
	got, _ := store.GetChunk(context.Background(), "chunk1")
	assert.Len(t, got.Replicas, 2)
}

func TestRebalancer_SkipsChunkAlreadyRepairedWhileWaitingForLock(t *testing.T) {
	store := newTestStore(t)
	saveWorkers(t, store, "w1", "w2", "w3")
	saveChunk(t, store, "chunk1", "w1", "w2")
	health := &fakeHealth{}
	health.setOnline("w1", "w3")
	workers := &fakeWorkers{}
	rb := newRebalancer(store, workers, health)

	tickWhileLocked(t, rb, "chunk1", func() {
		ctx := context.Background()
		require.NoError(t, store.RemoveChunkReplica(ctx, "chunk1", "w2"))
		require.NoError(t, store.AddChunkReplica(ctx, "chunk1", models.Replica{WorkerID: "w3", WorkerAddr: "localhost:9003"}))
	})
	assert.Empty(t, workers.getCalls(), "someone else already replaced the dead replica")
	got, _ := store.GetChunk(context.Background(), "chunk1")
	assert.Len(t, got.Replicas, 2, "no third replica")
}
