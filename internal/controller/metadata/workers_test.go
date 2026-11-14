package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func testWorker(id string, status models.WorkerStatus) *models.WorkerRecord {
	return &models.WorkerRecord{
		WorkerID:      id,
		Address:       "localhost",
		Port:          9000,
		CapacityBytes: 10 * 1024 * 1024 * 1024,
		Status:        status,
		LastSeen:      time.Now().UTC().Truncate(time.Second),
		VnodeCount:    50,
	}
}

func TestSaveAndGetWorker(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	worker := testWorker("w1", models.WorkerOnline)
	require.NoError(t, store.SaveWorker(ctx, worker))

	got, err := store.GetWorker(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, worker.WorkerID, got.WorkerID)
	assert.Equal(t, worker.Status, got.Status)
	assert.Equal(t, worker.VnodeCount, got.VnodeCount)
}

func TestGetWorker_NotFound(t *testing.T) {
	store := newTestStore(t)
	_, err := store.GetWorker(context.Background(), "ghost")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestSaveWorker_Overwrite(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveWorker(ctx, testWorker("w1", models.WorkerOnline)))

	updated := testWorker("w1", models.WorkerOffline)
	require.NoError(t, store.SaveWorker(ctx, updated))

	got, err := store.GetWorker(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, models.WorkerOffline, got.Status)
}

func TestUpdateWorkerStatus(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveWorker(ctx, testWorker("w1", models.WorkerOnline)))
	require.NoError(t, store.UpdateWorkerStatus(ctx, "w1", models.WorkerOffline))

	got, err := store.GetWorker(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, models.WorkerOffline, got.Status)
}

func TestUpdateWorkerStatus_NotFound(t *testing.T) {
	store := newTestStore(t)
	err := store.UpdateWorkerStatus(context.Background(), "ghost", models.WorkerOffline)
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestListWorkers_Empty(t *testing.T) {
	store := newTestStore(t)
	workers, err := store.ListWorkers(context.Background())
	require.NoError(t, err)
	assert.Empty(t, workers)
}

func TestListWorkers_ReturnsAll(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveWorker(ctx, testWorker("w1", models.WorkerOnline)))
	require.NoError(t, store.SaveWorker(ctx, testWorker("w2", models.WorkerOnline)))
	require.NoError(t, store.SaveWorker(ctx, testWorker("w3", models.WorkerOffline)))

	workers, err := store.ListWorkers(ctx)
	require.NoError(t, err)
	assert.Len(t, workers, 3)
}
