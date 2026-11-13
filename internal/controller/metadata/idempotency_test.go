package metadata

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// insertExpiredRecord writes a record with ExpiresAt in the past directly into bbolt.
// Used to test purge without waiting for real expiry.
func insertExpiredRecord(t *testing.T, store *MetadataStore, key string) {
	t.Helper()
	rec := models.IdempotencyRecord{
		Key:        key,
		Status:     statusCommitted,
		StatusCode: 201,
		ExpiresAt:  time.Now().UTC().Add(-1 * time.Hour),
	}
	err := store.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketIdempotency).Put([]byte(key), data)
	})
	require.NoError(t, err)
}

func TestAcquireIdempotencyLock_NewKey(t *testing.T) {
	store := newTestStore(t)

	existing, err := store.AcquireIdempotencyLock(context.Background(), "key-1")
	require.NoError(t, err)
	assert.Nil(t, existing, "nil means lock just acquired — caller should proceed")
}

func TestAcquireIdempotencyLock_KeyAlreadyProcessing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.AcquireIdempotencyLock(ctx, "key-1")
	require.NoError(t, err)

	existing, err := store.AcquireIdempotencyLock(ctx, "key-1")
	require.NoError(t, err)
	require.NotNil(t, existing)
	assert.Equal(t, statusProcessing, existing.Status)
}

func TestAcquireIdempotencyLock_KeyAlreadyCommitted(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.AcquireIdempotencyLock(ctx, "key-1")
	require.NoError(t, err)
	require.NoError(t, store.CommitIdempotencyResult(ctx, "key-1", 201, []byte(`{"id":"obj-1"}`)))

	existing, err := store.AcquireIdempotencyLock(ctx, "key-1")
	require.NoError(t, err)
	require.NotNil(t, existing)
	assert.Equal(t, statusCommitted, existing.Status)
	assert.Equal(t, 201, existing.StatusCode)
	assert.Equal(t, []byte(`{"id":"obj-1"}`), existing.Body)
}

func TestCommitIdempotencyResult(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.AcquireIdempotencyLock(ctx, "key-1")
	require.NoError(t, err)

	require.NoError(t, store.CommitIdempotencyResult(ctx, "key-1", 201, []byte(`{"id":"obj-1"}`)))

	existing, err := store.AcquireIdempotencyLock(ctx, "key-1")
	require.NoError(t, err)
	require.NotNil(t, existing)
	assert.Equal(t, statusCommitted, existing.Status)
	assert.Equal(t, 201, existing.StatusCode)
}

func TestCommitIdempotencyResult_NotFound(t *testing.T) {
	store := newTestStore(t)
	err := store.CommitIdempotencyResult(context.Background(), "ghost", 201, nil)
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestPurgeExpiredIdempotencyKeys(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Insert one expired record directly
	insertExpiredRecord(t, store, "key-expired")

	// Acquire a fresh key (not expired)
	_, err := store.AcquireIdempotencyLock(ctx, "key-fresh")
	require.NoError(t, err)

	require.NoError(t, store.PurgeExpiredIdempotencyKeys(ctx))

	// Expired key is gone — AcquireIdempotencyLock returns nil (lock re-acquired)
	existing, err := store.AcquireIdempotencyLock(ctx, "key-expired")
	require.NoError(t, err)
	assert.Nil(t, existing, "expired key should have been purged")

	// Fresh key still exists
	existing, err = store.AcquireIdempotencyLock(ctx, "key-fresh")
	require.NoError(t, err)
	assert.NotNil(t, existing, "fresh key should still exist")
}

func TestPurgeExpiredIdempotencyKeys_NothingToDelete(t *testing.T) {
	store := newTestStore(t)
	// Should not error on empty bucket
	require.NoError(t, store.PurgeExpiredIdempotencyKeys(context.Background()))
}
