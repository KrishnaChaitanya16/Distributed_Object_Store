package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// newTestStore opens a fresh bbolt database in a temp directory.
// It is closed and deleted automatically when the test ends.
func newTestStore(t *testing.T) *MetadataStore {
	t.Helper()
	store, err := NewMetadataStore(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func testObject(id string, status models.ObjectStatus) *models.ObjectManifest {
	return &models.ObjectManifest{
		ID:          id,
		Name:        "test-" + id + ".pdf",
		SizeBytes:   1024,
		ContentType: "application/pdf",
		Status:      status,
		Checksum:    "abc123",
		ChunkCount:  1,
		Version:     1,
		CreatedAt:   time.Now().UTC().Truncate(time.Second),
		ModifiedAt:  time.Now().UTC().Truncate(time.Second),
	}
}

// --- SaveObject / GetObject ---

func TestSaveAndGetObject(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	obj := testObject("obj-1", models.StatusStaged)
	require.NoError(t, store.SaveObject(ctx, obj))

	got, err := store.GetObject(ctx, "obj-1")
	require.NoError(t, err)
	assert.Equal(t, obj.ID, got.ID)
	assert.Equal(t, obj.Name, got.Name)
	assert.Equal(t, obj.Status, got.Status)
	assert.Equal(t, obj.Checksum, got.Checksum)
}

func TestGetObject_NotFound(t *testing.T) {
	store := newTestStore(t)
	_, err := store.GetObject(context.Background(), "does-not-exist")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestSaveObject_Overwrite(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	obj := testObject("obj-1", models.StatusStaged)
	require.NoError(t, store.SaveObject(ctx, obj))

	obj.Name = "updated.pdf"
	require.NoError(t, store.SaveObject(ctx, obj))

	got, err := store.GetObject(ctx, "obj-1")
	require.NoError(t, err)
	assert.Equal(t, "updated.pdf", got.Name)
}

// --- UpdateObjectStatus ---

func TestUpdateObjectStatus_StagedToCommitted(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveObject(ctx, testObject("obj-1", models.StatusStaged)))
	require.NoError(t, store.UpdateObjectStatus(ctx, "obj-1", models.StatusCommitted))

	got, err := store.GetObject(ctx, "obj-1")
	require.NoError(t, err)
	assert.Equal(t, models.StatusCommitted, got.Status)
}

func TestUpdateObjectStatus_NotFound(t *testing.T) {
	store := newTestStore(t)
	err := store.UpdateObjectStatus(context.Background(), "ghost", models.StatusCommitted)
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// --- ListObjects ---

func TestListObjects_Empty(t *testing.T) {
	store := newTestStore(t)
	objects, err := store.ListObjects(context.Background())
	require.NoError(t, err)
	assert.Empty(t, objects)
}

func TestListObjects_ReturnsAll(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveObject(ctx, testObject("obj-1", models.StatusCommitted)))
	require.NoError(t, store.SaveObject(ctx, testObject("obj-2", models.StatusCommitted)))
	require.NoError(t, store.SaveObject(ctx, testObject("obj-3", models.StatusStaged)))

	objects, err := store.ListObjects(ctx)
	require.NoError(t, err)
	assert.Len(t, objects, 3)
}

// --- ListObjectsByStatus ---

func TestListObjectsByStatus(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveObject(ctx, testObject("obj-1", models.StatusCommitted)))
	require.NoError(t, store.SaveObject(ctx, testObject("obj-2", models.StatusStaged)))
	require.NoError(t, store.SaveObject(ctx, testObject("obj-3", models.StatusStaged)))

	staged, err := store.ListObjectsByStatus(ctx, models.StatusStaged)
	require.NoError(t, err)
	assert.Len(t, staged, 2)

	committed, err := store.ListObjectsByStatus(ctx, models.StatusCommitted)
	require.NoError(t, err)
	assert.Len(t, committed, 1)
}

// --- DeleteObject ---

func TestDeleteObject(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveObject(ctx, testObject("obj-1", models.StatusCommitted)))
	require.NoError(t, store.DeleteObject(ctx, "obj-1"))

	_, err := store.GetObject(ctx, "obj-1")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestDeleteObject_NotFound(t *testing.T) {
	store := newTestStore(t)
	err := store.DeleteObject(context.Background(), "ghost")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// --- Store initialisation ---

func TestNewMetadataStore_CreatesAllBuckets(t *testing.T) {
	store := newTestStore(t)
	// If buckets weren't created, any operation would panic — this is an implicit check.
	// Explicit: verify we can write and read from each logical domain.
	ctx := context.Background()
	require.NoError(t, store.SaveObject(ctx, testObject("smoke", models.StatusStaged)))
	_, err := store.GetObject(ctx, "smoke")
	require.NoError(t, err)
}

func TestNewMetadataStore_Idempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	s1, err := NewMetadataStore(path)
	require.NoError(t, err)
	s1.Close()

	// Opening the same file again must succeed — buckets already exist.
	s2, err := NewMetadataStore(path)
	require.NoError(t, err)
	s2.Close()
}
