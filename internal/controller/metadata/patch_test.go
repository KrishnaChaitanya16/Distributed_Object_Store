package metadata

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── PatchCustomMetadata ───────────────────────────────────────────────────────

func committedObject(t *testing.T, s *MetadataStore, id string) *models.ObjectManifest {
	t.Helper()
	obj := testObject(id, models.StatusCommitted)
	require.NoError(t, s.SaveObject(context.Background(), obj))
	return obj
}

func TestPatchCustomMetadata_AddsKeys(t *testing.T) {
	s := newTestStore(t)
	committedObject(t, s, "o1")

	out, err := s.PatchCustomMetadata(context.Background(), "o1", -1, map[string]string{"env": "prod", "team": "platform"})
	require.NoError(t, err)
	assert.Equal(t, "prod", out.CustomMetadata["env"])
	assert.Equal(t, "platform", out.CustomMetadata["team"])
}

func TestPatchCustomMetadata_MergesWithExisting(t *testing.T) {
	s := newTestStore(t)
	obj := testObject("o1", models.StatusCommitted)
	obj.CustomMetadata = map[string]string{"existing": "value"}
	require.NoError(t, s.SaveObject(context.Background(), obj))

	out, err := s.PatchCustomMetadata(context.Background(), "o1", -1, map[string]string{"new": "key"})
	require.NoError(t, err)
	assert.Equal(t, "value", out.CustomMetadata["existing"], "existing key must be preserved")
	assert.Equal(t, "key", out.CustomMetadata["new"])
}

func TestPatchCustomMetadata_BumpsVersion(t *testing.T) {
	s := newTestStore(t)
	obj := committedObject(t, s, "o1")
	before := obj.Version

	out, err := s.PatchCustomMetadata(context.Background(), "o1", -1, map[string]string{"k": "v"})
	require.NoError(t, err)
	assert.Equal(t, before+1, out.Version, "version must increment on each patch")
}

func TestPatchCustomMetadata_IfMatchExact_Succeeds(t *testing.T) {
	s := newTestStore(t)
	obj := committedObject(t, s, "o1")

	_, err := s.PatchCustomMetadata(context.Background(), "o1", obj.Version, map[string]string{"k": "v"})
	require.NoError(t, err, "matching If-Match version must succeed")
}

func TestPatchCustomMetadata_IfMatchStale_ReturnsVersionConflict(t *testing.T) {
	s := newTestStore(t)
	committedObject(t, s, "o1")

	// Bump version once so the object is at version 2.
	_, err := s.PatchCustomMetadata(context.Background(), "o1", -1, map[string]string{"first": "patch"})
	require.NoError(t, err)

	// Now try to patch with the old version — must conflict.
	_, err = s.PatchCustomMetadata(context.Background(), "o1", 1, map[string]string{"stale": "attempt"})
	assert.ErrorIs(t, err, models.ErrVersionConflict)
}

func TestPatchCustomMetadata_MissingObject_ReturnsNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.PatchCustomMetadata(context.Background(), "doesnotexist", -1, map[string]string{"k": "v"})
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestPatchCustomMetadata_NonCommittedObject_ReturnsNotFound(t *testing.T) {
	s := newTestStore(t)
	for _, status := range []models.ObjectStatus{models.StatusStaged, models.StatusFailed, models.StatusDeleted} {
		obj := testObject("o-"+string(status), status)
		require.NoError(t, s.SaveObject(context.Background(), obj))
		_, err := s.PatchCustomMetadata(context.Background(), "o-"+string(status), -1, map[string]string{"k": "v"})
		assert.ErrorIs(t, err, models.ErrNotFound, "status %s must return ErrNotFound", status)
	}
}

func TestPatchCustomMetadata_ConcurrentPatches_AllSucceed(t *testing.T) {
	s := newTestStore(t)
	committedObject(t, s, "o1")

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.PatchCustomMetadata(context.Background(), "o1", -1,
				map[string]string{"goroutine": string(rune('a'+i))})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d must not error (no If-Match = no conflict)", i)
	}

	// Version must have incremented exactly n times.
	obj, err := s.GetObject(context.Background(), "o1")
	require.NoError(t, err)
	assert.Equal(t, int64(1+n), obj.Version, "version must reflect all concurrent patches")
}
