package metadata

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRefs(objectID string, count int) []ChunkRef {
	refs := make([]ChunkRef, count)
	for i := 0; i < count; i++ {
		refs[i] = ChunkRef{
			ChunkID:     fmt.Sprintf("chunk-%s-%04d", objectID, i),
			Index:       i,
			Offset:      int64(i) * 4 * 1024 * 1024,
			Size:        4 * 1024 * 1024,
			PrimaryID:   "w1",
			SecondaryID: "w2",
		}
	}
	return refs
}

func TestSaveAndGetChunkRefs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	refs := makeRefs("obj-1", 5)
	require.NoError(t, store.SaveChunkRefs(ctx, "obj-1", refs))

	got, err := store.GetChunkRefs(ctx, "obj-1")
	require.NoError(t, err)
	assert.Len(t, got, 5)
	for i, ref := range got {
		assert.Equal(t, refs[i].ChunkID, ref.ChunkID)
		assert.Equal(t, refs[i].Index, ref.Index)
		assert.Equal(t, refs[i].Offset, ref.Offset)
	}
}

func TestGetChunkRefs_ReturnsInOrder(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Save refs out of order — cursor must return them in index order
	refs := []ChunkRef{
		{ChunkID: "c3", Index: 2, PrimaryID: "w1", SecondaryID: "w2"},
		{ChunkID: "c1", Index: 0, PrimaryID: "w1", SecondaryID: "w2"},
		{ChunkID: "c2", Index: 1, PrimaryID: "w1", SecondaryID: "w2"},
	}
	require.NoError(t, store.SaveChunkRefs(ctx, "obj-1", refs))

	got, err := store.GetChunkRefs(ctx, "obj-1")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "c1", got[0].ChunkID)
	assert.Equal(t, "c2", got[1].ChunkID)
	assert.Equal(t, "c3", got[2].ChunkID)
}

func TestGetChunkRefs_Empty(t *testing.T) {
	store := newTestStore(t)
	refs, err := store.GetChunkRefs(context.Background(), "obj-nonexistent")
	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestGetChunkRefs_IsolatedByObjectID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.SaveChunkRefs(ctx, "obj-1", makeRefs("obj-1", 3)))
	require.NoError(t, store.SaveChunkRefs(ctx, "obj-2", makeRefs("obj-2", 5)))

	got1, err := store.GetChunkRefs(ctx, "obj-1")
	require.NoError(t, err)
	assert.Len(t, got1, 3)

	got2, err := store.GetChunkRefs(ctx, "obj-2")
	require.NoError(t, err)
	assert.Len(t, got2, 5)
}

func TestGetChunkRefs_LargeFile(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// 2560 chunks = ~10GB file at 4MB chunks
	refs := makeRefs("obj-large", 2560)
	require.NoError(t, store.SaveChunkRefs(ctx, "obj-large", refs))

	got, err := store.GetChunkRefs(ctx, "obj-large")
	require.NoError(t, err)
	assert.Len(t, got, 2560)
	assert.Equal(t, 0, got[0].Index)
	assert.Equal(t, 2559, got[2559].Index)
}
