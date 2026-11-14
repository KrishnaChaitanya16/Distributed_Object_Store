package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// Composite key format: "<objectID>/<10-digit-zero-padded-index>"
// Zero-padding ensures lexicographic order == chunk order.
// Example: "obj-abc/0000000000", "obj-abc/0000000001", ...
func chunkRefKey(objectID string, index int) []byte {
	return []byte(fmt.Sprintf("%s/%010d", objectID, index))
}

// chunkRefPrefix returns the key prefix for all chunks of an object.
// Used to seek to the first chunk and iterate forward.
func chunkRefPrefix(objectID string) []byte {
	return []byte(objectID + "/")
}

func (s *MetadataStore) SaveChunkRefs(_ context.Context, objectID string, refs []ChunkRef) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketObjChunks)
		for _, ref := range refs {
			data, err := json.Marshal(ref)
			if err != nil {
				return fmt.Errorf("marshal chunk ref: %w", err)
			}
			if err := b.Put(chunkRefKey(objectID, ref.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetChunkRefs uses a bbolt cursor to stream chunk refs in index order.
// Only the refs for this objectID are loaded — O(1) memory relative to
// the total number of objects in the bucket.
func (s *MetadataStore) GetChunkRefs(_ context.Context, objectID string) ([]ChunkRef, error) {
	var refs []ChunkRef
	prefix := chunkRefPrefix(objectID)

	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketObjChunks).Cursor()
		for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
			var ref ChunkRef
			if err := json.Unmarshal(v, &ref); err != nil {
				return fmt.Errorf("unmarshal chunk ref: %w", err)
			}
			refs = append(refs, ref)
		}
		return nil
	})
	return refs, err
}

func hasPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i, b := range prefix {
		if key[i] != b {
			return false
		}
	}
	return true
}

// type alias so test file can reference without importing models directly
type ChunkRef = models.ChunkRef
