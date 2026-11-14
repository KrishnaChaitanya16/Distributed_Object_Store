package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func (s *MetadataStore) SaveChunk(_ context.Context, chunk *models.ChunkRecord) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		if b.Get([]byte(chunk.ChunkID)) != nil {
			return models.ErrAlreadyExists
		}
		data, err := json.Marshal(chunk)
		if err != nil {
			return fmt.Errorf("marshal chunk: %w", err)
		}
		return b.Put([]byte(chunk.ChunkID), data)
	})
}

func (s *MetadataStore) GetChunk(_ context.Context, chunkID string) (*models.ChunkRecord, error) {
	var chunk models.ChunkRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketChunks).Get([]byte(chunkID))
		if data == nil {
			return models.ErrNotFound
		}
		return json.Unmarshal(data, &chunk)
	})
	if err != nil {
		return nil, err
	}
	return &chunk, nil
}

func (s *MetadataStore) UpdateChunkState(_ context.Context, chunkID string, state models.ChunkState) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		chunk, err := getChunkInTx(b, chunkID)
		if err != nil {
			return err
		}
		chunk.State = state
		return putChunkInTx(b, chunk)
	})
}

func (s *MetadataStore) AddChunkReplica(_ context.Context, chunkID string, replica models.Replica) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		chunk, err := getChunkInTx(b, chunkID)
		if err != nil {
			return err
		}
		chunk.Replicas = append(chunk.Replicas, replica)
		return putChunkInTx(b, chunk)
	})
}

func (s *MetadataStore) RemoveChunkReplica(_ context.Context, chunkID string, workerID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		chunk, err := getChunkInTx(b, chunkID)
		if err != nil {
			return err
		}
		filtered := chunk.Replicas[:0]
		for _, r := range chunk.Replicas {
			if r.WorkerID != workerID {
				filtered = append(filtered, r)
			}
		}
		chunk.Replicas = filtered
		return putChunkInTx(b, chunk)
	})
}

func (s *MetadataStore) IncrementChunkRefCount(_ context.Context, chunkID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		chunk, err := getChunkInTx(b, chunkID)
		if err != nil {
			return err
		}
		chunk.RefCount++
		return putChunkInTx(b, chunk)
	})
}

func (s *MetadataStore) DecrementChunkRefCount(_ context.Context, chunkID string) (int, error) {
	var newCount int
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		chunk, err := getChunkInTx(b, chunkID)
		if err != nil {
			return err
		}
		if chunk.RefCount > 0 {
			chunk.RefCount--
		}
		newCount = chunk.RefCount
		return putChunkInTx(b, chunk)
	})
	return newCount, err
}

func (s *MetadataStore) DeleteChunk(_ context.Context, chunkID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		if b.Get([]byte(chunkID)) == nil {
			return models.ErrNotFound
		}
		return b.Delete([]byte(chunkID))
	})
}

func (s *MetadataStore) ListChunksByState(_ context.Context, state models.ChunkState) ([]*models.ChunkRecord, error) {
	var chunks []*models.ChunkRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketChunks).ForEach(func(_, v []byte) error {
			var chunk models.ChunkRecord
			if err := json.Unmarshal(v, &chunk); err != nil {
				return fmt.Errorf("unmarshal chunk: %w", err)
			}
			if chunk.State == state {
				chunks = append(chunks, &chunk)
			}
			return nil
		})
	})
	return chunks, err
}

// ListUnderReplicatedChunks returns chunks with fewer than minReplicas replicas.
// PENDING_DELETE chunks are excluded — they are already scheduled for removal
// and must not be re-healed.
func (s *MetadataStore) ListUnderReplicatedChunks(_ context.Context, minReplicas int) ([]*models.ChunkRecord, error) {
	var chunks []*models.ChunkRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketChunks).ForEach(func(_, v []byte) error {
			var chunk models.ChunkRecord
			if err := json.Unmarshal(v, &chunk); err != nil {
				return fmt.Errorf("unmarshal chunk: %w", err)
			}
			if chunk.State == models.ChunkPendingDelete {
				return nil
			}
			if len(chunk.Replicas) < minReplicas {
				chunks = append(chunks, &chunk)
			}
			return nil
		})
	})
	return chunks, err
}

// --- helpers used only within this package ---

func getChunkInTx(b *bolt.Bucket, chunkID string) (*models.ChunkRecord, error) {
	data := b.Get([]byte(chunkID))
	if data == nil {
		return nil, models.ErrNotFound
	}
	var chunk models.ChunkRecord
	if err := json.Unmarshal(data, &chunk); err != nil {
		return nil, fmt.Errorf("unmarshal chunk: %w", err)
	}
	return &chunk, nil
}

func putChunkInTx(b *bolt.Bucket, chunk *models.ChunkRecord) error {
	data, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("marshal chunk: %w", err)
	}
	return b.Put([]byte(chunk.ChunkID), data)
}
