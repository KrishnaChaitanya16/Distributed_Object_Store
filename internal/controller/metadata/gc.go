package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// Every method here is one bbolt transaction, so liveness checks and the writes they guard can never interleave with an upload.

// AddChunkRef records that a STAGED object uses a chunk before the upload relies on it, and returns the chunk's record (nil if absent).
func (s *MetadataStore) AddChunkRef(_ context.Context, objectID string, ref models.ChunkRef) (*models.ChunkRecord, error) {
	var rec *models.ChunkRecord
	err := s.db.Update(func(tx *bolt.Tx) error {
		obj, err := getObjectInTx(tx.Bucket(bucketObjects), objectID)
		if err != nil || obj.Status != models.StatusStaged {
			return models.ErrNotStaged
		}
		data, err := json.Marshal(ref)
		if err != nil {
			return fmt.Errorf("marshal chunk ref: %w", err)
		}
		if err := tx.Bucket(bucketObjChunks).Put(chunkRefKey(objectID, ref.Index), data); err != nil {
			return err
		}
		obj.ModifiedAt = time.Now().UTC()
		if err := putObjectInTx(tx.Bucket(bucketObjects), obj); err != nil {
			return err
		}
		rec, err = getChunkInTx(tx.Bucket(bucketChunks), ref.ChunkID)
		if err == models.ErrNotFound {
			rec, err = nil, nil
		}
		return err
	})
	return rec, err
}

// PrepareChunk creates a PENDING record if none exists and returns the effective record; replace swaps the replicas of an existing PENDING record.
func (s *MetadataStore) PrepareChunk(_ context.Context, chunk *models.ChunkRecord, replace bool) (*models.ChunkRecord, error) {
	var out *models.ChunkRecord
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		existing, err := getChunkInTx(b, chunk.ChunkID)
		switch {
		case err == models.ErrNotFound:
			fresh := *chunk
			fresh.State = models.ChunkPending
			out = &fresh
			return putChunkInTx(b, out)
		case err != nil:
			return err
		case existing.State == models.ChunkPendingDelete:
			return models.ErrChunkBusy
		case existing.State == models.ChunkPending && replace:
			existing.Replicas = chunk.Replicas
			existing.SizeBytes = chunk.SizeBytes
			out = existing
			return putChunkInTx(b, out)
		}
		out = existing
		return nil
	})
	return out, err
}

// MarkChunkDurable flips PENDING to DURABLE once every replica has the bytes.
func (s *MetadataStore) MarkChunkDurable(_ context.Context, chunkID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		chunk, err := getChunkInTx(b, chunkID)
		if err != nil {
			return err
		}
		switch chunk.State {
		case models.ChunkDurable:
			return nil
		case models.ChunkPendingDelete:
			return models.ErrChunkBusy
		}
		chunk.State = models.ChunkDurable
		return putChunkInTx(b, chunk)
	})
}

// CommitObject flips a STAGED manifest to COMMITTED; it fails if GC already expired the upload.
func (s *MetadataStore) CommitObject(_ context.Context, obj *models.ObjectManifest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		cur, err := getObjectInTx(b, obj.ID)
		if err != nil || cur.Status != models.StatusStaged {
			return models.ErrNotStaged
		}
		committed := *obj
		committed.Status = models.StatusCommitted
		return putObjectInTx(b, &committed)
	})
}

// MarkObjectDeleted flips COMMITTED to DELETED; any other state reads as not found, so two concurrent deletes cannot both succeed.
func (s *MetadataStore) MarkObjectDeleted(_ context.Context, objectID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		obj, err := getObjectInTx(b, objectID)
		if err != nil || obj.Status != models.StatusCommitted {
			return models.ErrNotFound
		}
		obj.Status = models.StatusDeleted
		obj.ModifiedAt = time.Now().UTC()
		return putObjectInTx(b, obj)
	})
}

// FailStaleUploads marks STAGED uploads idle since before idleBefore as FAILED and returns their ids.
func (s *MetadataStore) FailStaleUploads(_ context.Context, idleBefore time.Time) ([]string, error) {
	var ids []string
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		var stale []*models.ObjectManifest
		err := b.ForEach(func(_, v []byte) error {
			var obj models.ObjectManifest
			if err := json.Unmarshal(v, &obj); err != nil {
				return fmt.Errorf("unmarshal object: %w", err)
			}
			if obj.Status == models.StatusStaged && obj.ModifiedAt.Before(idleBefore) {
				stale = append(stale, &obj)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, obj := range stale {
			obj.Status = models.StatusFailed
			if err := putObjectInTx(b, obj); err != nil {
				return err
			}
			ids = append(ids, obj.ID)
		}
		return nil
	})
	return ids, err
}

// ListAllChunks returns every chunk record regardless of state.
func (s *MetadataStore) ListAllChunks(_ context.Context) ([]*models.ChunkRecord, error) {
	var chunks []*models.ChunkRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketChunks).ForEach(func(_, v []byte) error {
			var chunk models.ChunkRecord
			if err := json.Unmarshal(v, &chunk); err != nil {
				return fmt.Errorf("unmarshal chunk: %w", err)
			}
			chunks = append(chunks, &chunk)
			return nil
		})
	})
	return chunks, err
}

// LiveChunkRefs counts, per chunk, the references held by COMMITTED and STAGED objects.
func (s *MetadataStore) LiveChunkRefs(_ context.Context) (map[string]int, error) {
	var live map[string]int
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		live, err = liveRefsInTx(tx)
		return err
	})
	return live, err
}

// ClaimChunksForDeletion moves every unreferenced candidate to PENDING_DELETE and returns the claimed records; holders seed records for files bbolt does not know.
func (s *MetadataStore) ClaimChunksForDeletion(_ context.Context, chunkIDs []string, holders map[string][]models.Replica) ([]*models.ChunkRecord, error) {
	var claimed []*models.ChunkRecord
	err := s.db.Update(func(tx *bolt.Tx) error {
		live, err := liveRefsInTx(tx)
		if err != nil {
			return err
		}
		b := tx.Bucket(bucketChunks)
		seen := make(map[string]bool, len(chunkIDs))
		for _, id := range chunkIDs {
			if seen[id] || live[id] > 0 {
				continue
			}
			seen[id] = true
			chunk, err := getChunkInTx(b, id)
			switch {
			case err == models.ErrNotFound:
				if len(holders[id]) == 0 {
					continue
				}
				chunk = &models.ChunkRecord{ChunkID: id, CreatedAt: time.Now().UTC()}
			case err != nil:
				return err
			}
			chunk.State = models.ChunkPendingDelete
			chunk.RefCount = 0
			chunk.Replicas = mergeReplicas(chunk.Replicas, holders[id])
			if err := putChunkInTx(b, chunk); err != nil {
				return err
			}
			claimed = append(claimed, chunk)
		}
		return nil
	})
	return claimed, err
}

// RemoveDeletedChunk drops a record only while it is still PENDING_DELETE; a missing record is not an error.
func (s *MetadataStore) RemoveDeletedChunk(_ context.Context, chunkID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunks)
		chunk, err := getChunkInTx(b, chunkID)
		if err == models.ErrNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		if chunk.State != models.ChunkPendingDelete {
			return models.ErrChunkBusy
		}
		return b.Delete([]byte(chunkID))
	})
}

// AdoptChunks recreates DURABLE records for referenced chunks whose record is missing but whose bytes were found on workers.
func (s *MetadataStore) AdoptChunks(_ context.Context, holders map[string][]models.Replica) ([]string, error) {
	var adopted []string
	err := s.db.Update(func(tx *bolt.Tx) error {
		live, err := liveRefsInTx(tx)
		if err != nil {
			return err
		}
		b := tx.Bucket(bucketChunks)
		for _, id := range sortedKeys(holders) {
			if live[id] == 0 || len(holders[id]) == 0 || b.Get([]byte(id)) != nil {
				continue
			}
			chunk := &models.ChunkRecord{
				ChunkID:   id,
				RefCount:  live[id],
				State:     models.ChunkDurable,
				Replicas:  mergeReplicas(nil, holders[id]),
				CreatedAt: time.Now().UTC(),
			}
			if err := putChunkInTx(b, chunk); err != nil {
				return err
			}
			adopted = append(adopted, id)
		}
		return nil
	})
	return adopted, err
}

// PurgeDeadObjects removes DELETED and FAILED manifests with their chunk refs and returns how many went.
func (s *MetadataStore) PurgeDeadObjects(_ context.Context) (int, error) {
	n := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		objs := tx.Bucket(bucketObjects)
		var dead [][]byte
		err := objs.ForEach(func(k, v []byte) error {
			var obj models.ObjectManifest
			if err := json.Unmarshal(v, &obj); err != nil {
				return fmt.Errorf("unmarshal object: %w", err)
			}
			if obj.Status == models.StatusDeleted || obj.Status == models.StatusFailed {
				dead = append(dead, append([]byte(nil), k...))
			}
			return nil
		})
		if err != nil {
			return err
		}
		refs := tx.Bucket(bucketObjChunks)
		for _, id := range dead {
			prefix := chunkRefPrefix(string(id))
			var keys [][]byte
			c := refs.Cursor()
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				keys = append(keys, append([]byte(nil), k...))
			}
			for _, k := range keys {
				if err := refs.Delete(k); err != nil {
					return err
				}
			}
			if err := objs.Delete(id); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// SyncRefCounts rewrites each chunk's ref_count to the number of live references, so the field never drifts.
func (s *MetadataStore) SyncRefCounts(_ context.Context) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		live, err := liveRefsInTx(tx)
		if err != nil {
			return err
		}
		b := tx.Bucket(bucketChunks)
		var changed []*models.ChunkRecord
		err = b.ForEach(func(_, v []byte) error {
			var chunk models.ChunkRecord
			if err := json.Unmarshal(v, &chunk); err != nil {
				return fmt.Errorf("unmarshal chunk: %w", err)
			}
			if chunk.State != models.ChunkPendingDelete && chunk.RefCount != live[chunk.ChunkID] {
				chunk.RefCount = live[chunk.ChunkID]
				changed = append(changed, &chunk)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, chunk := range changed {
			if err := putChunkInTx(b, chunk); err != nil {
				return err
			}
		}
		return nil
	})
}

// liveRefsInTx counts chunk references from COMMITTED and STAGED objects within one consistent snapshot.
func liveRefsInTx(tx *bolt.Tx) (map[string]int, error) {
	liveObjs := make(map[string]bool)
	err := tx.Bucket(bucketObjects).ForEach(func(k, v []byte) error {
		var obj models.ObjectManifest
		if err := json.Unmarshal(v, &obj); err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}
		if obj.Status == models.StatusCommitted || obj.Status == models.StatusStaged {
			liveObjs[string(k)] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	live := make(map[string]int)
	err = tx.Bucket(bucketObjChunks).ForEach(func(k, v []byte) error {
		i := bytes.LastIndexByte(k, '/')
		if i < 0 || !liveObjs[string(k[:i])] {
			return nil
		}
		var ref models.ChunkRef
		if err := json.Unmarshal(v, &ref); err != nil {
			return fmt.Errorf("unmarshal chunk ref: %w", err)
		}
		live[ref.ChunkID]++
		return nil
	})
	return live, err
}

// mergeReplicas appends holders not already listed, keeping the existing order.
func mergeReplicas(base, extra []models.Replica) []models.Replica {
	out := append([]models.Replica(nil), base...)
	for _, h := range extra {
		dup := false
		for _, r := range out {
			if r.WorkerID == h.WorkerID {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, h)
		}
	}
	return out
}

func sortedKeys(m map[string][]models.Replica) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func getObjectInTx(b *bolt.Bucket, objectID string) (*models.ObjectManifest, error) {
	data := b.Get([]byte(objectID))
	if data == nil {
		return nil, models.ErrNotFound
	}
	var obj models.ObjectManifest
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal object: %w", err)
	}
	return &obj, nil
}

func putObjectInTx(b *bolt.Bucket, obj *models.ObjectManifest) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal object: %w", err)
	}
	return b.Put([]byte(obj.ID), data)
}
