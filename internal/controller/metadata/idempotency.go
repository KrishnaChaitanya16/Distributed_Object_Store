package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const (
	statusProcessing = "PROCESSING"
	statusCommitted  = "COMMITTED"
)

// AcquireIdempotencyLock implements the first half of the 2PC idempotency pattern.
//
// Three outcomes:
//   - (nil, nil)        — lock just acquired, key written as PROCESSING. Caller proceeds.
//   - (record, nil)     — key already exists. Caller replays based on record.Status:
//     PROCESSING → return 409 (another request in flight)
//     COMMITTED  → return cached response
//   - (nil, err)        — bbolt failure
func (s *MetadataStore) AcquireIdempotencyLock(_ context.Context, key string) (*models.IdempotencyRecord, error) {
	var existing *models.IdempotencyRecord

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketIdempotency)
		data := b.Get([]byte(key))
		if data != nil {
			var rec models.IdempotencyRecord
			if err := json.Unmarshal(data, &rec); err != nil {
				return fmt.Errorf("unmarshal idempotency record: %w", err)
			}
			existing = &rec
			return nil
		}

		// Key not seen before — write PROCESSING sentinel atomically.
		rec := models.IdempotencyRecord{
			Key:       key,
			Status:    statusProcessing,
			ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		}
		encoded, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal idempotency record: %w", err)
		}
		return b.Put([]byte(key), encoded)
	})
	if err != nil {
		return nil, err
	}
	return existing, nil
}

// CommitIdempotencyResult implements the second half of the 2PC idempotency pattern.
// Atomically flips the key from PROCESSING to COMMITTED and stores the response.
// Must be called after the upload succeeds and the manifest is COMMITTED in bbolt.
func (s *MetadataStore) CommitIdempotencyResult(_ context.Context, key string, statusCode int, body []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketIdempotency)
		data := b.Get([]byte(key))
		if data == nil {
			return models.ErrNotFound
		}
		var rec models.IdempotencyRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return fmt.Errorf("unmarshal idempotency record: %w", err)
		}
		rec.Status = statusCommitted
		rec.StatusCode = statusCode
		rec.Body = body
		updated, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal idempotency record: %w", err)
		}
		return b.Put([]byte(key), updated)
	})
}

// PurgeExpiredIdempotencyKeys deletes all records past their ExpiresAt time.
// Called periodically by the reconciler to prevent unbounded growth.
func (s *MetadataStore) PurgeExpiredIdempotencyKeys(_ context.Context) error {
	now := time.Now().UTC()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketIdempotency)
		var expired [][]byte
		if err := b.ForEach(func(k, v []byte) error {
			var rec models.IdempotencyRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("unmarshal idempotency record: %w", err)
			}
			if rec.ExpiresAt.Before(now) {
				key := make([]byte, len(k))
				copy(key, k)
				expired = append(expired, key)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range expired {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}
