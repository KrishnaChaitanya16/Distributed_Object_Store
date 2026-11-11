package metadata

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketObjects     = []byte("objects")
	bucketChunks      = []byte("chunks")
	bucketObjChunks   = []byte("obj_chunks")
	bucketWorkers     = []byte("workers")
	bucketIdempotency = []byte("idempotency")
)

// MetadataStore is the single bbolt-backed implementation of models.MetadataRepository.
// Only this package imports bbolt — all callers go through the interface.
type MetadataStore struct {
	db *bolt.DB
}

// NewMetadataStore opens (or creates) the bbolt database at path and ensures
// all 5 buckets exist. Safe to call on an existing database — bucket creation
// is a no-op if the bucket is already there.
func NewMetadataStore(path string) (*MetadataStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bbolt at %s: %w", path, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{
			bucketObjects,
			bucketChunks,
			bucketObjChunks,
			bucketWorkers,
			bucketIdempotency,
		} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init buckets: %w", err)
	}

	return &MetadataStore{db: db}, nil
}

// Close releases the bbolt file lock. Must be called on shutdown.
func (s *MetadataStore) Close() error {
	return s.db.Close()
}
