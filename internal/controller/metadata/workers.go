package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func (s *MetadataStore) SaveWorker(_ context.Context, worker *models.WorkerRecord) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(worker)
		if err != nil {
			return fmt.Errorf("marshal worker: %w", err)
		}
		return tx.Bucket(bucketWorkers).Put([]byte(worker.WorkerID), data)
	})
}

func (s *MetadataStore) GetWorker(_ context.Context, workerID string) (*models.WorkerRecord, error) {
	var worker models.WorkerRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketWorkers).Get([]byte(workerID))
		if data == nil {
			return models.ErrNotFound
		}
		return json.Unmarshal(data, &worker)
	})
	if err != nil {
		return nil, err
	}
	return &worker, nil
}

func (s *MetadataStore) UpdateWorkerStatus(_ context.Context, workerID string, status models.WorkerStatus) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketWorkers)
		data := b.Get([]byte(workerID))
		if data == nil {
			return models.ErrNotFound
		}
		var worker models.WorkerRecord
		if err := json.Unmarshal(data, &worker); err != nil {
			return fmt.Errorf("unmarshal worker: %w", err)
		}
		worker.Status = status
		updated, err := json.Marshal(&worker)
		if err != nil {
			return fmt.Errorf("marshal worker: %w", err)
		}
		return b.Put([]byte(workerID), updated)
	})
}

func (s *MetadataStore) ListWorkers(_ context.Context) ([]*models.WorkerRecord, error) {
	var workers []*models.WorkerRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkers).ForEach(func(_, v []byte) error {
			var worker models.WorkerRecord
			if err := json.Unmarshal(v, &worker); err != nil {
				return fmt.Errorf("unmarshal worker: %w", err)
			}
			workers = append(workers, &worker)
			return nil
		})
	})
	return workers, err
}
