package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func (s *MetadataStore) SaveObject(_ context.Context, obj *models.ObjectManifest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(obj)
		if err != nil {
			return fmt.Errorf("marshal object: %w", err)
		}
		return tx.Bucket(bucketObjects).Put([]byte(obj.ID), data)
	})
}

func (s *MetadataStore) GetObject(_ context.Context, objectID string) (*models.ObjectManifest, error) {
	var obj models.ObjectManifest
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketObjects).Get([]byte(objectID))
		if data == nil {
			return models.ErrNotFound
		}
		return json.Unmarshal(data, &obj)
	})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (s *MetadataStore) UpdateObjectStatus(_ context.Context, objectID string, status models.ObjectStatus) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		data := b.Get([]byte(objectID))
		if data == nil {
			return models.ErrNotFound
		}
		var obj models.ObjectManifest
		if err := json.Unmarshal(data, &obj); err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}
		obj.Status = status
		updated, err := json.Marshal(&obj)
		if err != nil {
			return fmt.Errorf("marshal object: %w", err)
		}
		return b.Put([]byte(objectID), updated)
	})
}

func (s *MetadataStore) ListObjects(_ context.Context) ([]*models.ObjectManifest, error) {
	var objects []*models.ObjectManifest
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketObjects).ForEach(func(_, v []byte) error {
			var obj models.ObjectManifest
			if err := json.Unmarshal(v, &obj); err != nil {
				return fmt.Errorf("unmarshal object: %w", err)
			}
			objects = append(objects, &obj)
			return nil
		})
	})
	return objects, err
}

func (s *MetadataStore) ListObjectsByStatus(_ context.Context, status models.ObjectStatus) ([]*models.ObjectManifest, error) {
	var objects []*models.ObjectManifest
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketObjects).ForEach(func(_, v []byte) error {
			var obj models.ObjectManifest
			if err := json.Unmarshal(v, &obj); err != nil {
				return fmt.Errorf("unmarshal object: %w", err)
			}
			if obj.Status == status {
				objects = append(objects, &obj)
			}
			return nil
		})
	})
	return objects, err
}

// PatchCustomMetadata atomically reads, version-checks, merges, and bumps the manifest
// inside a single bbolt Update transaction — preventing concurrent PATCH races.
func (s *MetadataStore) PatchCustomMetadata(_ context.Context, id string, ifMatch int64, patch map[string]string) (*models.ObjectManifest, error) {
	var out models.ObjectManifest
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		data := b.Get([]byte(id))
		if data == nil {
			return models.ErrNotFound
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}
		if out.Status != models.StatusCommitted {
			return models.ErrNotFound
		}
		if ifMatch >= 0 && out.Version != ifMatch {
			return models.ErrVersionConflict
		}
		if out.CustomMetadata == nil {
			out.CustomMetadata = map[string]string{}
		}
		for k, v := range patch {
			out.CustomMetadata[k] = v
		}
		out.Version++
		out.ModifiedAt = time.Now().UTC()
		updated, err := json.Marshal(&out)
		if err != nil {
			return fmt.Errorf("marshal object: %w", err)
		}
		return b.Put([]byte(id), updated)
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *MetadataStore) DeleteObject(_ context.Context, objectID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketObjects).Get([]byte(objectID)) == nil {
			return models.ErrNotFound
		}
		return tx.Bucket(bucketObjects).Delete([]byte(objectID))
	})
}
