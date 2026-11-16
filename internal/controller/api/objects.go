package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	objects, err := s.meta.ListObjectsByStatus(r.Context(), models.StatusCommitted)
	if err != nil {
		slog.Error("list objects", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(objects) //nolint:errcheck
}

// GET /api/v1/objects/{id}/metadata — returns manifest with ETag header.
func (s *Server) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	objectID := r.PathValue("id")
	manifest, err := s.meta.GetObject(r.Context(), objectID)
	if err != nil {
		if errors.Is(err, models.ErrNotFound) {
			http.Error(w, `{"error":"object not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	if manifest.Status != models.StatusCommitted {
		http.Error(w, `{"error":"object not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", etagFor(manifest.Version))
	json.NewEncoder(w).Encode(manifest) //nolint:errcheck
}

// PATCH /api/v1/objects/{id}/metadata — updates custom metadata key-value pairs.
// Supports optimistic concurrency: If-Match header must match current ETag.
// Returns 409 on version conflict; new ETag on success.
func (s *Server) handlePatchMetadata(w http.ResponseWriter, r *http.Request) {
	objectID := r.PathValue("id")
	log := slog.With("handler", "patch-metadata", "object_id", objectID)

	manifest, err := s.meta.GetObject(r.Context(), objectID)
	if err != nil {
		if errors.Is(err, models.ErrNotFound) {
			http.Error(w, `{"error":"object not found"}`, http.StatusNotFound)
			return
		}
		log.Error("get object", "err", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	if manifest.Status != models.StatusCommitted {
		http.Error(w, `{"error":"object not found"}`, http.StatusNotFound)
		return
	}

	// Optimistic concurrency: reject if client's version is stale.
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if ifMatch != etagFor(manifest.Version) {
			log.Warn("version conflict on patch", "object_id", objectID,
				"current_version", manifest.Version, "client_etag", ifMatch)
			http.Error(w, `{"error":"version conflict — re-read and retry"}`, http.StatusConflict)
			return
		}
	}

	var patch map[string]string
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	if manifest.CustomMetadata == nil {
		manifest.CustomMetadata = make(map[string]string)
	}
	for k, v := range patch {
		manifest.CustomMetadata[k] = v
	}
	manifest.Version++
	manifest.ModifiedAt = time.Now().UTC()

	if err := s.meta.SaveObject(r.Context(), manifest); err != nil {
		log.Error("save object", "err", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	log.Info("metadata patched", "object_id", objectID, "new_version", manifest.Version)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", etagFor(manifest.Version))
	json.NewEncoder(w).Encode(manifest) //nolint:errcheck
}

// etagFor returns a quoted ETag string for the given version number.
func etagFor(version int64) string { return fmt.Sprintf(`"%d"`, version) }

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	objectID := r.PathValue("id")
	log := slog.With("handler", "delete", "object_id", objectID)

	manifest, err := s.meta.GetObject(r.Context(), objectID)
	if err != nil {
		if errors.Is(err, models.ErrNotFound) {
			http.Error(w, "object not found", http.StatusNotFound)
			return
		}
		log.Error("get object", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if manifest.Status == models.StatusDeleted {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Mark object deleted first so concurrent downloads fail fast
	if err := s.meta.UpdateObjectStatus(r.Context(), objectID, models.StatusDeleted); err != nil {
		log.Error("mark deleted", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	refs, err := s.meta.GetChunkRefs(r.Context(), objectID)
	if err != nil {
		log.Error("get chunk refs", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	for _, ref := range refs {
		newCount, err := s.meta.DecrementChunkRefCount(r.Context(), ref.ChunkID)
		if err != nil {
			log.Error("decrement ref count", "chunk_id", ref.ChunkID, "err", err)
			continue
		}
		if newCount > 0 {
			// other objects still reference this chunk — leave it on workers
			continue
		}

		// ref count hit zero — delete from workers and metadata
		if err := s.meta.UpdateChunkState(r.Context(), ref.ChunkID, models.ChunkPendingDelete); err != nil {
			log.Error("mark chunk pending delete", "chunk_id", ref.ChunkID, "err", err)
		}

		chunk, err := s.meta.GetChunk(r.Context(), ref.ChunkID)
		if err == nil {
			for _, replica := range chunk.Replicas {
				if delErr := s.workers.DeleteChunk(r.Context(), replica.WorkerAddr, ref.ChunkID); delErr != nil {
					log.Warn("delete chunk from worker", "chunk_id", ref.ChunkID,
						"worker", replica.WorkerID, "err", delErr)
				}
			}
		}

		if err := s.meta.DeleteChunk(r.Context(), ref.ChunkID); err != nil {
			log.Error("delete chunk metadata", "chunk_id", ref.ChunkID, "err", err)
		}
	}

	log.Info("delete complete", "chunks", len(refs))
	w.WriteHeader(http.StatusNoContent)
}
