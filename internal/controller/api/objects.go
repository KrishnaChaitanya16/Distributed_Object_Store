package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

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
