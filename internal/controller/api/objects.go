package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

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

// chunkLocation is the per-chunk entry in the metadata response (PDF §3).
type chunkLocation struct {
	ChunkID   string   `json:"chunk_id"`
	Index     int      `json:"index"`
	Offset    int64    `json:"offset"`
	Size      int      `json:"size"`
	Locations []string `json:"locations"` // [primaryWorkerID, secondaryWorkerID]
}

// metadataResponse embeds the full manifest and adds chunk storage locations.
type metadataResponse struct {
	*models.ObjectManifest
	Chunks []chunkLocation `json:"chunks"`
}

// GET /api/v1/objects/{id}/metadata — returns metadata, chunk locations, and ETag header.
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

	refs, err := s.meta.GetChunkRefs(r.Context(), objectID)
	if err != nil {
		slog.Error("get chunk refs for metadata", "object_id", objectID, "err", err)
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	chunks := make([]chunkLocation, len(refs))
	for i, ref := range refs {
		chunks[i] = chunkLocation{
			ChunkID:   ref.ChunkID,
			Index:     ref.Index,
			Offset:    ref.Offset,
			Size:      ref.Size,
			Locations: []string{ref.PrimaryID, ref.SecondaryID},
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", etagFor(manifest.Version))
	json.NewEncoder(w).Encode(metadataResponse{ //nolint:errcheck
		ObjectManifest: manifest,
		Chunks:         chunks,
	})
}

// PATCH /api/v1/objects/{id}/metadata — updates custom metadata key-value pairs.
// Supports optimistic concurrency via If-Match header (RFC 7232).
// The read-check-merge-bump happens in one atomic bbolt transaction.
// Returns 409 on version conflict; new ETag on success.
func (s *Server) handlePatchMetadata(w http.ResponseWriter, r *http.Request) {
	objectID := r.PathValue("id")
	log := slog.With("handler", "patch-metadata", "object_id", objectID)

	// Parse body first so bad JSON gives 400 before we touch the store.
	var patch map[string]string
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	ifMatch, err := parseIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		http.Error(w, `{"error":"invalid If-Match"}`, http.StatusBadRequest)
		return
	}

	manifest, err := s.meta.PatchCustomMetadata(r.Context(), objectID, ifMatch, patch)
	if err != nil {
		switch {
		case errors.Is(err, models.ErrNotFound):
			http.Error(w, `{"error":"object not found"}`, http.StatusNotFound)
		case errors.Is(err, models.ErrVersionConflict):
			log.Warn("version conflict on patch", "object_id", objectID)
			http.Error(w, `{"error":"version conflict — re-read and retry"}`, http.StatusConflict)
		default:
			log.Error("patch metadata", "err", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		}
		return
	}

	log.Info("metadata patched", "object_id", objectID, "new_version", manifest.Version)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", etagFor(manifest.Version))
	json.NewEncoder(w).Encode(manifest) //nolint:errcheck
}

// parseIfMatch parses an If-Match header value per RFC 7232.
// Returns -1 (no version check) for empty string or "*".
// Accepts quoted integers ("2") and weak tags (W/"2").
// Returns an error for any other format.
func parseIfMatch(h string) (int64, error) {
	if h == "" || h == "*" {
		return -1, nil
	}
	s := strings.TrimPrefix(h, "W/")
	s = strings.Trim(s, `"`)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errors.New("invalid If-Match")
	}
	return n, nil
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
	// Only COMMITTED objects can be deleted. STAGED objects are mid-upload;
	// FAILED objects belong to the GC; DELETED are already gone.
	if manifest.Status != models.StatusCommitted {
		http.Error(w, "object not found", http.StatusNotFound)
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

	// Remove the manifest itself so bbolt doesn't accumulate DELETED tombstones.
	// If this crashes, the GC's DELETED pass will clean it on the next tick.
	if err := s.meta.DeleteObject(r.Context(), objectID); err != nil {
		log.Warn("delete manifest", "object_id", objectID, "err", err)
		// Non-fatal: the object is already marked DELETED; GC will finish the job.
	}

	log.Info("delete complete", "chunks", len(refs))
	w.WriteHeader(http.StatusNoContent)
}
