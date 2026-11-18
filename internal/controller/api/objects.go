package api

import (
	"context"
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
		writeError(w, r, http.StatusInternalServerError, "internal error")
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
			writeError(w, r, http.StatusNotFound, "object not found")
			return
		}
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}
	if manifest.Status != models.StatusCommitted {
		writeError(w, r, http.StatusNotFound, "object not found")
		return
	}

	refs, err := s.meta.GetChunkRefs(r.Context(), objectID)
	if err != nil {
		slog.Error("get chunk refs for metadata", "object_id", objectID, "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}

	chunks := make([]chunkLocation, len(refs))
	for i, ref := range refs {
		chunks[i] = chunkLocation{
			ChunkID:   ref.ChunkID,
			Index:     ref.Index,
			Offset:    ref.Offset,
			Size:      ref.Size,
			Locations: s.locations(r.Context(), ref),
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
		writeError(w, r, http.StatusBadRequest, "invalid JSON")
		return
	}

	ifMatch, err := parseIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid If-Match")
		return
	}

	manifest, err := s.meta.PatchCustomMetadata(r.Context(), objectID, ifMatch, patch)
	if err != nil {
		switch {
		case errors.Is(err, models.ErrNotFound):
			writeError(w, r, http.StatusNotFound, "object not found")
		case errors.Is(err, models.ErrVersionConflict):
			log.Warn("version conflict on patch", "object_id", objectID)
			writeError(w, r, http.StatusConflict, "version conflict — re-read and retry")
		default:
			log.Error("patch metadata", "err", err)
			writeError(w, r, http.StatusInternalServerError, "internal error")
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
			writeError(w, r, http.StatusNotFound, "object not found")
			return
		}
		log.Error("get object", "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}
	// Only COMMITTED objects can be deleted. STAGED objects are mid-upload;
	// FAILED objects belong to the GC; DELETED are already gone.
	if manifest.Status != models.StatusCommitted {
		writeError(w, r, http.StatusNotFound, "object not found")
		return
	}

	refs, err := s.meta.GetChunkRefs(r.Context(), objectID)
	if err != nil {
		log.Error("get chunk refs", "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}

	// The COMMITTED to DELETED flip is atomic, so of two concurrent deletes exactly one succeeds.
	if err := s.meta.MarkObjectDeleted(r.Context(), objectID); err != nil {
		if errors.Is(err, models.ErrNotFound) {
			writeError(w, r, http.StatusNotFound, "object not found")
			return
		}
		log.Error("mark deleted", "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}

	// Chunks still used by other objects or in-flight uploads are skipped inside the claim transaction; anything left behind is retried by the reconciler.
	ids := make([]string, len(refs))
	for i, ref := range refs {
		ids[i] = ref.ChunkID
	}
	sweepCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), deleteSweepTimeout)
	defer cancel()
	deleted, err := s.collector.Collect(sweepCtx, ids, nil)
	if err != nil {
		log.Warn("some chunks of the deleted object were not removed yet; the reconciler will retry", "err", err)
	}

	// Drop the manifest and its chunk references now so DELETED tombstones never accumulate; the reconciler repeats this if the process dies first.
	if _, err := s.meta.PurgeDeadObjects(context.WithoutCancel(r.Context())); err != nil {
		log.Warn("purge deleted manifest", "object_id", objectID, "err", err)
	}

	log.Info("delete complete", "chunks", len(refs), "chunks_deleted", deleted)
	emit(withTrace(r.Context(), r.Header.Get(traceHeader)), "object.delete", "object deleted, chunks with no remaining reference removed from the workers", "object_id", objectID, "name", manifest.Name, "chunks", len(refs), "chunks_deleted", deleted)
	w.WriteHeader(http.StatusNoContent)
}
