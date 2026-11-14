package api

import (
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	objectID := r.PathValue("id")
	log := slog.With("handler", "download", "object_id", objectID)

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
	if manifest.Status != models.StatusCommitted {
		http.Error(w, "object not available", http.StatusNotFound)
		return
	}

	refs, err := s.meta.GetChunkRefs(r.Context(), objectID)
	if err != nil {
		log.Error("get chunk refs", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// set headers before streaming
	if manifest.ContentType != "" {
		w.Header().Set("Content-Type", manifest.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("X-Object-ID", objectID)
	w.Header().Set("Content-Disposition", `attachment; filename="`+manifest.Name+`"`)
	w.WriteHeader(http.StatusOK)

	// stream chunks in order
	for _, ref := range refs {
		if err := s.streamChunk(r, w, ref, log); err != nil {
			// headers already sent — can't change status, just close
			log.Error("stream chunk failed", "chunk_id", ref.ChunkID, "err", err)
			return
		}
	}

	log.Info("download complete", "chunks", len(refs), "bytes", manifest.SizeBytes)
}

// streamChunk fetches one chunk from primary, falls back to secondary on error.
func (s *Server) streamChunk(r *http.Request, w io.Writer, ref models.ChunkRef, log *slog.Logger) error {
	primaryAddr := s.workerAddr(ref.PrimaryID)
	rc, err := s.workers.GetChunk(r.Context(), primaryAddr, ref.ChunkID)
	if err != nil {
		log.Warn("primary chunk fetch failed — trying secondary",
			"chunk_id", ref.ChunkID, "primary", ref.PrimaryID, "err", err)

		secondaryAddr := s.workerAddr(ref.SecondaryID)
		rc, err = s.workers.GetChunk(r.Context(), secondaryAddr, ref.ChunkID)
		if err != nil {
			return err
		}
	}
	defer rc.Close()

	_, err = io.Copy(w, rc)
	return err
}
