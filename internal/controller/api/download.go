package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

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

	if manifest.ContentType != "" {
		w.Header().Set("Content-Type", manifest.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("X-Object-ID", objectID)
	w.Header().Set("Content-Disposition", `attachment; filename="`+manifest.Name+`"`)
	w.Header().Set("Content-Length", strconv.FormatInt(manifest.SizeBytes, 10))
	w.WriteHeader(http.StatusOK)

	for _, ref := range refs {
		if err := s.streamChunk(r.Context(), w, ref, log); err != nil {
			log.Error("stream chunk failed — cutting connection", "chunk_id", ref.ChunkID, "err", err)
			return
		}
	}

	log.Info("download complete", "chunks", len(refs), "bytes", manifest.SizeBytes)
}

// streamChunk fetches one chunk using hedged reads: starts with the primary, and
// if the primary hasn't responded within s.hedgeMs milliseconds, also fires the
// secondary. Whichever replies first wins; the loser's context is cancelled.
func (s *Server) streamChunk(ctx context.Context, w io.Writer, ref models.ChunkRef, log *slog.Logger) error {
	type result struct {
		rc       io.ReadCloser
		err      error
		workerID string
	}

	ch := make(chan result, 2)

	pCtx, pCancel := context.WithCancel(ctx)
	defer pCancel()

	primaryAddr := s.workerAddr(ref.PrimaryID)
	log.Debug("fetching chunk from primary", "chunk_id", ref.ChunkID, "index", ref.Index,
		"worker_id", ref.PrimaryID, "addr", primaryAddr)

	go func() {
		rc, err := s.workers.GetChunk(pCtx, primaryAddr, ref.ChunkID)
		select {
		case ch <- result{rc, err, ref.PrimaryID}:
		default:
		}
	}()

	threshold := time.Duration(s.hedgeMs) * time.Millisecond
	timer := time.NewTimer(threshold)
	defer timer.Stop()

	sCtx, sCancel := context.WithCancel(ctx)
	defer sCancel()

	secondaryAddr := s.workerAddr(ref.SecondaryID)

	select {
	case r := <-ch:
		// Primary responded before hedge threshold
		timer.Stop()
		if r.err == nil {
			log.Debug("chunk fetched from primary", "chunk_id", ref.ChunkID, "worker_id", ref.PrimaryID)
			defer r.rc.Close()
			_, err := io.Copy(w, r.rc)
			log.Debug("chunk streamed to client", "chunk_id", ref.ChunkID)
			return err
		}
		// Primary failed fast — start secondary immediately
		log.Warn("primary chunk fetch failed — trying secondary",
			"chunk_id", ref.ChunkID, "worker_id", ref.PrimaryID, "err", r.err)
		go func() {
			rc, err := s.workers.GetChunk(sCtx, secondaryAddr, ref.ChunkID)
			select {
			case ch <- result{rc, err, ref.SecondaryID}:
			default:
			}
		}()

	case <-timer.C:
		// Primary is slow — fire hedge request to secondary
		log.Debug("hedging chunk fetch to secondary", "chunk_id", ref.ChunkID,
			"threshold_ms", s.hedgeMs, "worker_id", ref.SecondaryID)
		go func() {
			rc, err := s.workers.GetChunk(sCtx, secondaryAddr, ref.ChunkID)
			select {
			case ch <- result{rc, err, ref.SecondaryID}:
			default:
			}
		}()
	}

	// Collect up to two results; return on first success, error only if both fail
	for attempts := 0; attempts < 2; attempts++ {
		select {
		case r := <-ch:
			if r.err != nil {
				log.Warn("chunk fetch failed", "chunk_id", ref.ChunkID,
					"worker_id", r.workerID, "err", r.err)
				continue
			}
			// Cancel the losing request
			if r.workerID == ref.PrimaryID {
				sCancel()
			} else {
				pCancel()
			}
			log.Debug("chunk fetched", "chunk_id", ref.ChunkID, "worker_id", r.workerID)
			defer r.rc.Close()
			_, err := io.Copy(w, r.rc)
			log.Debug("chunk streamed to client", "chunk_id", ref.ChunkID)
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("chunk %s: both replicas failed", ref.ChunkID)
}
