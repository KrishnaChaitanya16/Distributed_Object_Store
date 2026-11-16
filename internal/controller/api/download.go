package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// hashMismatchError carries the actual hash seen on the wire so callers can log it.
type hashMismatchError struct{ got string }

func (e *hashMismatchError) Error() string { return "hash mismatch: got " + e.got }

// fetchVerified GETs a chunk from addr, checks its byte count and SHA-256, and
// returns the verified bytes.  Returns *hashMismatchError on integrity failure so
// callers can distinguish corruption from plain network/HTTP errors.
func (s *Server) fetchVerified(ctx context.Context, addr, chunkID string, size int) ([]byte, error) {
	rc, err := s.workers.GetChunk(ctx, addr, chunkID)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// Read at most size+1 bytes so an oversized body is detected.
	buf, err := io.ReadAll(io.LimitReader(rc, int64(size)+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(buf) != size {
		return nil, fmt.Errorf("size mismatch: got %d want %d", len(buf), size)
	}
	sum := sha256.Sum256(buf)
	got := hex.EncodeToString(sum[:])
	if got != chunkID {
		return nil, &hashMismatchError{got: got}
	}
	return buf, nil
}

// fetchWithFallback tries the primary then the secondary.
// On a primary hash mismatch (and secondary OK) it returns a non-nil repairFunc
// that the caller should run in a background goroutine.
// Plain network errors and 404s do NOT trigger repair — the scrubber handles those.
func (s *Server) fetchWithFallback(ctx context.Context, ref models.ChunkRef, log *slog.Logger) (buf []byte, repairFunc func(), err error) {
	primaryAddr := s.workerAddr(ref.PrimaryID)
	secondaryAddr := s.workerAddr(ref.SecondaryID)

	log.Debug("fetching chunk from primary", "chunk_id", ref.ChunkID, "index", ref.Index,
		"worker_id", ref.PrimaryID, "addr", primaryAddr)

	buf, primaryErr := s.fetchVerified(ctx, primaryAddr, ref.ChunkID, ref.Size)
	if primaryErr == nil {
		log.Debug("chunk fetched from primary", "chunk_id", ref.ChunkID, "worker_id", ref.PrimaryID)
		return buf, nil, nil
	}

	// Log with "got" as a separate field so smoke-test greps can find it.
	var hme *hashMismatchError
	if errors.As(primaryErr, &hme) {
		log.Warn("chunk integrity check failed", "chunk_id", ref.ChunkID,
			"worker_id", ref.PrimaryID, "got", hme.got)
	} else {
		log.Warn("chunk fetch failed — trying secondary", "chunk_id", ref.ChunkID,
			"worker_id", ref.PrimaryID, "err", primaryErr)
	}

	log.Debug("fetching chunk from secondary", "chunk_id", ref.ChunkID,
		"worker_id", ref.SecondaryID, "addr", secondaryAddr)

	buf, secondaryErr := s.fetchVerified(ctx, secondaryAddr, ref.ChunkID, ref.Size)
	if secondaryErr != nil {
		var hme2 *hashMismatchError
		if errors.As(secondaryErr, &hme2) {
			log.Warn("chunk integrity check failed", "chunk_id", ref.ChunkID,
				"worker_id", ref.SecondaryID, "got", hme2.got)
		} else {
			log.Warn("secondary chunk fetch failed", "chunk_id", ref.ChunkID,
				"worker_id", ref.SecondaryID, "err", secondaryErr)
		}
		return nil, nil, fmt.Errorf("chunk %s: both replicas failed", ref.ChunkID)
	}

	log.Debug("chunk fetched from secondary", "chunk_id", ref.ChunkID, "worker_id", ref.SecondaryID)

	// Schedule read-repair only for hash mismatch, not network/404 errors.
	if errors.As(primaryErr, &hme) {
		chunkID := ref.ChunkID
		pAddr, sAddr, pID, sID := primaryAddr, secondaryAddr, ref.PrimaryID, ref.SecondaryID
		repairFunc = func() {
			repairCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if repairErr := s.workers.ReplicatePeer(repairCtx, sAddr, chunkID, pAddr); repairErr != nil {
				log.Warn("read-repair failed", "chunk_id", chunkID,
					"secondary", sID, "primary", pID, "err", repairErr)
			} else {
				log.Info("read-repair succeeded", "chunk_id", chunkID, "primary", pID)
			}
		}
	}

	return buf, repairFunc, nil
}

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

	// Verify chunk 0 before writing the status header so a fully corrupt first chunk
	// returns 502 instead of a 200 with no body.
	var firstBuf []byte
	var firstRepair func()
	if len(refs) > 0 {
		firstBuf, firstRepair, err = s.fetchWithFallback(r.Context(), refs[0], log)
		if err != nil {
			log.Error("chunk 0 unrecoverable", "object_id", objectID, "err", err)
			w.Header().Set("Content-Type", "application/json")
			http.Error(w, `{"error":"no valid replica"}`, http.StatusBadGateway)
			return
		}
	}

	// Status header committed — no error status changes possible from this point.
	if manifest.ContentType != "" {
		w.Header().Set("Content-Type", manifest.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("X-Object-ID", objectID)
	w.Header().Set("Content-Disposition", `attachment; filename="`+manifest.Name+`"`)
	w.WriteHeader(http.StatusOK)

	if len(refs) == 0 {
		log.Info("download complete", "chunks", 0, "bytes", 0)
		return
	}

	if firstRepair != nil {
		go firstRepair()
	}
	if _, writeErr := w.Write(firstBuf); writeErr != nil {
		return
	}
	log.Debug("chunk streamed to client", "chunk_id", refs[0].ChunkID, "bytes", len(firstBuf))

	for _, ref := range refs[1:] {
		buf, repair, fetchErr := s.fetchWithFallback(r.Context(), ref, log)
		if fetchErr != nil {
			// Headers already sent — cut the connection; corrupt bytes are never written.
			log.Error("chunk unrecoverable — cutting connection", "chunk_id", ref.ChunkID, "err", fetchErr)
			return
		}
		if repair != nil {
			go repair()
		}
		if _, writeErr := w.Write(buf); writeErr != nil {
			return
		}
		log.Debug("chunk streamed to client", "chunk_id", ref.ChunkID, "bytes", len(buf))
	}

	log.Info("download complete", "chunks", len(refs), "bytes", manifest.SizeBytes)
}
