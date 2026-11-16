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
	"strconv"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// hashMismatchError carries the actual hash seen on the wire so callers can log it.
type hashMismatchError struct{ got string }

func (e *hashMismatchError) Error() string { return "hash mismatch: got " + e.got }

// fetchVerified GETs a chunk from addr, checks its byte count and SHA-256.
// Returns *hashMismatchError on integrity failure so callers can distinguish
// corruption from plain network/HTTP errors.
func (s *Server) fetchVerified(ctx context.Context, addr, chunkID string, size int) ([]byte, error) {
	rc, err := s.workers.GetChunk(ctx, addr, chunkID)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

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

// fetchWithFallback tries the primary then the secondary, with hedged reads:
// if the primary has not responded within s.hedgeMs the secondary is also
// started in parallel and the first success wins.
// On a primary hash mismatch (and secondary OK) it returns a non-nil repairFunc.
// Plain network errors and 404s do NOT trigger repair.
func (s *Server) fetchWithFallback(ctx context.Context, ref models.ChunkRef, log *slog.Logger) (buf []byte, repairFunc func(), err error) {
	primaryAddr := s.workerAddr(ref.PrimaryID)
	secondaryAddr := s.workerAddr(ref.SecondaryID)

	log.Debug("fetching chunk from primary", "chunk_id", ref.ChunkID, "index", ref.Index,
		"worker_id", ref.PrimaryID, "addr", primaryAddr)

	type fetchResult struct {
		buf []byte
		err error
	}

	pCtx, pCancel := context.WithCancel(ctx)
	defer pCancel()
	sCtx, sCancel := context.WithCancel(ctx)
	defer sCancel()

	primaryCh := make(chan fetchResult, 1)
	secondaryCh := make(chan fetchResult, 1)

	go func() {
		b, e := s.fetchVerified(pCtx, primaryAddr, ref.ChunkID, ref.Size)
		primaryCh <- fetchResult{b, e}
	}()

	// hedgeMs == 0 means hedging is disabled; a nil channel blocks forever in select.
	var timerC <-chan time.Time
	var timer *time.Timer
	if s.hedgeMs > 0 {
		timer = time.NewTimer(time.Duration(s.hedgeMs) * time.Millisecond)
		timerC = timer.C
		defer timer.Stop()
	}

	// Wait for primary result or hedge threshold
	var primaryRes *fetchResult
	select {
	case r := <-primaryCh:
		if timer != nil {
			timer.Stop()
		}
		primaryRes = &r
	case <-timerC:
		// Primary is slow — start secondary hedge
		metrics.HedgedReadsTotal.Inc()
		log.Debug("hedging chunk fetch to secondary", "chunk_id", ref.ChunkID,
			"threshold_ms", s.hedgeMs, "worker_id", ref.SecondaryID)
		go func() {
			b, e := s.fetchVerified(sCtx, secondaryAddr, ref.ChunkID, ref.Size)
			secondaryCh <- fetchResult{b, e}
		}()

		// Now wait for primary or secondary, whichever comes first
		select {
		case r := <-primaryCh:
			if r.err == nil {
				sCancel()
				log.Debug("chunk fetched from primary", "chunk_id", ref.ChunkID, "worker_id", ref.PrimaryID)
				return r.buf, nil, nil
			}
			primaryRes = &r
			// Primary failed — wait for secondary
		case r := <-secondaryCh:
			if r.err == nil {
				pCancel()
				log.Debug("chunk fetched from secondary (hedge)", "chunk_id", ref.ChunkID, "worker_id", ref.SecondaryID)
				return r.buf, nil, nil
			}
			// Secondary failed — wait for primary
			secErr := r.err
			select {
			case pr := <-primaryCh:
				if pr.err == nil {
					log.Debug("chunk fetched from primary (hedge)", "chunk_id", ref.ChunkID, "worker_id", ref.PrimaryID)
					return pr.buf, nil, nil
				}
				logFetchErr(log, ref.ChunkID, ref.PrimaryID, pr.err)
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			logFetchErr(log, ref.ChunkID, ref.SecondaryID, secErr)
			return nil, nil, fmt.Errorf("chunk %s: both replicas failed", ref.ChunkID)
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	// Non-hedge path: primary result is known, launch secondary if primary failed
	if primaryRes.err == nil {
		log.Debug("chunk fetched from primary", "chunk_id", ref.ChunkID, "worker_id", ref.PrimaryID)
		return primaryRes.buf, nil, nil
	}

	logFetchErr(log, ref.ChunkID, ref.PrimaryID, primaryRes.err)

	// Launch secondary now (or it may already be running from the hedge timer path above)
	go func() {
		b, e := s.fetchVerified(sCtx, secondaryAddr, ref.ChunkID, ref.Size)
		secondaryCh <- fetchResult{b, e}
	}()

	select {
	case sr := <-secondaryCh:
		if sr.err != nil {
			logFetchErr(log, ref.ChunkID, ref.SecondaryID, sr.err)
			return nil, nil, fmt.Errorf("chunk %s: both replicas failed", ref.ChunkID)
		}
		log.Debug("chunk fetched from secondary", "chunk_id", ref.ChunkID, "worker_id", ref.SecondaryID)
		// Trigger repair only on primary hash mismatch
		var hme *hashMismatchError
		if errors.As(primaryRes.err, &hme) {
			metrics.ReadRepairsTotal.Inc()
			chunkID := ref.ChunkID
			pAddr, sAddr, pID, sID := primaryAddr, secondaryAddr, ref.PrimaryID, ref.SecondaryID
			repairFunc = func() {
				rCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if rErr := s.workers.ReplicatePeer(rCtx, sAddr, chunkID, pAddr); rErr != nil {
					log.Warn("read-repair failed", "chunk_id", chunkID,
						"secondary", sID, "primary", pID, "err", rErr)
				} else {
					log.Info("read-repair succeeded", "chunk_id", chunkID, "primary", pID)
				}
			}
		}
		return sr.buf, repairFunc, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func logFetchErr(log *slog.Logger, chunkID, workerID string, err error) {
	var hme *hashMismatchError
	if errors.As(err, &hme) {
		log.Warn("chunk integrity check failed", "chunk_id", chunkID, "worker_id", workerID, "got", hme.got)
	} else {
		log.Warn("chunk fetch failed", "chunk_id", chunkID, "worker_id", workerID, "err", err)
	}
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

	// Verify chunk 0 before writing the status header so a fully corrupt first
	// chunk returns 502 instead of a 200 with a truncated body.
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

	if manifest.ContentType != "" {
		w.Header().Set("Content-Type", manifest.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("X-Object-ID", objectID)
	w.Header().Set("Content-Disposition", `attachment; filename="`+manifest.Name+`"`)
	w.Header().Set("Content-Length", strconv.FormatInt(manifest.SizeBytes, 10))
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

	metrics.DownloadBytesTotal.Add(float64(manifest.SizeBytes))
	log.Info("download complete", "chunks", len(refs), "bytes", manifest.SizeBytes)
}
