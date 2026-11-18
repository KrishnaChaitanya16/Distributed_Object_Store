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
	"strings"
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
func (s *Server) fetchVerified(ctx context.Context, workerID, role, addr, chunkID string, size int) ([]byte, error) {
	started := time.Now()
	buf, err := s.fetchChunk(ctx, addr, chunkID, size)
	ms := time.Since(started).Milliseconds()
	var hme *hashMismatchError
	switch {
	case err == nil:
		emit(ctx, "download.fetch", "chunk fetched and SHA-256 verified", "chunk_id", chunkID, "worker", workerID, "role", role, "ok", true, "ms", ms)
	case errors.As(err, &hme):
		emit(ctx, "download.corrupt", "fetched bytes do not hash to the chunk id", "chunk_id", chunkID, "worker", workerID, "role", role, "got", hme.got, "ms", ms)
	case errors.Is(err, context.Canceled):
		emit(ctx, "download.fetch", "fetch cancelled, the other replica answered first", "chunk_id", chunkID, "worker", workerID, "role", role, "ok", false, "cancelled", true, "ms", ms)
	default:
		emit(ctx, "download.fetch", "fetch from a worker failed", "chunk_id", chunkID, "worker", workerID, "role", role, "ok", false, "error", err.Error(), "ms", ms)
	}
	return buf, err
}

func (s *Server) fetchChunk(ctx context.Context, addr, chunkID string, size int) ([]byte, error) {
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
	// Fast-fail before attempting network calls: if both replica workers are
	// known-offline we'd just wait for TCP timeouts on two dead hosts.
	primaryOnline := s.health.GetWorkerStatus(ref.PrimaryID) == models.WorkerOnline
	secondaryOnline := s.health.GetWorkerStatus(ref.SecondaryID) == models.WorkerOnline
	if !primaryOnline && !secondaryOnline {
		return nil, nil, fmt.Errorf("chunk %s: both replica workers offline (primary=%s secondary=%s)",
			ref.ChunkID, ref.PrimaryID, ref.SecondaryID)
	}

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
		b, e := s.fetchVerified(pCtx, ref.PrimaryID, "primary", primaryAddr, ref.ChunkID, ref.Size)
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
		emit(ctx, "download.hedge", "primary is slow, asking the secondary too", "chunk_id", ref.ChunkID, "primary", ref.PrimaryID, "secondary", ref.SecondaryID, "threshold_ms", s.hedgeMs)
		log.Debug("hedging chunk fetch to secondary", "chunk_id", ref.ChunkID,
			"threshold_ms", s.hedgeMs, "worker_id", ref.SecondaryID)
		go func() {
			b, e := s.fetchVerified(sCtx, ref.SecondaryID, "hedge", secondaryAddr, ref.ChunkID, ref.Size)
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
		b, e := s.fetchVerified(sCtx, ref.SecondaryID, "secondary", secondaryAddr, ref.ChunkID, ref.Size)
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
					emit(ctx, "download.repair", "read-repair failed", "chunk_id", chunkID, "from", sID, "to", pID, "ok", false, "error", rErr.Error())
				} else {
					log.Info("read-repair succeeded", "chunk_id", chunkID, "primary", pID)
					emit(ctx, "download.repair", "corrupt copy overwritten with the good one", "chunk_id", chunkID, "from", sID, "to", pID, "ok", true)
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
	ctx := withTrace(r.Context(), r.Header.Get(traceHeader))
	r = r.WithContext(ctx)

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
	if manifest.Status != models.StatusCommitted {
		writeError(w, r, http.StatusNotFound, "object not available")
		return
	}

	refs, err := s.meta.GetChunkRefs(r.Context(), objectID)
	if err != nil {
		log.Error("get chunk refs", "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}
	for i := range refs {
		refs[i] = s.locate(r.Context(), refs[i])
	}

	total := manifest.SizeBytes

	// Always advertise range support.
	w.Header().Set("Accept-Ranges", "bytes")

	// Parse an optional Range header (RFC 7233). rangeStart/rangeEnd are
	// inclusive byte positions within the object [0, total-1].
	rangeStart, rangeEnd, isRange, rangeErr := parseByteRange(r.Header.Get("Range"), total)
	if rangeErr != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
		writeError(w, r, http.StatusRequestedRangeNotSatisfiable, "invalid range")
		return
	}
	if !isRange {
		rangeStart, rangeEnd = 0, total-1
	}
	serveBytes := rangeEnd - rangeStart + 1

	emit(ctx, "download.start", "download requested", "object_id", objectID, "name", manifest.Name,
		"chunks", len(refs), "bytes", total, "range", isRange, "range_start", rangeStart, "range_end", rangeEnd)

	// Collect only the chunks that overlap [rangeStart, rangeEnd].
	type chunkSlice struct {
		ref        models.ChunkRef
		sliceStart int // byte offset within the fetched chunk buf to start writing
		sliceEnd   int // exclusive
	}
	var needed []chunkSlice
	for _, ref := range refs {
		chunkStart := ref.Offset
		chunkEnd := ref.Offset + int64(ref.Size) - 1
		if chunkEnd < rangeStart || chunkStart > rangeEnd {
			continue // chunk entirely outside range — skip the network fetch
		}
		ss := 0
		if rangeStart > chunkStart {
			ss = int(rangeStart - chunkStart)
		}
		se := ref.Size
		if rangeEnd < chunkEnd {
			se = int(rangeEnd-chunkStart) + 1
		}
		needed = append(needed, chunkSlice{ref: ref, sliceStart: ss, sliceEnd: se})
	}

	// Pre-fetch the first needed chunk before writing headers so corruption
	// on the first chunk returns 502 rather than a truncated 206/200 body.
	var firstBuf []byte
	var firstRepair func()
	if len(needed) > 0 {
		firstBuf, firstRepair, err = s.fetchWithFallback(r.Context(), needed[0].ref, log)
		if err != nil {
			log.Error("first chunk unrecoverable", "object_id", objectID, "err", err)
			// Both-offline means the data may still exist — return 503 (retryable)
			// rather than 502 (bad gateway / data corruption).
			if s.health.GetWorkerStatus(refs[0].PrimaryID) != models.WorkerOnline &&
				s.health.GetWorkerStatus(refs[0].SecondaryID) != models.WorkerOnline {
				writeError(w, r, http.StatusServiceUnavailable, "both replica workers offline — try again later")
			} else {
				writeError(w, r, http.StatusBadGateway, "no valid replica")
			}
			return
		}
	}

	// Write response headers.
	ct := manifest.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("X-Object-ID", objectID)
	w.Header().Set("Content-Disposition", `attachment; filename="`+manifest.Name+`"`)
	w.Header().Set("Content-Length", strconv.FormatInt(serveBytes, 10))
	if isRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, total))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if len(needed) == 0 {
		log.Info("download complete", "chunks", 0, "bytes", 0)
		return
	}

	// Stream the first chunk (already fetched).
	if firstRepair != nil {
		go firstRepair()
	}
	slice := needed[0]
	out := firstBuf[slice.sliceStart:slice.sliceEnd]
	if _, writeErr := w.Write(out); writeErr != nil {
		return
	}
	log.Debug("chunk streamed to client", "chunk_id", slice.ref.ChunkID, "bytes", len(out))

	// Stream remaining chunks, fetching only the ones we need.
	for _, cs := range needed[1:] {
		buf, repair, fetchErr := s.fetchWithFallback(r.Context(), cs.ref, log)
		if fetchErr != nil {
			log.Error("chunk unrecoverable — cutting connection", "chunk_id", cs.ref.ChunkID, "err", fetchErr)
			return
		}
		if repair != nil {
			go repair()
		}
		out := buf[cs.sliceStart:cs.sliceEnd]
		if _, writeErr := w.Write(out); writeErr != nil {
			return
		}
		log.Debug("chunk streamed to client", "chunk_id", cs.ref.ChunkID, "bytes", len(out))
	}

	metrics.DownloadBytesTotal.Add(float64(serveBytes))
	log.Info("download complete", "chunks", len(needed), "bytes", serveBytes, "range", isRange)
	emit(ctx, "download.done", "object streamed to the client, every chunk verified",
		"object_id", objectID, "chunks", len(needed), "bytes", serveBytes, "range", isRange)
}

// parseByteRange parses a single RFC 7233 Range header value against a known
// total size. Returns (start, end, true, nil) on a valid range request,
// (0, 0, false, nil) when no Range header is present, or an error for
// syntactically invalid or unsatisfiable ranges.
//
// Supports:
//
//	bytes=500-999   → start=500, end=999
//	bytes=500-      → start=500, end=total-1
//	bytes=-500      → last 500 bytes
func parseByteRange(header string, total int64) (start, end int64, isRange bool, err error) {
	if header == "" {
		return 0, 0, false, nil
	}
	if total == 0 {
		return 0, 0, false, errors.New("empty object")
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, errors.New("unsupported range unit")
	}
	spec := strings.TrimPrefix(header, "bytes=")
	// Multi-range (bytes=0-100,200-300) — not supported, return 416.
	if strings.Contains(spec, ",") {
		return 0, 0, false, errors.New("multi-range not supported")
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, errors.New("malformed range")
	}
	rawStart, rawEnd := parts[0], parts[1]

	switch {
	case rawStart == "" && rawEnd != "":
		// Suffix range: bytes=-N → last N bytes.
		n, e := strconv.ParseInt(rawEnd, 10, 64)
		if e != nil || n <= 0 {
			return 0, 0, false, errors.New("malformed suffix range")
		}
		if n > total {
			n = total
		}
		return total - n, total - 1, true, nil
	case rawStart != "" && rawEnd == "":
		// Open-ended range: bytes=N-
		s, e := strconv.ParseInt(rawStart, 10, 64)
		if e != nil || s < 0 {
			return 0, 0, false, errors.New("malformed open range")
		}
		if s >= total {
			return 0, 0, false, errors.New("range start beyond EOF")
		}
		return s, total - 1, true, nil
	default:
		// Explicit range: bytes=N-M
		s, e1 := strconv.ParseInt(rawStart, 10, 64)
		en, e2 := strconv.ParseInt(rawEnd, 10, 64)
		if e1 != nil || e2 != nil || s < 0 || en < s {
			return 0, 0, false, errors.New("malformed explicit range")
		}
		if s >= total {
			return 0, 0, false, errors.New("range start beyond EOF")
		}
		if en >= total {
			en = total - 1
		}
		return s, en, true, nil
	}
}

// locations lists the workers bbolt says hold the chunk, online ones first; refs from before a rebalance fall back to their original pair.
func (s *Server) locations(ctx context.Context, ref models.ChunkRef) []string {
	var ids []string
	add := func(id string) {
		for _, x := range ids {
			if x == id {
				return
			}
		}
		if id != "" {
			ids = append(ids, id)
		}
	}
	if rec, err := s.meta.GetChunk(ctx, ref.ChunkID); err == nil {
		for _, r := range rec.Replicas {
			if s.health.GetWorkerStatus(r.WorkerID) == models.WorkerOnline {
				add(r.WorkerID)
			}
		}
		for _, r := range rec.Replicas {
			add(r.WorkerID)
		}
	}
	if len(ids) == 0 {
		add(ref.PrimaryID)
		add(ref.SecondaryID)
	}
	return ids
}

// locate points a ref at the replicas bbolt currently lists, so reads follow copies moved by the rebalancer or reconciler.
func (s *Server) locate(ctx context.Context, ref models.ChunkRef) models.ChunkRef {
	ids := s.locations(ctx, ref)
	if len(ids) == 0 {
		return ref
	}
	ref.PrimaryID = ids[0]
	if len(ids) > 1 {
		ref.SecondaryID = ids[1]
	} else {
		ref.SecondaryID = ids[0]
	}
	return ref
}
