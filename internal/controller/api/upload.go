package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

type uploadResponse struct {
	ObjectID string `json:"object_id"`
	Chunks   int    `json:"chunks"`
	Bytes    int64  `json:"size_bytes"`
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	log := slog.With("handler", "upload")
	started := time.Now()
	ctx := withTrace(r.Context(), r.Header.Get(traceHeader))
	r = r.WithContext(ctx)

	// --- idempotency check (optional header) ---
	// If the client sends Idempotency-Key, we guarantee at-most-once semantics:
	//   PROCESSING record present → another request is in flight → 409
	//   COMMITTED  record present → replay the cached 201 response
	//   No record              → proceed; write PROCESSING sentinel atomically
	idempKey := r.Header.Get("Idempotency-Key")
	keepKey := false
	if idempKey != "" {
		existing, err := s.meta.AcquireIdempotencyLock(r.Context(), idempKey)
		if err != nil {
			log.Error("acquire idempotency lock", "key", idempKey, "err", err)
			writeError(w, r, http.StatusInternalServerError, "internal error")
			return
		}
		if existing != nil {
			switch existing.Status {
			case "COMMITTED":
				// Replay exact cached response — client gets same object_id.
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Idempotency-Replayed", "true")
				w.WriteHeader(existing.StatusCode)
				w.Write(existing.Body) //nolint:errcheck
				log.Info("idempotency replay", "key", idempKey)
				return
			default: // PROCESSING
				writeError(w, r, http.StatusConflict, "request with this Idempotency-Key is already processing")
				return
			}
		}
		// This request owns the key. If it leaves without committing, the key is released so the client can retry at once instead of waiting out the TTL.
		defer func() {
			if !keepKey {
				if err := s.meta.ReleaseIdempotencyLock(context.WithoutCancel(r.Context()), idempKey); err != nil {
					log.Warn("release idempotency key", "key", idempKey, "err", err)
				}
			}
		}()
	}

	// --- parse multipart (max 10 GB in memory buffer = 32 MB) ---
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, r, http.StatusBadRequest, "expected multipart/form-data with field 'file'")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "missing 'file' field")
		return
	}
	defer file.Close()

	// --- parse optional custom metadata before touching the store ---
	var custom map[string]string
	if raw := r.FormValue("metadata"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &custom); err != nil {
			writeError(w, r, http.StatusBadRequest, "metadata must be a JSON object of string values")
			return
		}
	}

	// Fail fast if fewer than 2 workers are online — placement needs at least
	// a primary and a secondary, and a connection-refused error mid-upload is
	// worse than an immediate 503.
	if online := s.health.ListOnlineWorkers(); len(online) < 2 {
		writeError(w, r, http.StatusServiceUnavailable, "insufficient healthy workers — try again shortly")
		return
	}

	// --- build STAGED manifest ---
	objectID := newID()
	manifest := &models.ObjectManifest{
		ID:             objectID,
		Name:           header.Filename,
		ContentType:    header.Header.Get("Content-Type"),
		Status:         models.StatusStaged,
		CustomMetadata: custom,
		CreatedAt:      time.Now().UTC(),
		ModifiedAt:     time.Now().UTC(),
	}
	if err := s.meta.SaveObject(r.Context(), manifest); err != nil {
		log.Error("save staged manifest", "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}
	emit(ctx, "upload.start", "upload received, manifest staged", "object_id", objectID, "name", header.Filename, "size", header.Size, "content_type", manifest.ContentType)

	// --- chunk, hash, place, push ---
	var (
		refs      []models.ChunkRef
		totalSize int64
		buf       = make([]byte, chunkSize)
		index     int
	)

	for {
		n, readErr := io.ReadFull(file, buf)
		if n == 0 {
			break
		}
		chunk := buf[:n]
		totalSize += int64(n)

		// content-addressed ID
		sum := sha256.Sum256(chunk)
		chunkID := hex.EncodeToString(sum[:])

		log.Debug("chunk hashed", "index", index, "chunk_id", chunkID, "size_bytes", n, "offset", totalSize-int64(n))
		emit(ctx, "upload.chunk", "chunk cut and hashed", "object_id", objectID, "index", index, "chunk_id", chunkID, "size", n, "offset", totalSize-int64(n))

		// placement
		primaryID, err := s.placement.Primary(chunkID)
		if err != nil {
			s.rollback(r.Context(), objectID, log)
			writeError(w, r, http.StatusServiceUnavailable, "no healthy workers")
			return
		}
		secondaryID, err := s.placement.Secondary(chunkID, primaryID)
		if err != nil {
			s.rollback(r.Context(), objectID, log)
			writeError(w, r, http.StatusServiceUnavailable, "insufficient workers for replication")
			return
		}

		log.Debug("chunk placed", "index", index, "chunk_id", chunkID, "primary", primaryID, "secondary", secondaryID)
		emit(ctx, "upload.place", "ring picked primary and secondary", "object_id", objectID, "index", index, "chunk_id", chunkID, "ring_pos", placement.KeyHash(chunkID), "primary", primaryID, "secondary", secondaryID)

		ref := models.ChunkRef{
			ChunkID:     chunkID,
			Index:       index,
			Offset:      totalSize - int64(n),
			Size:        n,
			PrimaryID:   primaryID,
			SecondaryID: secondaryID,
		}
		// Record the reference first: from here the garbage collector treats the chunk as live, so dedup below can never race a delete.
		status, msg, err := s.storeChunk(r.Context(), objectID, ref, chunk, log)
		if err != nil {
			log.Error("store chunk", "chunk_id", chunkID, "index", index, "err", err)
			emit(ctx, "upload.error", "storing a chunk failed", "object_id", objectID, "index", index, "chunk_id", chunkID, "error", err.Error())
			s.rollback(r.Context(), objectID, log)
			writeError(w, r, status, msg)
			return
		}

		refs = append(refs, ref)
		index++

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			s.rollback(r.Context(), objectID, log)
			writeError(w, r, http.StatusBadRequest, "read error")
			return
		}
	}

	// --- COMMIT: flip manifest ---
	manifest.SizeBytes = totalSize
	manifest.ChunkCount = len(refs)
	manifest.Status = models.StatusCommitted
	manifest.Checksum = objectChecksum(refs)
	manifest.ModifiedAt = time.Now().UTC()
	if err := s.meta.CommitObject(r.Context(), manifest); err != nil {
		log.Error("commit object", "object_id", objectID, "err", err)
		s.rollback(r.Context(), objectID, log)
		if errors.Is(err, models.ErrNotStaged) {
			writeError(w, r, http.StatusServiceUnavailable, "upload expired before commit, retry")
			return
		}
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}

	metrics.UploadBytesTotal.Add(float64(totalSize))
	log.Info("upload complete", "object_id", objectID, "chunks", len(refs), "bytes", totalSize)
	emit(ctx, "upload.commit", "object committed", "object_id", objectID, "chunks", len(refs), "bytes", totalSize, "ms", time.Since(started).Milliseconds())

	resp := uploadResponse{ObjectID: objectID, Chunks: len(refs), Bytes: totalSize}
	respBody, _ := json.Marshal(resp)

	// The object is committed: keep the key either way, because releasing it could let a retry store a second copy.
	keepKey = true

	// Commit idempotency result so retries replay this exact response.
	if idempKey != "" {
		if err := s.meta.CommitIdempotencyResult(r.Context(), idempKey, http.StatusCreated, respBody); err != nil {
			log.Warn("commit idempotency result", "key", idempKey, "err", err)
			// Non-fatal: upload succeeded, we just lose the idempotency guarantee for this key.
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(respBody) //nolint:errcheck
}

// storeChunk makes one chunk durable for this upload and returns the HTTP status and message to use on failure.
func (s *Server) storeChunk(ctx context.Context, objectID string, ref models.ChunkRef, data []byte, log *slog.Logger) (int, string, error) {
	dedupOn := !s.dedupOff.Load()
	maybe := dedupOn && s.bloom.Observe(ref.ChunkID)
	rec, err := s.meta.AddChunkRef(ctx, objectID, ref)
	if err == nil && dedupOn {
		s.bloom.RecordOutcome(maybe, rec != nil)
		verdict := "definitely_new"
		if maybe {
			verdict = "true_positive"
			if rec == nil {
				verdict = "false_positive"
			}
		}
		emit(ctx, "upload.bloom", "bloom filter consulted", "object_id", objectID, "index", ref.Index, "chunk_id", ref.ChunkID, "maybe", maybe, "in_store", rec != nil, "verdict", verdict)
	}
	if errors.Is(err, models.ErrNotStaged) {
		return http.StatusServiceUnavailable, "upload expired, retry", err
	}
	if err != nil {
		return http.StatusInternalServerError, "internal error", err
	}
	if rec != nil && rec.State == models.ChunkPendingDelete {
		if rec, err = s.waitChunkReleased(ctx, ref.ChunkID); err != nil {
			return http.StatusServiceUnavailable, "chunk is being garbage collected, retry", err
		}
	}
	if dedupOn && rec != nil && rec.State == models.ChunkDurable {
		log.Debug("chunk already exists (dedup) — skipping worker push", "chunk_id", ref.ChunkID)
		metrics.ChunksDedupTotal.Inc()
		emit(ctx, "upload.dedup", "identical chunk already stored, nothing sent to workers", "object_id", objectID, "index", ref.Index, "chunk_id", ref.ChunkID)
		return 0, "", nil
	}

	// A PENDING record belongs to an upload that has not finished or never will, so its bytes may be missing and must be pushed again.
	want := &models.ChunkRecord{
		ChunkID:   ref.ChunkID,
		SizeBytes: ref.Size,
		State:     models.ChunkPending,
		Replicas: []models.Replica{
			{WorkerID: ref.PrimaryID, WorkerAddr: s.workerAddr(ref.PrimaryID)},
			{WorkerID: ref.SecondaryID, WorkerAddr: s.workerAddr(ref.SecondaryID)},
		},
		CreatedAt: time.Now().UTC(),
	}
	replace := rec != nil && !s.allOnline(rec.Replicas)
	rec, err = s.meta.PrepareChunk(ctx, want, replace)
	if err != nil {
		return http.StatusInternalServerError, "internal error", err
	}
	if dedupOn && rec.State == models.ChunkDurable {
		metrics.ChunksDedupTotal.Inc()
		return 0, "", nil
	}
	log.Debug("chunk saved to bbolt", "chunk_id", ref.ChunkID, "state", "PENDING")
	for i, rep := range rec.Replicas {
		role := "secondary"
		if i == 0 {
			role = "primary"
		}
		log.Debug("pushing chunk to "+role+" worker", "chunk_id", ref.ChunkID, "worker_id", rep.WorkerID, "addr", s.workerAddr(rep.WorkerID))
		pushStart := time.Now()
		if err := s.workers.PutChunk(ctx, s.workerAddr(rep.WorkerID), ref.ChunkID, newBytesReader(data), int64(len(data))); err != nil {
			emit(ctx, "upload.push", "pushing the chunk to a worker failed", "object_id", objectID, "index", ref.Index, "chunk_id", ref.ChunkID, "worker", rep.WorkerID, "role", role, "ok", false, "error", err.Error())
			return http.StatusBadGateway, "worker error", fmt.Errorf("put chunk on %s %s: %w", role, rep.WorkerID, err)
		}
		emit(ctx, "upload.push", "chunk stored on a worker", "object_id", objectID, "index", ref.Index, "chunk_id", ref.ChunkID, "worker", rep.WorkerID, "role", role, "ok", true, "ms", time.Since(pushStart).Milliseconds())
		log.Debug("chunk stored on "+role, "chunk_id", ref.ChunkID, "worker_id", rep.WorkerID)
	}
	if err := s.meta.MarkChunkDurable(ctx, ref.ChunkID); err != nil {
		return http.StatusInternalServerError, "internal error", err
	}
	log.Debug("chunk state updated in bbolt", "chunk_id", ref.ChunkID, "state", "DURABLE")
	emit(ctx, "upload.durable", "chunk is on both workers and marked DURABLE", "object_id", objectID, "index", ref.Index, "chunk_id", ref.ChunkID)
	s.bloom.Add(ref.ChunkID)
	metrics.ChunksNewTotal.Inc()
	return 0, "", nil
}

// waitChunkReleased waits for an in-progress garbage collection of the chunk to finish and returns its record afterwards (nil once removed).
func (s *Server) waitChunkReleased(ctx context.Context, chunkID string) (*models.ChunkRecord, error) {
	deadline := time.Now().Add(gcWaitTimeout)
	for {
		rec, err := s.meta.GetChunk(ctx, chunkID)
		if errors.Is(err, models.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if rec.State != models.ChunkPendingDelete {
			return rec, nil
		}
		if time.Now().After(deadline) {
			return nil, models.ErrChunkBusy
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (s *Server) allOnline(reps []models.Replica) bool {
	for _, r := range reps {
		if s.health.GetWorkerStatus(r.WorkerID) != models.WorkerOnline {
			return false
		}
	}
	return len(reps) > 0
}

// rollback marks the object FAILED so it can be GC'd later.
func (s *Server) rollback(ctx interface{ Done() <-chan struct{} }, objectID string, log *slog.Logger) {
	if c, ok := ctx.(context.Context); ok {
		emit(c, "upload.fail", "upload rolled back, object marked FAILED", "object_id", objectID)
	}
	// use a background context since the request context may already be cancelled
	bgCtx := noCancel(ctx)
	if err := s.meta.UpdateObjectStatus(bgCtx, objectID, models.StatusFailed); err != nil {
		log.Error("rollback failed", "object_id", objectID, "err", err)
	}
}

// workerAddr resolves a workerID to its network address via the metadata store.
// Returns empty string if not found — callers propagate the error upstream.
func (s *Server) workerAddr(workerID string) string {
	w, err := s.meta.GetWorker(noCancel2(), workerID)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", w.Address, w.Port)
}

// objectChecksum is a simple hex summary of all chunk IDs for integrity display.
func objectChecksum(refs []models.ChunkRef) string {
	h := sha256.New()
	for _, r := range refs {
		h.Write([]byte(r.ChunkID)) //nolint:errcheck
	}
	return hex.EncodeToString(h.Sum(nil))
}
