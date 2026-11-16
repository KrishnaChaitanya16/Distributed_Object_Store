package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

type uploadResponse struct {
	ObjectID string `json:"object_id"`
	Chunks   int    `json:"chunks"`
	Bytes    int64  `json:"size_bytes"`
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	log := slog.With("handler", "upload")

	// --- parse multipart (max 10 GB in memory buffer = 32 MB) ---
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "expected multipart/form-data with field 'file'", http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing 'file' field", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// --- build STAGED manifest ---
	objectID := newID()
	manifest := &models.ObjectManifest{
		ID:          objectID,
		Name:        header.Filename,
		ContentType: header.Header.Get("Content-Type"),
		Status:      models.StatusStaged,
		CreatedAt:   time.Now().UTC(),
		ModifiedAt:  time.Now().UTC(),
	}
	if err := s.meta.SaveObject(r.Context(), manifest); err != nil {
		log.Error("save staged manifest", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

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

		// placement
		primaryID, err := s.placement.Primary(chunkID)
		if err != nil {
			s.rollback(r.Context(), objectID, log)
			http.Error(w, "no healthy workers", http.StatusServiceUnavailable)
			return
		}
		secondaryID, err := s.placement.Secondary(chunkID, primaryID)
		if err != nil {
			s.rollback(r.Context(), objectID, log)
			http.Error(w, "insufficient workers for replication", http.StatusServiceUnavailable)
			return
		}

		log.Debug("chunk placed", "index", index, "chunk_id", chunkID, "primary", primaryID, "secondary", secondaryID)

		// Bloom filter O(1) pre-check — skips bbolt round-trip for definitely-new chunks.
		// False positives fall through to the GetChunk bbolt lookup.
		chunkIsNew := !s.bloom.MightExist(chunkID)
		if !chunkIsNew {
			existing, _ := s.meta.GetChunk(r.Context(), chunkID)
			chunkIsNew = existing == nil
		}
		if chunkIsNew {
			log.Debug("chunk is new — saving to bbolt and pushing to workers", "chunk_id", chunkID)
			rec := &models.ChunkRecord{
				ChunkID:   chunkID,
				SizeBytes: n,
				RefCount:  0,
				State:     models.ChunkPending,
				Replicas: []models.Replica{
					{WorkerID: primaryID, WorkerAddr: s.workerAddr(primaryID)},
					{WorkerID: secondaryID, WorkerAddr: s.workerAddr(secondaryID)},
				},
				CreatedAt: time.Now().UTC(),
			}
			if err := s.meta.SaveChunk(r.Context(), rec); err != nil {
				s.rollback(r.Context(), objectID, log)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			log.Debug("chunk saved to bbolt", "chunk_id", chunkID, "state", "PENDING")

			// push to both workers
			log.Debug("pushing chunk to primary worker", "chunk_id", chunkID, "worker_id", primaryID, "addr", s.workerAddr(primaryID))
			if err := s.workers.PutChunk(r.Context(), s.workerAddr(primaryID), chunkID,
				newBytesReader(chunk), int64(n)); err != nil {
				log.Error("put chunk primary", "chunk_id", chunkID, "worker", primaryID, "err", err)
				s.rollback(r.Context(), objectID, log)
				http.Error(w, "worker error", http.StatusBadGateway)
				return
			}
			log.Debug("chunk stored on primary", "chunk_id", chunkID, "worker_id", primaryID)

			log.Debug("pushing chunk to secondary worker", "chunk_id", chunkID, "worker_id", secondaryID, "addr", s.workerAddr(secondaryID))
			if err := s.workers.PutChunk(r.Context(), s.workerAddr(secondaryID), chunkID,
				newBytesReader(chunk), int64(n)); err != nil {
				log.Error("put chunk secondary", "chunk_id", chunkID, "worker", secondaryID, "err", err)
				s.rollback(r.Context(), objectID, log)
				http.Error(w, "worker error", http.StatusBadGateway)
				return
			}
			log.Debug("chunk stored on secondary", "chunk_id", chunkID, "worker_id", secondaryID)

			if err := s.meta.UpdateChunkState(r.Context(), chunkID, models.ChunkDurable); err != nil {
				s.rollback(r.Context(), objectID, log)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			log.Debug("chunk state updated in bbolt", "chunk_id", chunkID, "state", "DURABLE")
			s.bloom.Add(chunkID)
		} else {
			log.Debug("chunk already exists (dedup) — skipping worker push", "chunk_id", chunkID)
		}

		if err := s.meta.IncrementChunkRefCount(r.Context(), chunkID); err != nil {
			s.rollback(r.Context(), objectID, log)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		refs = append(refs, models.ChunkRef{
			ChunkID:     chunkID,
			Index:       index,
			Offset:      totalSize - int64(n),
			Size:        n,
			PrimaryID:   primaryID,
			SecondaryID: secondaryID,
		})
		index++

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			s.rollback(r.Context(), objectID, log)
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
	}

	// --- save chunk refs ---
	if err := s.meta.SaveChunkRefs(r.Context(), objectID, refs); err != nil {
		s.rollback(r.Context(), objectID, log)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// --- COMMIT: flip manifest ---
	manifest.SizeBytes = totalSize
	manifest.ChunkCount = len(refs)
	manifest.Status = models.StatusCommitted
	manifest.Checksum = objectChecksum(refs)
	manifest.ModifiedAt = time.Now().UTC()
	if err := s.meta.SaveObject(r.Context(), manifest); err != nil {
		s.rollback(r.Context(), objectID, log)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	log.Info("upload complete", "object_id", objectID, "chunks", len(refs), "bytes", totalSize)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(uploadResponse{ //nolint:errcheck
		ObjectID: objectID,
		Chunks:   len(refs),
		Bytes:    totalSize,
	})
}

// rollback marks the object FAILED so it can be GC'd later.
func (s *Server) rollback(ctx interface{ Done() <-chan struct{} }, objectID string, log *slog.Logger) {
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
