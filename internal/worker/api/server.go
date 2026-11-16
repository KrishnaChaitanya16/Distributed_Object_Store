package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/scrubber"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const maxControlBody = 4 << 10

// Server exposes a worker's ChunkStore over the internal HTTP API.
type Server struct {
	store    models.ChunkStore
	workerID string
	selfAddr string
	peer     *http.Client
	scrub    ScrubberControl
}

// ScrubberControl is the part of the scrubber the HTTP API exposes; *scrubber.Scrubber satisfies it.
type ScrubberControl interface {
	Trigger() bool
	Status() scrubber.Status
}

// New builds a Server; selfAddr is this worker's advertised host:port, peer is used for replicate-peer pushes.
func New(store models.ChunkStore, workerID, selfAddr string, peer *http.Client) *Server {
	return &Server{store: store, workerID: workerID, selfAddr: selfAddr, peer: peer}
}

// WithScrubber exposes the scrubber's status and manual trigger; without it those routes answer 404.
func (s *Server) WithScrubber(sc ScrubberControl) *Server {
	s.scrub = sc
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /internal/chunks/{id}", s.putChunk)
	mux.HandleFunc("GET /internal/chunks/{id}", s.getChunk)
	mux.HandleFunc("DELETE /internal/chunks/{id}", s.deleteChunk)
	mux.HandleFunc("GET /internal/workers/{id}/chunks", s.inventory)
	mux.HandleFunc("POST /internal/workers/replicate-peer", s.replicatePeer)
	if s.scrub != nil {
		mux.HandleFunc("GET /internal/scrubber/status", s.scrubStatus)
		mux.HandleFunc("POST /internal/scrubber/run", s.scrubRun)
	}
	return mux
}

func (s *Server) putChunk(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength < 0 {
		writeErr(w, http.StatusLengthRequired, "LENGTH_REQUIRED", "Content-Length header is required")
		return
	}
	if r.ContentLength > storage.MaxChunkSize {
		writeErr(w, http.StatusRequestEntityTooLarge, "CHUNK_TOO_LARGE", storage.ErrChunkTooLarge.Error())
		return
	}
	body := &trackedReader{r: r.Body}
	err := s.store.Write(r.Context(), r.PathValue("id"), body, r.ContentLength)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusCreated)
	case errors.Is(err, storage.ErrInvalidChunkID):
		writeErr(w, http.StatusBadRequest, "INVALID_CHUNK_ID", err.Error())
	case body.err != nil:
		writeErr(w, http.StatusBadRequest, "BODY_READ_FAILED", body.err.Error())
	case errors.Is(err, storage.ErrSizeMismatch):
		writeErr(w, http.StatusBadRequest, "SIZE_MISMATCH", err.Error())
	case errors.Is(err, storage.ErrChecksumMismatch):
		writeErr(w, http.StatusBadRequest, "CHECKSUM_MISMATCH", err.Error())
	case errors.Is(err, context.Canceled):
		writeErr(w, http.StatusServiceUnavailable, "REQUEST_CANCELLED", err.Error())
	default:
		s.internalErr(w, "STORAGE_ERROR", "put chunk", err)
	}
}

func (s *Server) getChunk(w http.ResponseWriter, r *http.Request) {
	rc, ok := s.openChunk(r.Context(), w, r.PathValue("id"))
	if !ok {
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	if sz, ok := rc.(interface{ Size() int64 }); ok {
		w.Header().Set("Content-Length", strconv.FormatInt(sz.Size(), 10))
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := io.Copy(w, rc); err != nil {
		log.Printf("get chunk %s: stream aborted: %v", r.PathValue("id"), err)
	}
}

// deleteChunk is idempotent: deleting a chunk that is already gone succeeds, so GC retries never get stuck.
func (s *Server) deleteChunk(w http.ResponseWriter, r *http.Request) {
	err := s.store.Delete(r.Context(), r.PathValue("id"))
	switch {
	case err == nil, errors.Is(err, models.ErrNotFound):
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, storage.ErrInvalidChunkID):
		writeErr(w, http.StatusBadRequest, "INVALID_CHUNK_ID", err.Error())
	default:
		s.internalErr(w, "STORAGE_ERROR", "delete chunk", err)
	}
}

func (s *Server) inventory(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("id") != s.workerID {
		writeErr(w, http.StatusNotFound, "WORKER_MISMATCH", fmt.Sprintf("this is worker %q", s.workerID))
		return
	}
	ids, err := s.store.List(r.Context())
	if err != nil {
		s.internalErr(w, "STORAGE_ERROR", "list chunks", err)
		return
	}
	writeJSON(w, http.StatusOK, models.InventoryResponse{WorkerID: s.workerID, ChunkIDs: ids, Count: len(ids)})
}

// replicatePeer streams a local chunk straight to another worker so the controller stays out of the data path.
func (s *Server) replicatePeer(w http.ResponseWriter, r *http.Request) {
	var req models.ReplicatePeerRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxControlBody)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "body must be JSON {chunk_id, target_addr}")
		return
	}
	if !validHostPort(req.TargetAddr) {
		writeErr(w, http.StatusBadRequest, "INVALID_TARGET", "target_addr must be host:port")
		return
	}
	if req.TargetAddr == s.selfAddr {
		writeErr(w, http.StatusBadRequest, "TARGET_IS_SELF", "cannot replicate a chunk to the worker that holds it")
		return
	}
	rc, ok := s.openChunk(r.Context(), w, req.ChunkID)
	if !ok {
		return
	}
	defer rc.Close()

	url := "http://" + req.TargetAddr + "/internal/chunks/" + req.ChunkID
	preq, err := http.NewRequestWithContext(r.Context(), http.MethodPut, url, io.NopCloser(rc))
	if err != nil {
		s.internalErr(w, "INTERNAL_ERROR", "build peer request", err)
		return
	}
	if sz, ok := rc.(interface{ Size() int64 }); ok {
		preq.ContentLength = sz.Size()
	}
	// Go's client treats ContentLength 0 with a non-nil body as unknown and would send it chunked.
	if preq.ContentLength == 0 {
		preq.Body = http.NoBody
	}
	preq.Header.Set("Content-Type", "application/octet-stream")
	resp, err := s.peer.Do(preq)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "PEER_UNREACHABLE", err.Error())
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxControlBody))
	if resp.StatusCode != http.StatusCreated {
		writeErr(w, http.StatusBadGateway, "PEER_REJECTED", fmt.Sprintf("target answered %d", resp.StatusCode))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) scrubStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.scrub.Status())
}

// scrubRun queues a pass and returns at once; queued is false when a pass was already waiting to start.
func (s *Server) scrubRun(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusAccepted, map[string]bool{"queued": s.scrub.Trigger()})
}

// openChunk reads a chunk or writes the matching error response and returns ok=false.
func (s *Server) openChunk(ctx context.Context, w http.ResponseWriter, id string) (io.ReadCloser, bool) {
	rc, err := s.store.Read(ctx, id)
	switch {
	case err == nil:
		return rc, true
	case errors.Is(err, storage.ErrInvalidChunkID):
		writeErr(w, http.StatusBadRequest, "INVALID_CHUNK_ID", err.Error())
	case errors.Is(err, models.ErrNotFound):
		writeErr(w, http.StatusNotFound, "CHUNK_NOT_FOUND", "chunk not found")
	case errors.Is(err, storage.ErrCorruptChunk):
		s.internalErr(w, "CHUNK_CORRUPT", "read chunk "+id, err)
	default:
		s.internalErr(w, "STORAGE_ERROR", "read chunk "+id, err)
	}
	return nil, false
}

func validHostPort(v string) bool {
	host, port, err := net.SplitHostPort(v)
	if err != nil || host == "" {
		return false
	}
	p, err := strconv.Atoi(port)
	return err == nil && p >= 1 && p <= 65535
}

func (s *Server) internalErr(w http.ResponseWriter, code, op string, err error) {
	log.Printf("worker %s: %s: %v", s.workerID, op, err)
	writeErr(w, http.StatusInternalServerError, code, err.Error())
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, models.ErrorResponse{Error: msg, Code: code})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// trackedReader remembers a failed read so a broken client upload maps to 400 instead of a storage 500.
type trackedReader struct {
	r   io.Reader
	err error
}

func (t *trackedReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}
