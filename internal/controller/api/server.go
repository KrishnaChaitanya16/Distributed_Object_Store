package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/dedup"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const chunkSize = 1 * 1024 * 1024 // 1 MiB — per spec §4

// ChunkSize is the size objects are cut into, for display.
const ChunkSize = chunkSize

// gcWaitTimeout bounds how long an upload waits for a garbage collection of the same chunk to finish; tests shorten it.
var gcWaitTimeout = 10 * time.Second

// deleteSweepTimeout bounds the synchronous removal of a deleted object's chunks.
const deleteSweepTimeout = 30 * time.Second

// ReconcilerControl is the part of the reconciler the HTTP API exposes; *gc.Reconciler satisfies it.
type ReconcilerControl interface {
	Trigger() bool
	Status() gc.Status
}

// Server wires together all controller dependencies and serves the REST API.
type Server struct {
	meta      models.MetadataRepository
	placement models.PlacementStrategy
	workers   models.WorkerClientPool
	health    models.HealthMonitor
	mux       *http.ServeMux
	http      *http.Server
	hedgeMs   int // milliseconds before hedging to the secondary; 0 disables hedging
	bloom     *dedup.Filter
	collector *gc.Collector
	recon     ReconcilerControl
}

func New(
	addr string,
	meta models.MetadataRepository,
	placement models.PlacementStrategy,
	workers models.WorkerClientPool,
	health models.HealthMonitor,
) *Server {
	hedgeMs := 25 // default: hedge after 25 ms
	if v := os.Getenv("HEDGE_THRESHOLD_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			hedgeMs = n
		}
	}

	s := &Server{
		meta:      meta,
		placement: placement,
		workers:   workers,
		health:    health,
		hedgeMs:   hedgeMs,
		bloom:     dedup.New(1_000_000, 0.01), // 1 M chunks, 1 % false-positive rate
		collector: gc.NewCollector(meta, workers, health),
	}

	mux := http.NewServeMux()
	// Client-facing API — §5
	mux.HandleFunc("POST /api/v1/objects", s.instrument("upload", s.handleUpload))
	mux.HandleFunc("GET /api/v1/objects", s.instrument("list", s.handleList))
	mux.HandleFunc("GET /api/v1/objects/{id}", s.instrument("download", s.handleDownload))
	mux.HandleFunc("GET /api/v1/objects/{id}/metadata", s.instrument("get_metadata", s.handleGetMetadata))
	mux.HandleFunc("PATCH /api/v1/objects/{id}/metadata", s.instrument("patch_metadata", s.handlePatchMetadata))
	mux.HandleFunc("DELETE /api/v1/objects/{id}", s.instrument("delete", s.handleDelete))

	// Internal worker API — §6
	mux.HandleFunc("POST /internal/workers/register", s.instrument("register_worker", s.handleRegisterWorker))
	mux.HandleFunc("POST /internal/workers/heartbeat", s.instrument("heartbeat", s.handleHeartbeat))

	// Prometheus metrics scrape endpoint
	mux.Handle("GET /metrics", promhttp.Handler())

	s.mux = mux
	s.http = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}
	return s
}

// instrument wraps a handler with Prometheus request counting and latency recording.
func (s *Server) instrument(name string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next(rw, r)
		dur := time.Since(start).Seconds()
		code := fmt.Sprintf("%d", rw.code)
		metrics.HTTPRequests.WithLabelValues(name, r.Method, code).Inc()
		metrics.HTTPDuration.WithLabelValues(name, r.Method).Observe(dur)
	}
}

// statusWriter captures the HTTP status code written by a handler.
type statusWriter struct {
	http.ResponseWriter
	code    int
	written bool
}

func (sw *statusWriter) WriteHeader(code int) {
	if !sw.written {
		sw.code = code
		sw.written = true
	}
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(b []byte) (int, error) {
	if !sw.written {
		sw.written = true
	}
	return sw.ResponseWriter.Write(b)
}

// WithReconciler exposes the reconciler's status and manual trigger; call it at most once, before serving.
func (s *Server) WithReconciler(rc ReconcilerControl) *Server {
	s.recon = rc
	s.mux.HandleFunc("GET /internal/gc/status", s.instrument("gc_status", s.handleGCStatus))
	s.mux.HandleFunc("POST /internal/gc/run", s.instrument("gc_run", s.handleGCRun))
	return s
}

func (s *Server) handleGCStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.recon.Status()) //nolint:errcheck
}

// handleGCRun queues a pass and returns at once; queued is false when one was already waiting.
func (s *Server) handleGCRun(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]bool{"queued": s.recon.Trigger()}) //nolint:errcheck
}

// HedgeMs is the hedged-read threshold in milliseconds; 0 means hedging is off.
func (s *Server) HedgeMs() int { return s.hedgeMs }

// Bloom exposes the dedup filter so the web UI can show what it has seen.
func (s *Server) Bloom() *dedup.Filter { return s.bloom }

// Handler returns the HTTP mux. Used in tests to wrap the server in httptest.NewServer.
func (s *Server) Handler() http.Handler { return s.mux }

// PopulateBloom pre-loads all DURABLE chunk IDs from bbolt into the bloom filter.
// Call once after New(), before Start().
func (s *Server) PopulateBloom(ctx context.Context) {
	chunks, err := s.meta.ListChunksByState(ctx, models.ChunkDurable)
	if err != nil {
		return
	}
	for _, c := range chunks {
		s.bloom.Add(c.ChunkID)
	}
}

func (s *Server) Start() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
