package api

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const chunkSize = 1 * 1024 * 1024 // 1 MiB — per spec §4

// Server wires together all controller dependencies and serves the REST API.
type Server struct {
	meta      models.MetadataRepository
	placement models.PlacementStrategy
	workers   models.WorkerClientPool
	health    models.HealthMonitor
	mux       *http.ServeMux
	http      *http.Server
	hedgeMs   int // milliseconds before hedging to the secondary; 0 disables hedging
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
	}

	mux := http.NewServeMux()
	// Client-facing API — §5
	mux.HandleFunc("POST /api/v1/objects", s.handleUpload)
	mux.HandleFunc("GET /api/v1/objects", s.handleList)
	mux.HandleFunc("GET /api/v1/objects/{id}", s.handleDownload)
	mux.HandleFunc("GET /api/v1/objects/{id}/metadata", s.handleGetMetadata)
	mux.HandleFunc("PATCH /api/v1/objects/{id}/metadata", s.handlePatchMetadata)
	mux.HandleFunc("DELETE /api/v1/objects/{id}", s.handleDelete)

	// Internal worker API — §6
	mux.HandleFunc("POST /internal/workers/register", s.handleRegisterWorker)
	mux.HandleFunc("POST /internal/workers/heartbeat", s.handleHeartbeat)

	s.mux = mux
	s.http = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}
	return s
}

// Handler returns the HTTP mux. Used in tests to wrap the server in httptest.NewServer.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) Start() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
