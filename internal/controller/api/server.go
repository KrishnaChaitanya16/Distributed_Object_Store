package api

import (
	"context"
	"net/http"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const chunkSize = 4 * 1024 * 1024 // 4 MiB

// Server wires together all controller dependencies and serves the REST API.
type Server struct {
	meta      models.MetadataRepository
	placement models.PlacementStrategy
	workers   models.WorkerClientPool
	health    models.HealthMonitor
	http      *http.Server
}

func New(
	addr string,
	meta models.MetadataRepository,
	placement models.PlacementStrategy,
	workers models.WorkerClientPool,
	health models.HealthMonitor,
) *Server {
	s := &Server{
		meta:      meta,
		placement: placement,
		workers:   workers,
		health:    health,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /objects", s.handleUpload)
	mux.HandleFunc("GET /objects", s.handleList)
	mux.HandleFunc("GET /objects/{id}", s.handleDownload)
	mux.HandleFunc("DELETE /objects/{id}", s.handleDelete)

	s.http = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}
	return s
}

func (s *Server) Start() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
