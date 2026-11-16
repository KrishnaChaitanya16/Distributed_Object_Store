// Package web serves the demo UI: a single-page app plus the JSON and event-stream endpoints it reads.
package web

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/dedup"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

//go:embed static
var staticFiles embed.FS

// Ring is the part of the hash ring the UI reads; *placement.Ring satisfies it.
type Ring interface {
	Workers() []string
	Snapshot() []placement.Point
	Locate(key string) placement.Walk
}

// Health is the lease table the UI reads; *health.LeaseTable satisfies it.
type Health interface {
	Snapshot() map[string]health.WorkerLease
}

// Bloom is the dedup filter the UI reads; *dedup.Filter satisfies it.
type Bloom interface {
	Stats(cells int) dedup.Stats
	Probe(chunkID string, cells int) dedup.Probe
}

// Reconciler is the garbage collector and replica reconciler; *gc.Reconciler satisfies it.
type Reconciler interface {
	Status() gc.Status
	Trigger() bool
}

// Config is the tuning shown on the UI so the numbers on screen can be explained.
type Config struct {
	ChunkSize        int `json:"chunk_size"`
	ReplicationLevel int `json:"replication_factor"`
	HedgeMs          int `json:"hedge_ms"`
	LeaseTimeoutS    int `json:"lease_timeout_s"`
	HeartbeatS       int `json:"heartbeat_interval_s"`
	GCIntervalS      int `json:"gc_interval_s"`
	StagedTimeoutMin int `json:"staged_timeout_min"`
	OrphanGraceS     int `json:"orphan_grace_s"`
}

// Deps are the controller components the UI observes.
type Deps struct {
	Meta         models.MetadataRepository
	Workers      models.WorkerClientPool
	Ring         Ring
	Health       Health
	Bloom        Bloom
	Recon        Reconciler
	API          http.Handler
	Bus          *events.Bus
	Demo         *Docker
	HTTP         *http.Client
	Config       Config
	Started      time.Time
	InventoryTTL time.Duration

	// StaticDir serves the UI files from disk instead of the copy built into the binary; it is for developing the UI.
	StaticDir string
}

// Server is the UI's HTTP handler plus its background observers.
type Server struct {
	d   Deps
	inv *inventoryCache
	mux *http.ServeMux

	demoMu  sync.Mutex
	demoAt  time.Time
	demoVal demoView
}

// New builds the UI server; missing optional dependencies get safe defaults.
func New(d Deps) *Server {
	if d.Bus == nil {
		d.Bus = events.Default
	}
	if d.HTTP == nil {
		d.HTTP = &http.Client{Timeout: 1500 * time.Millisecond}
	}
	if d.Started.IsZero() {
		d.Started = time.Now()
	}
	if d.InventoryTTL <= 0 {
		d.InventoryTTL = time.Second
	}
	s := &Server{d: d}
	s.inv = newInventoryCache(s, d.InventoryTTL)
	s.mux = http.NewServeMux()
	s.routes()
	return s
}

// Handler returns the UI's HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	m := s.mux
	if s.d.API != nil {
		m.Handle("/api/", s.d.API)
	}
	m.HandleFunc("GET /ui/api/state", s.handleState)
	m.HandleFunc("GET /ui/api/objects", s.handleObjects)
	m.HandleFunc("GET /ui/api/events", s.handleEvents)
	m.HandleFunc("GET /ui/api/events/stream", s.handleStream)
	m.HandleFunc("GET /ui/api/ring/locate", s.handleLocate)
	m.HandleFunc("GET /ui/api/bloom/probe", s.handleProbe)
	m.HandleFunc("POST /ui/api/demo/worker/{id}/{action}", s.handleDemoWorker)
	m.HandleFunc("POST /ui/api/demo/corrupt", s.handleDemoCorrupt)
	m.HandleFunc("POST /ui/api/demo/scrub/{id}", s.handleDemoScrub)
	m.HandleFunc("POST /ui/api/demo/reconcile", s.handleDemoReconcile)
	m.HandleFunc("/ui/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "no such UI endpoint")
	})
	var sub fs.FS
	if s.d.StaticDir != "" {
		sub = os.DirFS(s.d.StaticDir)
	} else {
		var err error
		if sub, err = fs.Sub(staticFiles, "static"); err != nil {
			panic(err)
		}
	}
	files := http.FileServerFS(sub)
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, http.StatusMethodNotAllowed, "static files are read-only")
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if strings.HasSuffix(r.URL.Path, ".js") {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		}
		files.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
