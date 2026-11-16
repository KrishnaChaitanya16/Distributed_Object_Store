package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/dedup"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// fakePool answers ListChunks from a settable inventory and counts the calls.
type fakePool struct {
	mu    sync.Mutex
	inv   map[string][]string
	fail  map[string]error
	calls atomic.Int64
}

func (p *fakePool) PutChunk(context.Context, string, string, io.Reader, int64) error { return nil }
func (p *fakePool) GetChunk(context.Context, string, string) (io.ReadCloser, error) {
	return nil, models.ErrNotFound
}
func (p *fakePool) DeleteChunk(context.Context, string, string) error           { return nil }
func (p *fakePool) ReplicatePeer(context.Context, string, string, string) error { return nil }
func (p *fakePool) ListChunks(_ context.Context, _ string, workerID string) ([]string, error) {
	p.calls.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.fail[workerID]; err != nil {
		return nil, err
	}
	return append([]string(nil), p.inv[workerID]...), nil
}

func (p *fakePool) set(worker string, chunks ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inv[worker] = chunks
}

// fakeHealth is a settable lease table.
type fakeHealth struct {
	mu sync.Mutex
	m  map[string]health.WorkerLease
}

func (h *fakeHealth) Snapshot() map[string]health.WorkerLease {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]health.WorkerLease, len(h.m))
	for k, v := range h.m {
		out[k] = v
	}
	return out
}

func (h *fakeHealth) set(id string, st models.WorkerStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.m[id] = health.WorkerLease{Status: st, LastSeen: time.Now().Add(-1500 * time.Millisecond)}
}

type fakeRecon struct {
	triggers atomic.Int32
	queued   bool
}

func (r *fakeRecon) Status() gc.Status { return gc.Status{Passes: 4, IntervalSeconds: 30} }
func (r *fakeRecon) Trigger() bool     { r.triggers.Add(1); return r.queued }

// workerHTTP stands in for a worker's HTTP server: scrubber status and run.
type workerHTTP struct {
	ts       *httptest.Server
	scrubber atomic.Value
	runs     atomic.Int32
	runCode  atomic.Int32
}

type fixture struct {
	t       *testing.T
	store   *metadata.MetadataStore
	ring    *placement.Ring
	health  *fakeHealth
	bloom   *dedup.Filter
	bus     *events.Bus
	pool    *fakePool
	recon   *fakeRecon
	srv     *Server
	ts      *httptest.Server
	workers map[string]*workerHTTP
}

func newFixture(t *testing.T, ids ...string) *fixture {
	t.Helper()
	if len(ids) == 0 {
		ids = []string{"worker1", "worker2", "worker3", "worker4"}
	}
	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	f := &fixture{
		t: t, store: store, ring: placement.NewRing(), bloom: dedup.New(1000, 0.01), bus: events.New(500),
		health:  &fakeHealth{m: map[string]health.WorkerLease{}},
		pool:    &fakePool{inv: map[string][]string{}, fail: map[string]error{}},
		recon:   &fakeRecon{queued: true},
		workers: map[string]*workerHTTP{},
	}
	for _, id := range ids {
		w := &workerHTTP{}
		w.scrubber.Store(map[string]any{"passes": 3, "corrupt_quarantined": 0, "recent_corruption": []any{}})
		w.runCode.Store(http.StatusAccepted)
		mux := http.NewServeMux()
		mux.HandleFunc("GET /internal/scrubber/status", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			json.NewEncoder(rw).Encode(w.scrubber.Load()) //nolint:errcheck
		})
		mux.HandleFunc("POST /internal/scrubber/run", func(rw http.ResponseWriter, r *http.Request) {
			w.runs.Add(1)
			rw.WriteHeader(int(w.runCode.Load()))
		})
		w.ts = httptest.NewServer(mux)
		t.Cleanup(w.ts.Close)
		u, _ := url.Parse(w.ts.URL)
		port, _ := strconv.Atoi(u.Port())
		require.NoError(t, store.SaveWorker(context.Background(), &models.WorkerRecord{WorkerID: id, Address: u.Hostname(), Port: port, Status: models.WorkerOnline}))
		f.workers[id] = w
		f.health.set(id, models.WorkerOnline)
		f.ring.AddWorker(id, u.Host)
	}
	f.build(nil)
	return f
}

// build (re)creates the UI server, optionally with a Docker client.
func (f *fixture) build(demo *Docker) {
	if f.ts != nil {
		f.ts.Close()
	}
	f.srv = New(Deps{
		Meta: f.store, Workers: f.pool, Ring: f.ring, Health: f.health, Bloom: f.bloom, Recon: f.recon, Bus: f.bus, Demo: demo,
		Config: Config{ChunkSize: 1 << 20, ReplicationLevel: 2, HedgeMs: 25}, InventoryTTL: time.Millisecond,
		API: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-From", "api")
			w.WriteHeader(http.StatusTeapot)
		}),
	})
	f.srv.inv.ttl = 0
	f.ts = httptest.NewServer(f.srv.Handler())
	f.t.Cleanup(f.ts.Close)
}

func (f *fixture) get(path string) (int, http.Header, []byte) {
	f.t.Helper()
	resp, err := http.Get(f.ts.URL + path)
	require.NoError(f.t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func (f *fixture) getJSON(path string, v any) int {
	f.t.Helper()
	code, _, body := f.get(path)
	if v != nil && code == http.StatusOK {
		require.NoError(f.t, json.Unmarshal(body, v), string(body))
	}
	return code
}

func (f *fixture) state() stateView {
	f.t.Helper()
	var st stateView
	require.Equal(f.t, http.StatusOK, f.getJSON("/ui/api/state", &st))
	return st
}

func (f *fixture) workerView(st stateView, id string) workerView {
	f.t.Helper()
	for _, w := range st.Workers {
		if w.ID == id {
			return w
		}
	}
	f.t.Fatalf("worker %s not in state", id)
	return workerView{}
}

func (f *fixture) chunk(id string, state models.ChunkState, workers ...string) {
	f.t.Helper()
	reps := make([]models.Replica, len(workers))
	for i, w := range workers {
		reps[i] = models.Replica{WorkerID: w, WorkerAddr: w + ":1"}
	}
	require.NoError(f.t, f.store.SaveChunk(context.Background(), &models.ChunkRecord{ChunkID: id, State: state, Replicas: reps, RefCount: 1}))
}

func (f *fixture) object(id string, status models.ObjectStatus, age time.Duration, chunks ...string) {
	f.t.Helper()
	ctx := context.Background()
	require.NoError(f.t, f.store.SaveObject(ctx, &models.ObjectManifest{ID: id, Name: id + ".bin", Status: status, SizeBytes: 100, ChunkCount: len(chunks), CreatedAt: time.Now().Add(-age).UTC(), ModifiedAt: time.Now().UTC()}))
	refs := make([]models.ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = models.ChunkRef{ChunkID: c, Index: i, Offset: int64(i) * 10, Size: 10, PrimaryID: "worker1", SecondaryID: "worker2"}
	}
	require.NoError(f.t, f.store.SaveChunkRefs(ctx, id, refs))
}

func sortedIDs(ws []workerView) []string {
	ids := make([]string, len(ws))
	for i, w := range ws {
		ids[i] = w.ID
	}
	sort.Strings(ids)
	return ids
}
