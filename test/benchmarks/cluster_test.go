// Package benchmarks measures MiniDFS against the questions in docs/BENCHMARKS_SPEC.md.
// Everything is real: a controller with bbolt, workers that write chunk files to disk with fsync, and chunks travelling over loopback HTTP.
package benchmarks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	mathrand "math/rand"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controllerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	workerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/scrubber"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// replicationFactor is how many copies of every chunk MiniDFS stores.
const replicationFactor = 2

// countingPool counts the chunk bytes the controller sends to workers.
type countingPool struct {
	*workerclient.Pool
	puts     atomic.Int64
	putBytes atomic.Int64
}

func (p *countingPool) PutChunk(ctx context.Context, addr, chunkID string, data io.Reader, size int64) error {
	p.puts.Add(1)
	p.putBytes.Add(size)
	return p.Pool.PutChunk(ctx, addr, chunkID, data, size)
}

type node struct {
	id       string
	srv      *httptest.Server
	dir      string
	store    *storage.Store
	scrub    *scrubber.Scrubber
	alive    chan struct{}
	killOnce sync.Once
}

type cluster struct {
	tb      testing.TB
	ctrl    *httptest.Server
	api     *controllerapi.Server
	pool    *countingPool
	meta    *metadata.MetadataStore
	ring    *placement.Ring
	table   *health.LeaseTable
	workers []*node
	client  *http.Client
	ctx     context.Context
	slow    map[string]*slowControl
	opts    options
}

// options tunes a cluster for one benchmark.
type options struct {
	workers       int
	dedupOff      bool
	singleReplica bool
	heartbeats    bool   // workers send heartbeats and the health monitor runs, as in production
	slowWorker    string // this worker sits behind a proxy whose delay the benchmark controls
}

func newCluster(tb testing.TB, n int, dedup bool) *cluster {
	return newClusterWith(tb, options{workers: n, dedupOff: !dedup})
}

// newClusterWith starts a controller and the workers described by o.
func newClusterWith(tb testing.TB, o options) *cluster {
	tb.Helper()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	store, err := metadata.NewMetadataStore(filepath.Join(tb.TempDir(), "meta.db"))
	if err != nil {
		tb.Fatal(err)
	}
	ring := placement.NewRing()
	pool := &countingPool{Pool: workerclient.New()}
	table := health.NewLeaseTable()
	srv := controllerapi.New(":0", store, ring, pool, table)
	if o.dedupOff {
		srv.DisableDedup()
	}
	if o.singleReplica {
		srv.UseSingleReplica()
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctrl := httptest.NewServer(srv.Handler())
	c := &cluster{tb: tb, ctrl: ctrl, api: srv, pool: pool, meta: store, ring: ring, table: table, client: &http.Client{}, ctx: ctx, slow: map[string]*slowControl{}, opts: o}
	if o.heartbeats {
		health.NewMonitor(table).Start(ctx)
	}
	tb.Cleanup(func() {
		cancel()
		ctrl.Close()
		for _, w := range c.workers {
			w.srv.Close()
		}
		for _, s := range c.slow {
			s.srv.Close()
		}
		store.Close()
	})
	for i := 1; i <= o.workers; i++ {
		c.addWorker(fmt.Sprintf("worker%d", i))
	}
	return c
}

// slowControl is a proxy in front of one worker whose added delay can be changed while a benchmark runs.
type slowControl struct {
	srv      *httptest.Server
	delay    atomic.Int64
	fraction atomic.Uint64 // math.Float64bits of the share of reads that are held back
	mu       sync.Mutex
	rng      *mathrand.Rand
}

// SetDelay holds back every chunk read from the worker by d.
func (s *slowControl) SetDelay(d time.Duration) {
	s.delay.Store(int64(d))
	s.fraction.Store(math.Float64bits(1))
}

// SetIntermittent holds back a random share of chunk reads by d and serves the rest at full speed.
func (s *slowControl) SetIntermittent(d time.Duration, share float64) {
	s.delay.Store(int64(d))
	s.fraction.Store(math.Float64bits(share))
}

func (s *slowControl) shouldDelay() bool {
	share := math.Float64frombits(s.fraction.Load())
	if share >= 1 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Float64() < share
}

func newSlowProxy(target string) *slowControl {
	u, _ := url.Parse(target)
	sc := &slowControl{rng: mathrand.New(mathrand.NewSource(7))}
	sc.fraction.Store(math.Float64bits(1))
	rp := httputil.NewSingleHostReverseProxy(u)
	sc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/internal/chunks/") && sc.shouldDelay() {
			time.Sleep(time.Duration(sc.delay.Load()))
		}
		rp.ServeHTTP(w, r)
	}))
	return sc
}

func (c *cluster) addWorker(id string) {
	c.tb.Helper()
	dir := c.tb.TempDir()
	st, err := storage.New(dir)
	if err != nil {
		c.tb.Fatal(err)
	}
	// the scrubber only runs when a benchmark triggers it: its interval is far longer than any run
	sc, err := scrubber.New(st, scrubber.Config{Interval: 24 * time.Hour, Logf: func(string, ...any) {}})
	if err != nil {
		c.tb.Fatal(err)
	}
	go sc.Run(c.ctx) //nolint:errcheck
	srv := httptest.NewServer(workerapi.New(st, id, "", &http.Client{Timeout: 30 * time.Second}).WithScrubber(sc).Handler())
	advertised := srv.URL
	if id == c.opts.slowWorker {
		sc := newSlowProxy(srv.URL)
		c.slow[id] = sc
		advertised = sc.srv.URL
	}
	addr := strings.TrimPrefix(advertised, "http://")
	i := strings.LastIndex(addr, ":")
	var port int
	fmt.Sscanf(addr[i+1:], "%d", &port)
	body, _ := json.Marshal(models.RegisterRequest{WorkerID: id, Address: addr[:i], Port: port, CapacityBytes: 100 << 30})
	resp, err := http.Post(c.ctrl.URL+"/internal/workers/register", "application/json", bytes.NewReader(body))
	if err != nil {
		c.tb.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		c.tb.Fatalf("registering %s: status %d", id, resp.StatusCode)
	}
	n := &node{id: id, srv: srv, dir: dir, store: st, scrub: sc, alive: make(chan struct{})}
	c.workers = append(c.workers, n)
	if c.opts.heartbeats {
		go c.heartbeat(n)
	}
}

// heartbeat reports the worker alive every 2 seconds until it is killed.
func (c *cluster) heartbeat(n *node) {
	body, _ := json.Marshal(map[string]string{"worker_id": n.id})
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-n.alive:
			return
		case <-c.ctx.Done():
			return
		case <-t.C:
			if resp, err := http.Post(c.ctrl.URL+"/internal/workers/heartbeat", "application/json", bytes.NewReader(body)); err == nil {
				resp.Body.Close()
			}
		}
	}
}

func (c *cluster) node(id string) *node {
	for _, w := range c.workers {
		if w.id == id {
			return w
		}
	}
	c.tb.Fatalf("no worker %s", id)
	return nil
}

// kill takes a worker off the network for good: it stops heartbeating and its server goes away.
func (c *cluster) kill(id string) {
	n := c.node(id)
	n.killOnce.Do(func() {
		close(n.alive)
		n.srv.CloseClientConnections()
		n.srv.Close()
	})
}

// chunkPath is where a worker keeps a chunk file.
func (n *node) chunkPath(chunkID string) string {
	return filepath.Join(n.dir, "chunks", chunkID+".bin")
}

// uploadFile streams a file to the controller and returns how long the whole request took.
func (c *cluster) uploadFile(path string) (time.Duration, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		fw, err := mw.CreateFormFile("file", filepath.Base(path))
		if err == nil {
			_, err = io.Copy(fw, f)
		}
		if err == nil {
			err = mw.Close()
		}
		pw.CloseWithError(err)
	}()
	req, err := http.NewRequest(http.MethodPost, c.ctrl.URL+"/api/v1/objects", pr)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	start := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	el := time.Since(start)
	if resp.StatusCode != http.StatusCreated {
		return el, st.Size(), fmt.Errorf("upload %s: status %d: %s", filepath.Base(path), resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return el, st.Size(), nil
}

// diskUsage sums the chunk files on every worker's disk.
func (c *cluster) diskUsage() (bytes int64, files int) {
	for _, w := range c.workers {
		entries, err := os.ReadDir(filepath.Join(w.dir, "chunks"))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".bin") {
				continue
			}
			if info, err := e.Info(); err == nil {
				bytes += info.Size()
				files++
			}
		}
	}
	return bytes, files
}

// percentile returns the p-th percentile (0 to 100) by the nearest-rank method; an empty sample gives 0.
func percentile(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	rank := int(p/100*float64(len(s)) + 0.9999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(s) {
		rank = len(s)
	}
	return s[rank-1]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// uploadBytes uploads in-memory data and returns the new object's id.
func (c *cluster) uploadBytes(name string, data []byte) (string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", name)
	if err != nil {
		return "", err
	}
	if _, err := fw.Write(data); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}
	resp, err := c.client.Post(c.ctrl.URL+"/api/v1/objects", mw.FormDataContentType(), &buf)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("upload %s: status %d: %s", name, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		ObjectID string `json:"object_id"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", err
	}
	return out.ObjectID, nil
}

// download fetches an object and reports the status, the bytes received and how long it took.
func (c *cluster) download(id string) (int, []byte, time.Duration) {
	start := time.Now()
	resp, err := c.client.Get(c.ctrl.URL + "/api/v1/objects/" + id)
	if err != nil {
		return 0, nil, time.Since(start)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, time.Since(start)
}

// chunkInfo is where one chunk of an object lives according to the controller.
type chunkInfo struct {
	ChunkID   string   `json:"chunk_id"`
	Index     int      `json:"index"`
	Locations []string `json:"locations"`
}

func (c *cluster) objectChunks(id string) []chunkInfo {
	resp, err := c.client.Get(c.ctrl.URL + "/api/v1/objects/" + id + "/metadata")
	if err != nil {
		c.tb.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Chunks []chunkInfo `json:"chunks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.tb.Fatal(err)
	}
	return out.Chunks
}

// scrubNow runs one scrubber pass on the worker and waits for it to finish, returning how long the pass took.
func (n *node) scrubNow(tb testing.TB) time.Duration {
	tb.Helper()
	before := n.scrub.Status().Passes
	start := time.Now()
	for !n.scrub.Trigger() {
		time.Sleep(time.Millisecond)
	}
	for {
		st := n.scrub.Status()
		if st.Passes > before && !st.Running {
			return time.Since(start)
		}
		if time.Since(start) > 5*time.Minute {
			tb.Fatal("scrubber pass did not finish")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// chunkIDs lists the chunk files on the worker's disk.
func (n *node) chunkIDs() []string {
	entries, _ := os.ReadDir(filepath.Join(n.dir, "chunks"))
	var ids []string
	for _, e := range entries {
		if name := e.Name(); strings.HasSuffix(name, ".bin") {
			ids = append(ids, strings.TrimSuffix(name, ".bin"))
		}
	}
	sort.Strings(ids)
	return ids
}
