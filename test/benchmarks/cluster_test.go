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
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controllerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	workerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/api"
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
	id  string
	srv *httptest.Server
	dir string
}

type cluster struct {
	tb      testing.TB
	ctrl    *httptest.Server
	api     *controllerapi.Server
	pool    *countingPool
	workers []*node
	client  *http.Client
}

// newCluster starts a controller and n workers; dedup false switches deduplication off for the baseline runs.
func newCluster(tb testing.TB, n int, dedup bool) *cluster {
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
	if !dedup {
		srv.DisableDedup()
	}
	ctrl := httptest.NewServer(srv.Handler())
	c := &cluster{tb: tb, ctrl: ctrl, api: srv, pool: pool, client: &http.Client{}}
	tb.Cleanup(func() {
		ctrl.Close()
		for _, w := range c.workers {
			w.srv.Close()
		}
		store.Close()
	})
	for i := 1; i <= n; i++ {
		c.addWorker(fmt.Sprintf("worker%d", i))
	}
	return c
}

func (c *cluster) addWorker(id string) {
	c.tb.Helper()
	dir := c.tb.TempDir()
	st, err := storage.New(dir)
	if err != nil {
		c.tb.Fatal(err)
	}
	srv := httptest.NewServer(workerapi.New(st, id, "", &http.Client{Timeout: 30 * time.Second}).Handler())
	addr := strings.TrimPrefix(srv.URL, "http://")
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
	c.workers = append(c.workers, &node{id: id, srv: srv, dir: dir})
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
