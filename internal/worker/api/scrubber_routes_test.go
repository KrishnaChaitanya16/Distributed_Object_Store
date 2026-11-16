package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/scrubber"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
)

type scrubWorker struct {
	addr  string
	url   string
	root  string
	store *storage.Store
	scrub *scrubber.Scrubber
}

// startScrubWorker runs a real worker with a real scrubber whose interval is long enough that only triggers cause passes.
func startScrubWorker(t *testing.T, id string) *scrubWorker {
	t.Helper()
	root := t.TempDir()
	st, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := scrubber.New(st, scrubber.Config{Interval: time.Hour, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go sc.Run(ctx)
	srv := httptest.NewServer(nil)
	srv.Config.Handler = api.New(st, id, srv.Listener.Addr().String(), srv.Client()).WithScrubber(sc).Handler()
	t.Cleanup(func() { cancel(); srv.Close() })
	return &scrubWorker{addr: srv.Listener.Addr().String(), url: srv.URL, root: root, store: st, scrub: sc}
}

func (w *scrubWorker) chunkPath(id string) string {
	return filepath.Join(w.root, "chunks", id+".bin")
}

func (w *scrubWorker) status(t *testing.T) scrubber.Status {
	t.Helper()
	resp, err := http.Get(w.url + "/internal/scrubber/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("status endpoint: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	var st scrubber.Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

// runPass triggers a pass over HTTP and waits until it has completed.
func (w *scrubWorker) runPass(t *testing.T) scrubber.Status {
	t.Helper()
	before := w.status(t).Passes
	resp, err := http.Post(w.url+"/internal/scrubber/run", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]bool
	json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("run endpoint: %d", resp.StatusCode)
	}
	if _, ok := body["queued"]; !ok {
		t.Fatalf("run response missing queued: %v", body)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		st := w.status(t)
		if st.Passes > before && !st.Running {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatal("triggered pass never completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func flipOnDisk(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := []byte{0}
	f.ReadAt(b, off)
	b[0] ^= 0x80
	f.WriteAt(b, off)
}

func httpCode(t *testing.T, method, url string) int {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func TestScrubberStatusBeforeAnyPass(t *testing.T) {
	w := startScrubWorker(t, "w1")
	st := w.status(t)
	if st.Passes != 0 || st.Running || st.IntervalSeconds != 3600 || st.Recent == nil || len(st.Recent) != 0 {
		t.Fatalf("status %+v", st)
	}
}

func TestScrubberRunOverHTTPCountsChunks(t *testing.T) {
	w := startScrubWorker(t, "w1")
	pool := workerclient.New()
	for i := 0; i < 3; i++ {
		data, id := randomChunk(t, 1000*(i+1))
		if err := pool.PutChunk(context.Background(), w.addr, id, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
	}
	st := w.runPass(t)
	if st.Passes != 1 || st.ChunksScanned != 3 || st.LastPassScanned != 3 || st.CorruptQuarantined != 0 || st.LastPassStarted == nil {
		t.Fatalf("status %+v", st)
	}
}

func TestScrubberRoutesMethodsAndAbsence(t *testing.T) {
	w := startScrubWorker(t, "w1")
	if c := httpCode(t, http.MethodPost, w.url+"/internal/scrubber/status"); c != http.StatusMethodNotAllowed {
		t.Fatalf("POST status: %d", c)
	}
	if c := httpCode(t, http.MethodGet, w.url+"/internal/scrubber/run"); c != http.StatusMethodNotAllowed {
		t.Fatalf("GET run: %d", c)
	}
	if c := httpCode(t, http.MethodHead, w.url+"/internal/scrubber/status"); c != http.StatusOK {
		t.Fatalf("HEAD status: %d", c)
	}

	plain, _ := startWorker(t, "w2")
	for _, r := range []struct{ method, path string }{{"GET", "/internal/scrubber/status"}, {"POST", "/internal/scrubber/run"}} {
		if c := httpCode(t, r.method, "http://"+plain+r.path); c != http.StatusNotFound {
			t.Fatalf("worker without scrubber %s %s: %d", r.method, r.path, c)
		}
	}
}

func TestScrubberRunCoalescesOverHTTP(t *testing.T) {
	st, _ := storage.New(t.TempDir())
	sc, _ := scrubber.New(st, scrubber.Config{Interval: time.Hour, Logf: func(string, ...any) {}})
	srv := httptest.NewServer(api.New(st, "w1", "x:1", nil).WithScrubber(sc).Handler())
	defer srv.Close()
	var queued []bool
	for i := 0; i < 3; i++ {
		resp, err := http.Post(srv.URL+"/internal/scrubber/run", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]bool
		json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		queued = append(queued, body["queued"])
	}
	if !queued[0] || queued[1] || queued[2] {
		t.Fatalf("without a running loop only the first trigger can queue: %v", queued)
	}
}

// TestCorruptChunkIsDetectedFallsBackAndIsRestored walks the whole integrity story across two real workers.
func TestCorruptChunkIsDetectedFallsBackAndIsRestored(t *testing.T) {
	ctx := context.Background()
	pool := workerclient.New()
	primary := startScrubWorker(t, "w1")
	secondary := startScrubWorker(t, "w2")
	data, id := randomChunk(t, storage.MaxChunkSize)
	for _, w := range []*scrubWorker{primary, secondary} {
		if err := pool.PutChunk(ctx, w.addr, id, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
	}

	flipOnDisk(t, primary.chunkPath(id), 4242)
	st := primary.runPass(t)
	if st.CorruptQuarantined != 1 || st.LastPassCorrupt != 1 || len(st.Recent) != 1 || st.Recent[0].ChunkID != id {
		t.Fatalf("primary status %+v", st)
	}
	if c := httpCode(t, http.MethodGet, primary.url+"/internal/chunks/"+id); c != http.StatusNotFound {
		t.Fatalf("quarantined chunk GET: %d", c)
	}
	inv, err := pool.ListChunks(ctx, primary.addr, "w1")
	if err != nil || len(inv) != 0 {
		t.Fatalf("inventory must drop the quarantined chunk: %v %v", inv, err)
	}

	if _, err := pool.GetChunk(ctx, primary.addr, id); err == nil {
		t.Fatal("controller fetch from primary must fail so it falls back")
	}
	rc, err := pool.GetChunk(ctx, secondary.addr, id)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("secondary copy differs")
	}
	if s2 := secondary.runPass(t); s2.CorruptQuarantined != 0 {
		t.Fatal("healthy secondary must not be quarantined")
	}

	if err := pool.ReplicatePeer(ctx, secondary.addr, id, primary.addr); err != nil {
		t.Fatalf("repair via replicate-peer: %v", err)
	}
	rc, err = pool.GetChunk(ctx, primary.addr, id)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("restored primary copy differs")
	}
	st = primary.runPass(t)
	if st.LastPassCorrupt != 0 || st.LastPassScanned != 1 || st.CorruptQuarantined != 1 {
		t.Fatalf("restored copy must pass the next scrub: %+v", st)
	}
	q, _ := os.ReadDir(filepath.Join(primary.root, "quarantine"))
	if len(q) != 1 || !strings.HasPrefix(q[0].Name(), id) {
		t.Fatalf("quarantine evidence %v", q)
	}
}

func TestCorruptSourceCannotSpreadBeforeScrub(t *testing.T) {
	ctx := context.Background()
	pool := workerclient.New()
	src := startScrubWorker(t, "w1")
	dst := startScrubWorker(t, "w2")
	data, id := randomChunk(t, 64<<10)
	pool.PutChunk(ctx, src.addr, id, bytes.NewReader(data), int64(len(data)))
	flipOnDisk(t, src.chunkPath(id), 100)
	if err := pool.ReplicatePeer(ctx, src.addr, id, dst.addr); err == nil {
		t.Fatal("replicating a corrupt copy must be rejected by the target's hash check")
	}
	if c := httpCode(t, http.MethodGet, dst.url+"/internal/chunks/"+id); c != http.StatusNotFound {
		t.Fatalf("target must not store the corrupt copy: %d", c)
	}
}

func TestScrubberStatusDuringPass(t *testing.T) {
	w := startScrubWorker(t, "w1")
	pool := workerclient.New()
	data, id := randomChunk(t, 100)
	pool.PutChunk(context.Background(), w.addr, id, bytes.NewReader(data), 100)
	for i := 0; i < 20; i++ {
		http.Post(w.url+"/internal/scrubber/run", "", nil)
		w.status(t)
	}
	deadline := time.Now().Add(3 * time.Second)
	for w.status(t).Running && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if st := w.status(t); st.Passes < 1 || st.Errors != 0 {
		t.Fatalf("status %+v", st)
	}
}
