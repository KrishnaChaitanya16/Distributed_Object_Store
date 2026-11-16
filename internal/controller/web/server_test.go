package web

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func sha(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// --- static files and routing ---

func TestIndexIsServedAsHTML(t *testing.T) {
	f := newFixture(t)
	code, h, body := f.get("/")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, h.Get("Content-Type"), "text/html")
	assert.Equal(t, "no-cache", h.Get("Cache-Control"), "the UI must never be served stale during development")
	assert.Equal(t, "nosniff", h.Get("X-Content-Type-Options"))
	assert.Contains(t, string(body), "<title>")
}

func TestUnknownStaticPathsAre404(t *testing.T) {
	f := newFixture(t)
	for _, p := range []string{"/nope.html", "/js/missing.js", "/static/../secret", "/%2e%2e/%2e%2e/etc/passwd", "/..%2f..%2fetc/passwd"} {
		code, _, _ := f.get(p)
		assert.NotEqual(t, http.StatusOK, code, p)
	}
}

func TestOnlyGetIsAllowedOnStaticFiles(t *testing.T) {
	f := newFixture(t)
	resp, err := http.Post(f.ts.URL+"/", "text/plain", nil)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestUnknownUIEndpointGivesJSON404(t *testing.T) {
	f := newFixture(t)
	code, h, body := f.get("/ui/api/nothing-here")
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "application/json", h.Get("Content-Type"))
	assert.Contains(t, string(body), "error")
}

func TestPublicAPIIsServedUnderApiOnTheSameOrigin(t *testing.T) {
	f := newFixture(t)
	code, h, _ := f.get("/api/v1/objects")
	assert.Equal(t, http.StatusTeapot, code, "requests under /api/ reach the REST API handler")
	assert.Equal(t, "api", h.Get("X-From"))
}

func TestStateEndpointsNeverCache(t *testing.T) {
	f := newFixture(t)
	for _, p := range []string{"/ui/api/state", "/ui/api/objects", "/ui/api/events"} {
		_, h, _ := f.get(p)
		assert.Equal(t, "no-store", h.Get("Cache-Control"), p)
		assert.Equal(t, "application/json", h.Get("Content-Type"), p)
	}
}

// --- state ---

func TestStateListsEveryWorkerWithItsHealthAndVnodes(t *testing.T) {
	f := newFixture(t)
	st := f.state()
	assert.Equal(t, []string{"worker1", "worker2", "worker3", "worker4"}, sortedIDs(st.Workers))
	assert.Equal(t, placement.VnodesPerWorker(), st.VnodesPerNode)
	for _, w := range st.Workers {
		assert.Equal(t, "ONLINE", w.Health, w.ID)
		assert.True(t, w.InRing, w.ID)
		assert.Len(t, w.Vnodes, placement.VnodesPerWorker(), w.ID)
		assert.GreaterOrEqual(t, w.HeartbeatAgeMs, int64(1400), w.ID)
		assert.NotNil(t, w.LastSeen, w.ID)
		assert.True(t, w.InventoryOK, w.ID)
		assert.NotNil(t, w.Chunks, "never null so the UI can iterate")
	}
	assert.Equal(t, 4, st.Cluster.WorkersOnline)
	assert.Equal(t, 4, st.Cluster.WorkersTotal)
	assert.Equal(t, 1<<20, st.Config.ChunkSize)
	assert.False(t, st.Demo.Enabled)
	assert.GreaterOrEqual(t, st.UptimeS, int64(0))
}

func TestStateShowsAnOfflineWorkerAsNotInTheRingAndWithoutInventory(t *testing.T) {
	f := newFixture(t)
	f.pool.set("worker2", sha("x"))
	f.health.set("worker2", models.WorkerOffline)
	f.ring.RemoveWorker("worker2")
	w := f.workerView(f.state(), "worker2")
	assert.Equal(t, "OFFLINE", w.Health)
	assert.False(t, w.InRing)
	assert.False(t, w.InventoryOK, "an offline worker is not asked what it holds")
	assert.Empty(t, w.Chunks)
	assert.Len(t, w.Vnodes, placement.VnodesPerWorker(), "its vnodes are still reported so the UI can draw them as ghosts")
	assert.Equal(t, 3, f.state().Cluster.WorkersOnline)
}

func TestStateShowsWorkersTheLeaseTableHasNeverSeenAsUnknown(t *testing.T) {
	f := newFixture(t)
	f.health.mu.Lock()
	delete(f.health.m, "worker3")
	f.health.mu.Unlock()
	w := f.workerView(f.state(), "worker3")
	assert.Equal(t, "UNKNOWN", w.Health)
	assert.Equal(t, int64(-1), w.HeartbeatAgeMs)
	assert.Nil(t, w.LastSeen)
}

func TestStateReportsInventoryAndPassesScrubberThrough(t *testing.T) {
	f := newFixture(t)
	a, b := sha("a"), sha("b")
	f.pool.set("worker1", b, a)
	f.workers["worker1"].scrubber.Store(map[string]any{"passes": 7, "corrupt_quarantined": 2})
	w := f.workerView(f.state(), "worker1")
	assert.Equal(t, 2, w.ChunkCount)
	want := []string{a, b}
	sort.Strings(want)
	assert.Equal(t, want, w.Chunks, "chunk ids are sorted")
	assert.EqualValues(t, 7, w.Scrubber["passes"])
	assert.EqualValues(t, 2, w.Scrubber["corrupt_quarantined"])
}

func TestStateSurvivesAWorkerWhoseInventoryCallFails(t *testing.T) {
	f := newFixture(t)
	f.pool.fail["worker4"] = fmt.Errorf("connection refused")
	st := f.state()
	w := f.workerView(st, "worker4")
	assert.False(t, w.InventoryOK)
	assert.Contains(t, w.InventoryError, "refused")
	assert.True(t, f.workerView(st, "worker1").InventoryOK, "one bad worker does not hide the others")
}

func TestStateToleratesAWorkerWithoutAScrubber(t *testing.T) {
	f := newFixture(t)
	f.workers["worker1"].ts.Close()
	w := f.workerView(f.state(), "worker1")
	assert.Nil(t, w.Scrubber)
}

func TestStateCountsObjectsAndChunksByState(t *testing.T) {
	f := newFixture(t)
	f.object("o1", models.StatusCommitted, 0, sha("a"))
	f.object("o2", models.StatusStaged, 0)
	f.object("o3", models.StatusFailed, 0)
	f.chunk(sha("a"), models.ChunkDurable, "worker1", "worker2")
	f.chunk(sha("b"), models.ChunkPending, "worker3")
	f.chunk(sha("c"), models.ChunkPendingDelete, "worker3")
	cl := f.state().Cluster
	assert.Equal(t, map[string]int{"COMMITTED": 1, "STAGED": 1, "FAILED": 1}, cl.Objects)
	assert.Equal(t, 3, cl.Chunks.Total)
	assert.Equal(t, map[string]int{"DURABLE": 1, "PENDING": 1, "PENDING_DELETE": 1}, cl.Chunks.ByState)
}

func TestChunkHealthIsJudgedFromWhatWorkersReallyHold(t *testing.T) {
	f := newFixture(t)
	healthy, degraded, gone, extra := sha("healthy"), sha("degraded"), sha("gone"), sha("extra")
	f.chunk(healthy, models.ChunkDurable, "worker1", "worker2")
	f.chunk(degraded, models.ChunkDurable, "worker1", "worker2")
	f.chunk(gone, models.ChunkDurable, "worker1", "worker2")
	f.chunk(extra, models.ChunkDurable, "worker1", "worker2")
	f.pool.set("worker1", healthy, degraded, extra)
	f.pool.set("worker2", healthy, extra)
	f.pool.set("worker3", extra)

	ch := f.state().Cluster.Chunks
	assert.Equal(t, 2, ch.Healthy, "healthy and extra each have both listed copies")
	assert.Equal(t, 1, ch.Degraded, "one listed copy is missing")
	assert.Equal(t, 1, ch.Unavailable, "no listed copy is reachable")
	assert.Equal(t, 1, ch.ExtraCopies, "worker3 holds a copy the record does not list")
}

func TestChunksOnAnOfflineWorkerCountAsUnreachableNotAsLost(t *testing.T) {
	f := newFixture(t)
	c := sha("c")
	f.chunk(c, models.ChunkDurable, "worker1", "worker2")
	f.pool.set("worker1", c)
	f.pool.set("worker2", c)
	assert.Equal(t, 1, f.state().Cluster.Chunks.Healthy)
	f.health.set("worker2", models.WorkerOffline)
	ch := f.state().Cluster.Chunks
	assert.Equal(t, 0, ch.Healthy)
	assert.Equal(t, 1, ch.Degraded, "only worker1's copy can be reached")
}

func TestStateIncludesBloomGCAndEventSummary(t *testing.T) {
	f := newFixture(t)
	f.bloom.Add("x")
	f.bus.Emit("upload.start", "hi")
	f.bus.Emit("upload.start", "hi")
	st := f.state()
	b, ok := st.Bloom.(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 1, b["items"])
	assert.Len(t, b["cells"], bloomCells)
	assert.EqualValues(t, 4, st.GC.Passes)
	assert.EqualValues(t, 2, st.Events.LastSeq)
	assert.EqualValues(t, 2, st.Events.Counts["upload.start"])
}

func TestStateOfAnEmptyCluster(t *testing.T) {
	f := newFixture(t)
	empty := New(Deps{Meta: f.store, Workers: f.pool, Ring: placement.NewRing(), Health: &fakeHealth{m: nil}, Bloom: f.bloom, Bus: f.bus})
	st, err := empty.buildState(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, st.Workers)
	assert.Equal(t, 4, len(st.Workers), "registered workers are listed even before they ever heartbeat")
	for _, w := range st.Workers {
		assert.Equal(t, "UNKNOWN", w.Health)
		assert.False(t, w.InRing)
	}
}

// --- inventory cache ---

func TestInventoryIsCachedForItsTTL(t *testing.T) {
	f := newFixture(t)
	f.srv.inv.ttl = time.Hour
	f.state()
	n := f.pool.calls.Load()
	require.EqualValues(t, 4, n, "one ListChunks per online worker")
	for i := 0; i < 5; i++ {
		f.state()
	}
	assert.Equal(t, n, f.pool.calls.Load(), "many readers within the TTL share one worker query")
	f.srv.inv.forget()
	f.state()
	assert.Equal(t, n+4, f.pool.calls.Load(), "forget makes the next read fresh")
}

func TestConcurrentReadersShareOneInventoryRefresh(t *testing.T) {
	f := newFixture(t)
	f.srv.inv.ttl = time.Hour
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); f.srv.inv.get(context.Background()) }()
	}
	wg.Wait()
	assert.EqualValues(t, 4, f.pool.calls.Load())
}

// --- objects ---

func TestObjectsAreListedNewestFirstWithChunkDetail(t *testing.T) {
	f := newFixture(t)
	a, b := sha("a"), sha("b")
	f.chunk(a, models.ChunkDurable, "worker1", "worker2")
	f.chunk(b, models.ChunkDurable, "worker2", "worker3")
	f.object("old", models.StatusCommitted, 2*time.Hour, a)
	f.object("new", models.StatusCommitted, time.Minute, a, b)
	f.pool.set("worker1", a)
	f.pool.set("worker2", a, b)
	f.pool.set("worker3", b)

	var out struct{ Objects []objectView }
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/objects", &out))
	require.Len(t, out.Objects, 2)
	assert.Equal(t, "new", out.Objects[0].ID)
	assert.Equal(t, "old", out.Objects[1].ID)

	c := out.Objects[0].Chunks
	require.Len(t, c, 2)
	assert.Equal(t, a, c[0].ChunkID)
	assert.Equal(t, 0, c[0].Index)
	assert.Equal(t, placement.KeyHash(a), c[0].RingPos, "ring position is the real key hash")
	assert.Equal(t, "DURABLE", c[0].State)
	assert.Equal(t, 2, c[0].Reachable)
	require.Len(t, c[0].Replicas, 2)
	for _, r := range c[0].Replicas {
		require.NotNil(t, r.Physical)
		assert.True(t, *r.Physical)
		assert.True(t, r.Online)
	}
	assert.Empty(t, c[0].Extra)
	assert.EqualValues(t, 10, c[1].Offset-c[0].Offset)
}

func TestObjectChunkShowsMissingAndExtraCopiesAndOfflineHolders(t *testing.T) {
	f := newFixture(t)
	c := sha("c")
	f.chunk(c, models.ChunkDurable, "worker1", "worker2")
	f.object("o", models.StatusCommitted, 0, c)
	f.pool.set("worker2", c)
	f.pool.set("worker3", c)
	f.health.set("worker1", models.WorkerOffline)

	var out struct{ Objects []objectView }
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/objects", &out))
	ch := out.Objects[0].Chunks[0]
	byWorker := map[string]replicaView{}
	for _, r := range ch.Replicas {
		byWorker[r.Worker] = r
	}
	assert.False(t, byWorker["worker1"].Online)
	assert.Nil(t, byWorker["worker1"].Physical, "an offline holder's disk is unknown, not empty")
	require.NotNil(t, byWorker["worker2"].Physical)
	assert.True(t, *byWorker["worker2"].Physical)
	assert.Equal(t, []string{"worker3"}, ch.Extra, "a copy on an unlisted worker is called out")
	assert.Equal(t, 1, ch.Reachable)

	f.health.set("worker1", models.WorkerOnline)
	f.pool.set("worker1")
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/objects", &out))
	for _, r := range out.Objects[0].Chunks[0].Replicas {
		if r.Worker == "worker1" {
			require.NotNil(t, r.Physical)
			assert.False(t, *r.Physical, "an online worker that lacks the copy is shown as missing it")
		}
	}
}

func TestObjectWithAMissingChunkRecordIsFlagged(t *testing.T) {
	f := newFixture(t)
	f.object("o", models.StatusCommitted, 0, sha("no-record"))
	var out struct{ Objects []objectView }
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/objects", &out))
	ch := out.Objects[0].Chunks[0]
	assert.True(t, ch.Missing)
	assert.Equal(t, "NO_RECORD", ch.State)
	assert.NotNil(t, ch.Replicas)
}

func TestObjectsLimitValidationAndTruncation(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 5; i++ {
		f.object(fmt.Sprintf("o%d", i), models.StatusCommitted, time.Duration(i)*time.Minute)
	}
	var out struct{ Objects []objectView }
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/objects?limit=2", &out))
	assert.Len(t, out.Objects, 2)
	assert.Equal(t, "o0", out.Objects[0].ID)
	for _, bad := range []string{"0", "-1", "abc", "1.5", ""} {
		code := f.getJSON("/ui/api/objects?limit="+bad, nil)
		if bad == "" {
			assert.Equal(t, http.StatusOK, code, "no limit means the default")
		} else {
			assert.Equal(t, http.StatusBadRequest, code, bad)
		}
	}
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/objects?limit=1000000", &out))
	assert.Len(t, out.Objects, 5, "a huge limit is capped, not an error")
}

func TestNoObjectsGivesAnEmptyArrayNotNull(t *testing.T) {
	f := newFixture(t)
	_, _, body := f.get("/ui/api/objects")
	assert.JSONEq(t, `{"objects":[]}`, string(body))
}

// --- locate and probe ---

func TestLocateHashesTextLikeAChunk(t *testing.T) {
	f := newFixture(t)
	var out struct {
		ChunkID string         `json:"chunk_id"`
		Walk    placement.Walk `json:"walk"`
	}
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/ring/locate?text=hello", &out))
	assert.Equal(t, sha("hello"), out.ChunkID)
	want := f.ring.Locate(sha("hello"))
	assert.Equal(t, want, out.Walk)
	assert.NotEmpty(t, out.Walk.Primary)
	assert.NotEqual(t, out.Walk.Primary, out.Walk.Secondary)
}

func TestLocateAcceptsAChunkIdAndEmptyText(t *testing.T) {
	f := newFixture(t)
	var out struct {
		ChunkID string `json:"chunk_id"`
	}
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/ring/locate?chunk="+sha("x"), &out))
	assert.Equal(t, sha("x"), out.ChunkID)
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/ring/locate?text=", &out), "empty text is still a valid input to hash")
	assert.Equal(t, sha(""), out.ChunkID)
}

func TestLocateRejectsMissingOrOversizedInput(t *testing.T) {
	f := newFixture(t)
	assert.Equal(t, http.StatusBadRequest, f.getJSON("/ui/api/ring/locate", nil))
	long := make([]byte, 200)
	for i := range long {
		long[i] = 'a'
	}
	assert.Equal(t, http.StatusBadRequest, f.getJSON("/ui/api/ring/locate?chunk="+string(long), nil))
}

func TestLocateOnAnEmptyRingGivesNoSteps(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{"worker1", "worker2", "worker3", "worker4"} {
		f.ring.RemoveWorker(id)
	}
	var out struct {
		Walk placement.Walk `json:"walk"`
	}
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/ring/locate?text=x", &out))
	assert.Empty(t, out.Walk.Steps)
	assert.Equal(t, "", out.Walk.Primary)
}

func TestProbeShowsBitsAndWhetherTheStoreHasTheChunk(t *testing.T) {
	f := newFixture(t)
	known, absent := sha("known"), sha("absent")
	f.bloom.Add(known)
	f.chunk(known, models.ChunkDurable, "worker1", "worker2")
	var out struct {
		Probe   map[string]any `json:"probe"`
		InStore bool           `json:"in_store"`
	}
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/bloom/probe?chunk="+known, &out))
	assert.Equal(t, true, out.Probe["might_exist"])
	assert.True(t, out.InStore)
	assert.NotEmpty(t, out.Probe["positions"])
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/bloom/probe?chunk="+absent, &out))
	assert.Equal(t, false, out.Probe["might_exist"])
	assert.False(t, out.InStore)
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/bloom/probe?text=known", &out))
	assert.Equal(t, known, out.Probe["chunk_id"], "text is hashed the way a chunk id is")
	assert.Equal(t, http.StatusBadRequest, f.getJSON("/ui/api/bloom/probe", nil))
}

func TestStaticDirServesFilesFromDiskInsteadOfTheBuiltInCopy(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.html"), []byte("<title>from disk</title>"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.js"), []byte("export const x = 1;"), 0o644))
	f := newFixture(t)
	f.srv = New(Deps{Meta: f.store, Workers: f.pool, Ring: f.ring, Health: f.health, Bloom: f.bloom, Bus: f.bus, StaticDir: dir})
	ts := httptest.NewServer(f.srv.Handler())
	defer ts.Close()
	f.ts = ts
	code, _, body := f.get("/")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), "from disk")
	code, h, _ := f.get("/x.js")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, h.Get("Content-Type"), "text/javascript")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.html"), []byte("<title>edited</title>"), 0o644))
	_, _, body = f.get("/")
	assert.Contains(t, string(body), "edited", "edits show up without a restart")
	code, _, _ = f.get("/js/not-there.js")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestScriptsAreServedWithAJavaScriptContentType(t *testing.T) {
	f := newFixture(t)
	for _, p := range []string{"/js/main.js", "/js/util.js", "/js/views/flow.js"} {
		code, h, _ := f.get(p)
		assert.Equal(t, http.StatusOK, code, p)
		assert.Contains(t, h.Get("Content-Type"), "text/javascript", p+" must not be sent as text/plain or modules refuse to load")
	}
	code, h, _ := f.get("/css/app.css")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, h.Get("Content-Type"), "text/css")
}

func TestOnlineWorkersWhoseDiskCannotBeReadAreCountedAsUnscanned(t *testing.T) {
	f := newFixture(t)
	assert.Equal(t, 0, f.state().Cluster.WorkersUnscanned)
	f.pool.fail["worker2"] = fmt.Errorf("circuit breaker open")
	f.health.set("worker3", models.WorkerOffline)
	cl := f.state().Cluster
	assert.Equal(t, 1, cl.WorkersUnscanned, "worker2 is online but unreadable; offline worker3 is simply down, not unscanned")
	assert.Equal(t, 3, cl.WorkersOnline)
}
