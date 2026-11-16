package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func chunkIDOf(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func rawUpload(t *testing.T, srv *Server, content string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "f.bin")
	require.NoError(t, err)
	io.WriteString(fw, content)
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func (cs *chunkStore) has(addr, id string) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	_, ok := cs.chunks[cs.key(addr, id)]
	return ok
}

func (cs *chunkStore) move(id, from, to string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.chunks[cs.key(to, id)] = cs.chunks[cs.key(from, id)]
	delete(cs.chunks, cs.key(from, id))
}

func addrOfID(id string) string { return "localhost:900" + strings.TrimPrefix(id, "w") }

// hookedPool lets a test act in the middle of an upload.
type hookedPool struct {
	*chunkStore
	onPut func()
}

func (p *hookedPool) PutChunk(ctx context.Context, addr, id string, data io.Reader, size int64) error {
	if p.onPut != nil {
		p.onPut()
	}
	return p.chunkStore.PutChunk(ctx, addr, id, data, size)
}

// --- upload ---

func TestUpload_PendingChunkFromFailedUploadIsPushedAgainNotDeduped(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	content := "bytes that a crashed upload never wrote"
	id := chunkIDOf(content)
	require.NoError(t, h.store.SaveChunk(ctx, &models.ChunkRecord{ChunkID: id, State: models.ChunkPending, Replicas: []models.Replica{{WorkerID: "w1"}, {WorkerID: "w2"}}}))

	r := h.upload(t, "x", content)
	assert.Len(t, h.workers.callsFor("PUT", id), 2, "a PENDING chunk may have no bytes anywhere; dedup would point the object at nothing")
	code, body := h.download(t, r.ObjectID)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
	rec, _ := h.store.GetChunk(ctx, id)
	assert.Equal(t, models.ChunkDurable, rec.State)
}

func TestUpload_DurableChunkIsDeduped(t *testing.T) {
	h := newHarness(t)
	h.upload(t, "a", "same")
	before := len(h.workers.callsFor("PUT", ""))
	r := h.upload(t, "b", "same")
	assert.Equal(t, before, len(h.workers.callsFor("PUT", "")), "no bytes move for a duplicate")
	_, body := h.download(t, r.ObjectID)
	assert.Equal(t, "same", string(body))
}

func TestUpload_ReuploadAfterDeleteRewritesBytes(t *testing.T) {
	h := newHarness(t)
	content := "come back"
	r := h.upload(t, "a", content)
	require.Equal(t, http.StatusNoContent, h.deleteObject(t, r.ObjectID))
	_, err := h.store.GetChunk(context.Background(), chunkIDOf(content))
	require.ErrorIs(t, err, models.ErrNotFound, "delete removes the record synchronously")

	r2 := h.upload(t, "b", content)
	code, body := h.download(t, r2.ObjectID)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
}

func TestUpload_WaitsForGarbageCollectionOfSameChunk(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	content := "being deleted right now"
	id := chunkIDOf(content)
	require.NoError(t, h.store.SaveChunk(ctx, &models.ChunkRecord{ChunkID: id, State: models.ChunkPendingDelete, Replicas: []models.Replica{{WorkerID: "w1"}}}))
	go func() {
		time.Sleep(150 * time.Millisecond)
		h.store.RemoveDeletedChunk(ctx, id) //nolint:errcheck
	}()
	start := time.Now()
	r := h.upload(t, "x", content)
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "upload must not dedup onto a chunk that is being deleted")
	assert.Len(t, h.workers.callsFor("PUT", id), 2)
	_, body := h.download(t, r.ObjectID)
	assert.Equal(t, content, string(body))
}

func TestUpload_GiveUpWhenGarbageCollectionNeverFinishes(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	old := gcWaitTimeout
	gcWaitTimeout = 100 * time.Millisecond
	t.Cleanup(func() { gcWaitTimeout = old })
	content := "stuck"
	id := chunkIDOf(content)
	require.NoError(t, h.store.SaveChunk(ctx, &models.ChunkRecord{ChunkID: id, State: models.ChunkPendingDelete}))
	rec := rawUpload(t, h.srv, content)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Empty(t, h.workers.callsFor("PUT", id))
	live, _ := h.store.LiveChunkRefs(ctx)
	assert.Zero(t, live[id], "the failed upload no longer pins the chunk")
}

func TestUpload_WorkerFailureLeavesNothingLive(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	failing := &failingPool{chunkStore: h.workers}
	srv := New(":0", h.store, h.ring, failing, &fakeHealth{})
	rec := rawUpload(t, srv, strings.Repeat("z", chunkSize+10))
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	live, _ := h.store.LiveChunkRefs(ctx)
	assert.Empty(t, live, "a rolled-back upload holds no references, so GC can reclaim its chunks")
	objs, _ := h.store.ListObjectsByStatus(ctx, models.StatusFailed)
	assert.Len(t, objs, 1)
}

type failingPool struct {
	*chunkStore
	n atomic.Int32
}

func (p *failingPool) PutChunk(ctx context.Context, addr, id string, data io.Reader, size int64) error {
	if p.n.Add(1) == 3 {
		return io.ErrUnexpectedEOF
	}
	return p.chunkStore.PutChunk(ctx, addr, id, data, size)
}

func TestUpload_ExpiredWhileUploadingCannotCommit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	pool := &hookedPool{chunkStore: h.workers}
	pool.onPut = func() { h.store.FailStaleUploads(ctx, time.Now().Add(time.Hour)) } //nolint:errcheck
	srv := New(":0", h.store, h.ring, pool, &fakeHealth{})
	rec := rawUpload(t, srv, "slow upload")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "the GC already gave up on this upload")
	objs, _ := h.store.ListObjectsByStatus(ctx, models.StatusCommitted)
	assert.Empty(t, objs)
}

func TestUpload_ConcurrentIdenticalContent(t *testing.T) {
	h := newHarness(t)
	content := strings.Repeat("q", chunkSize+5)
	var wg sync.WaitGroup
	ids := make([]string, 10)
	codes := make([]int, 10)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := rawUpload(t, h.srv, content)
			codes[i] = rec.Code
			var r uploadResponse
			json.NewDecoder(rec.Body).Decode(&r) //nolint:errcheck
			ids[i] = r.ObjectID
		}(i)
	}
	wg.Wait()
	for i, id := range ids {
		require.Equal(t, http.StatusCreated, codes[i])
		_, body := h.download(t, id)
		assert.Equal(t, content, string(body))
	}
	require.NoError(t, h.store.SyncRefCounts(context.Background()))
	rec, _ := h.store.GetChunk(context.Background(), chunkIDOf(content[:chunkSize]))
	assert.Equal(t, 10, rec.RefCount)
	assert.Equal(t, models.ChunkDurable, rec.State)
}

// --- delete ---

func TestDelete_ConcurrentExactlyOneSucceeds(t *testing.T) {
	h := newHarness(t)
	r := h.upload(t, "a", "delete me once")
	var ok, notFound atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch h.deleteObject(t, r.ObjectID) {
			case http.StatusNoContent:
				ok.Add(1)
			case http.StatusNotFound:
				notFound.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), ok.Load())
	assert.Equal(t, int32(9), notFound.Load())
}

func TestDelete_KeepsChunkPinnedByInFlightUpload(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	content := "shared with an upload in progress"
	r := h.upload(t, "a", content)
	require.NoError(t, h.store.SaveObject(ctx, &models.ObjectManifest{ID: "inflight", Status: models.StatusStaged, ModifiedAt: time.Now().UTC()}))
	_, err := h.store.AddChunkRef(ctx, "inflight", models.ChunkRef{ChunkID: chunkIDOf(content)})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, h.deleteObject(t, r.ObjectID))
	assert.Empty(t, h.workers.callsFor("DEL", ""), "the in-flight upload already relies on this chunk")
	rec, err := h.store.GetChunk(ctx, chunkIDOf(content))
	require.NoError(t, err)
	assert.Equal(t, models.ChunkDurable, rec.State)
}

func TestDelete_StagedOrFailedObjectIs404(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for _, st := range []models.ObjectStatus{models.StatusStaged, models.StatusFailed} {
		require.NoError(t, h.store.SaveObject(ctx, &models.ObjectManifest{ID: "o" + string(st), Status: st}))
		assert.Equal(t, http.StatusNotFound, h.deleteObject(t, "o"+string(st)), st)
	}
}

// --- reads follow the replica list ---

func TestDownloadAndMetadataFollowRebalancedReplicas(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	content := "moved by the rebalancer"
	id := chunkIDOf(content)
	r := h.upload(t, "a", content)
	rec, _ := h.store.GetChunk(ctx, id)
	var spare []string
	for _, w := range []string{"w1", "w2", "w3", "w4"} {
		if w != rec.Replicas[0].WorkerID && w != rec.Replicas[1].WorkerID {
			spare = append(spare, w)
		}
	}
	for i, old := range rec.Replicas {
		h.workers.move(id, addrOfID(old.WorkerID), addrOfID(spare[i]))
		require.NoError(t, h.store.RemoveChunkReplica(ctx, id, old.WorkerID))
		require.NoError(t, h.store.AddChunkReplica(ctx, id, models.Replica{WorkerID: spare[i], WorkerAddr: addrOfID(spare[i])}))
	}
	code, body := h.download(t, r.ObjectID)
	assert.Equal(t, http.StatusOK, code, "the original pair no longer holds the chunk")
	assert.Equal(t, content, string(body))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+r.ObjectID+"/metadata", nil)
	var md struct {
		Chunks []chunkLocation `json:"chunks"`
	}
	require.NoError(t, json.NewDecoder(h.do(req).Body).Decode(&md))
	assert.Equal(t, spare, md.Chunks[0].Locations)
}

func TestDownloadWithSingleListedReplica(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	content := "one copy left"
	id := chunkIDOf(content)
	r := h.upload(t, "a", content)
	rec, _ := h.store.GetChunk(ctx, id)
	require.NoError(t, h.store.RemoveChunkReplica(ctx, id, rec.Replicas[0].WorkerID))
	code, body := h.download(t, r.ObjectID)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
}

// --- GC routes ---

type fakeRecon struct {
	queued   bool
	triggers atomic.Int32
}

func (f *fakeRecon) Trigger() bool { f.triggers.Add(1); return f.queued }
func (f *fakeRecon) Status() gc.Status {
	return gc.Status{Passes: 3, IntervalSeconds: 30, LastPass: &gc.Report{ExcessDeleted: 2}}
}

func TestGCRoutes(t *testing.T) {
	h := newHarness(t)
	assert.Equal(t, http.StatusNotFound, h.do(httptest.NewRequest(http.MethodGet, "/internal/gc/status", nil)).Code, "absent unless a reconciler is attached")

	h.srv.WithReconciler(&fakeRecon{queued: true})
	rec := h.do(httptest.NewRequest(http.MethodGet, "/internal/gc/status", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var st gc.Status
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&st))
	assert.Equal(t, int64(3), st.Passes)
	assert.Equal(t, 2, st.LastPass.ExcessDeleted)

	rec = h.do(httptest.NewRequest(http.MethodPost, "/internal/gc/run", nil))
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.JSONEq(t, `{"queued":true}`, rec.Body.String())
	assert.Equal(t, http.StatusMethodNotAllowed, h.do(httptest.NewRequest(http.MethodPost, "/internal/gc/status", nil)).Code)
	assert.Equal(t, http.StatusMethodNotAllowed, h.do(httptest.NewRequest(http.MethodGet, "/internal/gc/run", nil)).Code)
}

func TestRegistrationTriggersReconcile(t *testing.T) {
	h := newHarness(t)
	reg := func() int {
		req := httptest.NewRequest(http.MethodPost, "/internal/workers/register", strings.NewReader(`{"worker_id":"w9","address":"w9","port":8001}`))
		return h.do(req).Code
	}
	assert.Equal(t, http.StatusCreated, reg(), "works without a reconciler")
	rc := &fakeRecon{}
	h.srv.WithReconciler(rc)
	assert.Equal(t, http.StatusCreated, reg())
	assert.Equal(t, int32(1), rc.triggers.Load(), "a rejoining worker may carry stale copies")
	bad := httptest.NewRequest(http.MethodPost, "/internal/workers/register", strings.NewReader(`{}`))
	assert.Equal(t, http.StatusBadRequest, h.do(bad).Code)
	assert.Equal(t, int32(1), rc.triggers.Load(), "a rejected registration triggers nothing")
}
