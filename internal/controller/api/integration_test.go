package api

// Integration tests for the controller REST API.
// Each test uses a real bbolt store, a real placement ring, and a fake
// worker pool that stores chunks in memory.  A log-capture handler lets
// tests assert that specific slog records were emitted.

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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── log capture ──────────────────────────────────────────────────────────────

type logRecord struct {
	Level   string
	Message string
	Attrs   map[string]any
}

type logCapture struct {
	mu      sync.Mutex
	records []logRecord
}

func (l *logCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (l *logCapture) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, logRecord{
		Level:   r.Level.String(),
		Message: r.Message,
		Attrs:   attrs,
	})
	return nil
}

func (l *logCapture) WithAttrs(attrs []slog.Attr) slog.Handler { return l }
func (l *logCapture) WithGroup(name string) slog.Handler       { return l }

func (l *logCapture) hasMessage(msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.records {
		if strings.Contains(r.Message, msg) {
			return true
		}
	}
	return false
}

func (l *logCapture) messagesContaining(msg string) []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []logRecord
	for _, r := range l.records {
		if strings.Contains(r.Message, msg) {
			out = append(out, r)
		}
	}
	return out
}

func (l *logCapture) countMessage(msg string) int {
	return len(l.messagesContaining(msg))
}

// ── fake worker pool ──────────────────────────────────────────────────────────

type chunkStore struct {
	mu     sync.Mutex
	chunks map[string][]byte // "addr|chunkID" → data
	calls  []string          // log of all calls: "PUT addr chunkID", etc.
}

func newChunkStore() *chunkStore {
	return &chunkStore{chunks: make(map[string][]byte)}
}

func (cs *chunkStore) key(addr, id string) string { return addr + "|" + id }

func (cs *chunkStore) PutChunk(_ context.Context, addr, id string, data io.Reader, _ int64) error {
	b, _ := io.ReadAll(data)
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.chunks[cs.key(addr, id)] = b
	cs.calls = append(cs.calls, fmt.Sprintf("PUT %s %s", addr, id))
	return nil
}

func (cs *chunkStore) GetChunk(_ context.Context, addr, id string) (io.ReadCloser, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	b, ok := cs.chunks[cs.key(addr, id)]
	if !ok {
		return nil, models.ErrNotFound
	}
	cs.calls = append(cs.calls, fmt.Sprintf("GET %s %s", addr, id))
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (cs *chunkStore) DeleteChunk(_ context.Context, addr, id string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.chunks, cs.key(addr, id))
	cs.calls = append(cs.calls, fmt.Sprintf("DEL %s %s", addr, id))
	return nil
}

func (cs *chunkStore) ReplicatePeer(_ context.Context, _, _, _ string) error { return nil }
func (cs *chunkStore) ListChunks(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

func (cs *chunkStore) callsFor(op, id string) []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	var out []string
	for _, c := range cs.calls {
		if strings.Contains(c, op) && strings.Contains(c, id) {
			out = append(out, c)
		}
	}
	return out
}

// ── test harness ─────────────────────────────────────────────────────────────

type testHarness struct {
	srv     *Server
	store   *metadata.MetadataStore
	ring    *placement.Ring
	workers *chunkStore
	logs    *logCapture
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()

	cap := &logCapture{}
	slog.SetDefault(slog.New(cap))

	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ring := placement.NewRing()
	ring.AddWorker("w1", "localhost:9001")
	ring.AddWorker("w2", "localhost:9002")
	ring.AddWorker("w3", "localhost:9003")
	ring.AddWorker("w4", "localhost:9004")

	ctx := context.Background()
	for _, w := range []struct{ id, addr string }{
		{"w1", "localhost"}, {"w2", "localhost"}, {"w3", "localhost"}, {"w4", "localhost"},
	} {
		store.SaveWorker(ctx, &models.WorkerRecord{
			WorkerID: w.id, Address: w.addr,
			Port: 9000 + workerNum(w.id), Status: models.WorkerOnline, VnodeCount: 50,
		})
	}

	workers := newChunkStore()
	srv := New(":0", store, ring, workers, &fakeHealth{})

	return &testHarness{srv: srv, store: store, ring: ring, workers: workers, logs: cap}
}

func workerNum(id string) int {
	switch id {
	case "w1":
		return 1
	case "w2":
		return 2
	case "w3":
		return 3
	default:
		return 4
	}
}

func (h *testHarness) do(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func (h *testHarness) upload(t *testing.T, filename, content string) uploadResponse {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	io.WriteString(fw, content)
	mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := h.do(req)
	require.Equal(t, http.StatusCreated, rec.Code, "upload failed: %s", rec.Body.String())

	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

func (h *testHarness) download(t *testing.T, objectID string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+objectID, nil)
	rec := h.do(req)
	return rec.Code, rec.Body.Bytes()
}

func (h *testHarness) deleteObject(t *testing.T, objectID string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/objects/"+objectID, nil)
	return h.do(req).Code
}

// ── happy path tests ──────────────────────────────────────────────────────────

func TestIntegration_Upload_SmallFile_SingleChunk(t *testing.T) {
	h := newHarness(t)
	content := "hello distributed world"

	resp := h.upload(t, "hello.txt", content)

	assert.NotEmpty(t, resp.ObjectID)
	assert.Equal(t, 1, resp.Chunks)
	assert.Equal(t, int64(len(content)), resp.Bytes)

	// log: chunk was placed on primary + secondary
	assert.True(t, h.logs.hasMessage("chunk placed"), "expected 'chunk placed' log")
	assert.True(t, h.logs.hasMessage("chunk stored on primary"), "expected primary store log")
	assert.True(t, h.logs.hasMessage("chunk stored on secondary"), "expected secondary store log")
	assert.True(t, h.logs.hasMessage("chunk state updated in bbolt"), "expected bbolt state log")
	assert.True(t, h.logs.hasMessage("upload complete"), "expected upload complete log")

	// worker pool: 2 PUTs (primary + secondary)
	assert.Equal(t, 2, len(h.workers.callsFor("PUT", "")), "expected 2 PUT calls")
}

func TestIntegration_Upload_MultiChunk(t *testing.T) {
	h := newHarness(t)
	// 3 chunks: each chunk is exactly chunkSize bytes
	content := strings.Repeat("A", chunkSize) + strings.Repeat("B", chunkSize) + strings.Repeat("C", 512)

	resp := h.upload(t, "big.bin", content)

	assert.Equal(t, 3, resp.Chunks)
	assert.Equal(t, int64(len(content)), resp.Bytes)

	// 3 chunks × 2 workers = 6 PUT calls
	assert.Len(t, h.workers.callsFor("PUT", ""), 6)

	// log: 3 "chunk placed" entries
	assert.Equal(t, 3, h.logs.countMessage("chunk placed"))
	assert.Equal(t, 3, h.logs.countMessage("chunk stored on primary"))
	assert.Equal(t, 3, h.logs.countMessage("chunk stored on secondary"))
}

func TestIntegration_Download_RoundTrip_SmallFile(t *testing.T) {
	h := newHarness(t)
	content := "round trip content"

	resp := h.upload(t, "rt.txt", content)
	code, body := h.download(t, resp.ObjectID)

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))

	assert.True(t, h.logs.hasMessage("chunk fetched from primary"), "expected primary fetch log")
	assert.True(t, h.logs.hasMessage("chunk streamed to client"), "expected stream log")
	assert.True(t, h.logs.hasMessage("download complete"), "expected download complete log")
}

func TestIntegration_Download_RoundTrip_MultiChunk(t *testing.T) {
	h := newHarness(t)
	chunk1 := strings.Repeat("X", chunkSize)
	chunk2 := strings.Repeat("Y", chunkSize)
	chunk3 := "last partial chunk"
	content := chunk1 + chunk2 + chunk3

	resp := h.upload(t, "multi.bin", content)
	code, body := h.download(t, resp.ObjectID)

	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body), "downloaded content must exactly match uploaded content")

	// 3 chunks fetched
	assert.Equal(t, 3, h.logs.countMessage("chunk fetched from primary"))
}

func TestIntegration_Download_ExactlyOneChunk_Boundary(t *testing.T) {
	h := newHarness(t)
	content := strings.Repeat("Z", chunkSize) // exactly 1 MB

	resp := h.upload(t, "boundary.bin", content)
	assert.Equal(t, 1, resp.Chunks)

	code, body := h.download(t, resp.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
}

func TestIntegration_CAS_Dedup_SameContent(t *testing.T) {
	h := newHarness(t)
	content := "identical content"

	r1 := h.upload(t, "file1.txt", content)
	r2 := h.upload(t, "file2.txt", content)

	// different object IDs
	assert.NotEqual(t, r1.ObjectID, r2.ObjectID)

	// but only 2 PUTs total (not 4) — chunk was deduped on second upload
	assert.Len(t, h.workers.callsFor("PUT", ""), 2, "dedup: second upload should not re-push chunk")

	// log confirms dedup path
	assert.True(t, h.logs.hasMessage("chunk already exists (dedup)"), "expected dedup log")

	// both objects download correctly
	_, b1 := h.download(t, r1.ObjectID)
	_, b2 := h.download(t, r2.ObjectID)
	assert.Equal(t, content, string(b1))
	assert.Equal(t, content, string(b2))
}

func TestIntegration_List_ReturnsOnlyCommitted(t *testing.T) {
	h := newHarness(t)

	h.upload(t, "a.txt", "aaa")
	h.upload(t, "b.txt", "bbb")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	rec := h.do(req)
	require.Equal(t, http.StatusOK, rec.Code)

	var objects []*models.ObjectManifest
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&objects))
	assert.Len(t, objects, 2)
	for _, o := range objects {
		assert.Equal(t, models.StatusCommitted, o.Status)
	}
}

func TestIntegration_GetMetadata(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "meta.txt", "content")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID+"/metadata", nil)
	rec := h.do(req)
	require.Equal(t, http.StatusOK, rec.Code)

	var manifest models.ObjectManifest
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&manifest))
	assert.Equal(t, resp.ObjectID, manifest.ID)
	assert.Equal(t, "meta.txt", manifest.Name)
	assert.Equal(t, models.StatusCommitted, manifest.Status)
	assert.Equal(t, 1, manifest.ChunkCount)
}

func TestIntegration_PatchMetadata(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "patch.txt", "content")

	body, _ := json.Marshal(map[string]string{"env": "prod", "owner": "team-a"})
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+resp.ObjectID+"/metadata",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)
	require.Equal(t, http.StatusOK, rec.Code)

	var manifest models.ObjectManifest
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&manifest))
	assert.Equal(t, "prod", manifest.CustomMetadata["env"])
	assert.Equal(t, "team-a", manifest.CustomMetadata["owner"])
}

func TestIntegration_Delete_RemovesObjectAndChunks(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "del.txt", "delete me")

	code := h.deleteObject(t, resp.ObjectID)
	assert.Equal(t, http.StatusNoContent, code)

	// download returns 404
	dlCode, _ := h.download(t, resp.ObjectID)
	assert.Equal(t, http.StatusNotFound, dlCode)

	// workers received DELETE calls
	assert.NotEmpty(t, h.workers.callsFor("DEL", ""))

	// bbolt: manifest fully removed (not just marked DELETED)
	_, err := h.store.GetObject(context.Background(), resp.ObjectID)
	assert.ErrorIs(t, err, models.ErrNotFound, "manifest must be purged from bbolt after delete")
}

func TestIntegration_Delete_AlreadyDeleted_Returns404(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "idem.txt", "content")

	assert.Equal(t, http.StatusNoContent, h.deleteObject(t, resp.ObjectID))
	assert.Equal(t, http.StatusNotFound, h.deleteObject(t, resp.ObjectID)) // PDF §5.5: 404 on second delete
}

// ── edge case / error path tests ──────────────────────────────────────────────

func TestIntegration_Download_NotFound(t *testing.T) {
	h := newHarness(t)
	code, _ := h.download(t, "nonexistent-id")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestIntegration_GetMetadata_NotFound(t *testing.T) {
	h := newHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/ghost/metadata", nil)
	rec := h.do(req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestIntegration_PatchMetadata_NotFound(t *testing.T) {
	h := newHarness(t)
	body, _ := json.Marshal(map[string]string{"k": "v"})
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/ghost/metadata", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestIntegration_Delete_NotFound(t *testing.T) {
	h := newHarness(t)
	assert.Equal(t, http.StatusNotFound, h.deleteObject(t, "ghost"))
}

func TestIntegration_Upload_MissingFileField(t *testing.T) {
	h := newHarness(t)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mw.CreateFormField("not_file") // wrong field name
	mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := h.do(req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestIntegration_Download_PrimaryFails_FallsBackToSecondary(t *testing.T) {
	h := newHarness(t)
	content := "failover content"

	resp := h.upload(t, "failover.txt", content)

	// Find which primary worker got the chunk and remove it from the store
	// so the GET returns ErrNotFound — simulating a worker that lost the chunk.
	chunkRefs, err := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	require.NoError(t, err)
	require.Len(t, chunkRefs, 1)

	ref := chunkRefs[0]
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(ref.PrimaryID))

	// Delete from primary in fake store to force fallback
	h.workers.mu.Lock()
	delete(h.workers.chunks, h.workers.key(primaryAddr, ref.ChunkID))
	h.workers.mu.Unlock()

	// Download should still succeed using secondary
	code, body := h.download(t, resp.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))

	// Log confirms fallback path
	assert.True(t, h.logs.hasMessage("chunk fetch failed"), "expected fallback warning")
	assert.True(t, h.logs.hasMessage("chunk fetched"), "expected secondary fetch log")
}

func TestIntegration_WorkerRegistration(t *testing.T) {
	h := newHarness(t)

	body, _ := json.Marshal(models.RegisterRequest{
		WorkerID: "w5", Address: "localhost", Port: 9005, CapacityBytes: 10 << 30,
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/workers/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)
	require.Equal(t, http.StatusCreated, rec.Code)

	var regResp models.RegisterResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&regResp))
	assert.Equal(t, "registered", regResp.Status)
	assert.Equal(t, 50, regResp.VnodeCount)

	// Worker is now in bbolt
	w, err := h.store.GetWorker(context.Background(), "w5")
	require.NoError(t, err)
	assert.Equal(t, models.WorkerOnline, w.Status)
}

func TestIntegration_Heartbeat_UnknownWorker_Returns404(t *testing.T) {
	h := newHarness(t)

	body, _ := json.Marshal(map[string]string{"worker_id": "ghost-worker"})
	req := httptest.NewRequest(http.MethodPost, "/internal/workers/heartbeat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestIntegration_Heartbeat_KnownWorker_Returns204(t *testing.T) {
	h := newHarness(t)

	body, _ := json.Marshal(map[string]string{"worker_id": "w1"})
	req := httptest.NewRequest(http.MethodPost, "/internal/workers/heartbeat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// ── ring-recovery regression tests ───────────────────────────────────────────

// TestIntegration_RingRebuild_AfterRestart simulates a controller restart: same bbolt,
// fresh empty ring. Upload must succeed because the ring re-populates from bbolt.
func TestIntegration_RingRebuild_AfterRestart(t *testing.T) {
	dbPath := t.TempDir() + "/meta.db"

	// --- first controller: register workers and upload ---
	store1, err := metadata.NewMetadataStore(dbPath)
	require.NoError(t, err)

	ring1 := placement.NewRing()
	ring1.AddWorker("w1", "localhost:9001")
	ring1.AddWorker("w2", "localhost:9002")
	ring1.AddWorker("w3", "localhost:9003")
	ring1.AddWorker("w4", "localhost:9004")

	ctx := context.Background()
	for _, rec := range []struct {
		id   string
		port int
	}{
		{"w1", 9001}, {"w2", 9002}, {"w3", 9003}, {"w4", 9004},
	} {
		store1.SaveWorker(ctx, &models.WorkerRecord{
			WorkerID: rec.id, Address: "localhost", Port: rec.port,
			Status: models.WorkerOnline, VnodeCount: 50,
		})
	}

	workers := newChunkStore()
	srv1 := New(":0", store1, ring1, workers, &fakeHealth{})
	h1 := &testHarness{srv: srv1, store: store1, ring: ring1, workers: workers, logs: &logCapture{}}
	resp := h1.upload(t, "pre-restart.txt", "data before restart")
	assert.NotEmpty(t, resp.ObjectID)
	store1.Close()

	// --- second controller: same bbolt, fresh empty ring (simulates restart) ---
	store2, err := metadata.NewMetadataStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { store2.Close() })

	ring2 := placement.NewRing()
	// Rebuild ring from bbolt — same logic as main.go on startup
	known, err := store2.ListWorkers(ctx)
	require.NoError(t, err)
	for _, w := range known {
		ring2.AddWorker(w.WorkerID, fmt.Sprintf("%s:%d", w.Address, w.Port))
	}

	srv2 := New(":0", store2, ring2, workers, &fakeHealth{})
	h2 := &testHarness{srv: srv2, store: store2, ring: ring2, workers: workers, logs: &logCapture{}}

	resp2 := h2.upload(t, "post-restart.txt", "data after restart")
	assert.NotEmpty(t, resp2.ObjectID, "upload must succeed after ring rebuild from bbolt")
}

// TestIntegration_Heartbeat_ReAddsWorkerRemovedFromRing verifies that a heartbeat
// from a worker that was dropped from the ring (e.g. marked OFFLINE then back) puts
// it back so chunks can be placed on it again.
func TestIntegration_Heartbeat_ReAddsWorkerRemovedFromRing(t *testing.T) {
	h := newHarness(t)

	// Remove w1 from ring to simulate it having been declared OFFLINE
	h.ring.RemoveWorker("w1")

	// Send a heartbeat for w1
	body, _ := json.Marshal(map[string]string{"worker_id": "w1"})
	req := httptest.NewRequest(http.MethodPost, "/internal/workers/heartbeat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)
	assert.Equal(t, http.StatusNoContent, rec.Code, "heartbeat should succeed")

	// w1 must be back in the ring
	workers := h.ring.Workers()
	found := false
	for _, id := range workers {
		if id == "w1" {
			found = true
			break
		}
	}
	assert.True(t, found, "w1 must be back in the ring after heartbeat")
}

// TestIntegration_Heartbeat_UnknownDatabase_Returns404 ensures that a worker the
// database has never seen still gets a 404, triggering full re-registration.
func TestIntegration_Heartbeat_NeverSeenWorker_Returns404(t *testing.T) {
	h := newHarness(t)

	body, _ := json.Marshal(map[string]string{"worker_id": "ghost-worker"})
	req := httptest.NewRequest(http.MethodPost, "/internal/workers/heartbeat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestIntegration_EmptyDatabase_BootsClean verifies the controller starts up and
// serves requests correctly with a brand-new empty database (no stored workers).
func TestIntegration_EmptyDatabase_BootsClean(t *testing.T) {
	store, err := metadata.NewMetadataStore(t.TempDir() + "/empty.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ring := placement.NewRing()
	srv := New(":0", store, ring, newChunkStore(), &fakeHealth{})

	// List objects on empty db — must not crash
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Upload on empty ring — must return 503, not panic
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", "test.txt")
	io.WriteString(fw, "data")
	mw.Close()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
	req2.Header.Set("Content-Type", mw.FormDataContentType())
	rec2 := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusServiceUnavailable, rec2.Code, "empty ring must return 503, not panic")
}

func TestIntegration_CAS_RefCount_DecrementsToZero_DeletesFromWorkers(t *testing.T) {
	h := newHarness(t)
	content := "shared chunk content"

	// Upload same content in two objects
	r1 := h.upload(t, "obj1.txt", content)
	r2 := h.upload(t, "obj2.txt", content)

	// Delete first object — ref count goes 2→1, chunk stays on workers
	h.deleteObject(t, r1.ObjectID)
	assert.Empty(t, h.workers.callsFor("DEL", ""), "chunk should NOT be deleted while refcount=1")

	// Delete second object — ref count goes 1→0, chunk deleted from workers
	h.deleteObject(t, r2.ObjectID)
	assert.NotEmpty(t, h.workers.callsFor("DEL", ""), "chunk SHOULD be deleted when refcount=0")
}
