package api

// Deep failure coverage: scenarios that none of the existing test files reach.
//
// Covered here:
//   T1  — Download non-committed (STAGED / FAILED / DELETED) → 404
//   T2  — Upload with 0 or 1 online workers → 503
//   T3  — Worker data loss: primary returns ErrNotFound → silent fallback to secondary
//   T4  — Concurrent CAS-dedup upload: N goroutines upload identical content
//          simultaneously → correct final ref_count, all objects downloadable
//   T5  — Delete STAGED / FAILED object → 404 (not an accidental mid-upload wipe)
//   T6  — List only returns COMMITTED objects, never STAGED / FAILED / DELETED
//   T7  — Multi-chunk: primary loses only the middle chunk → fallback, full 200
//   T8  — handleDelete removes the manifest from bbolt entirely (B3 fix)

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── notFoundPool: wraps corruptPool, injects ErrNotFound for specific keys ────

type notFoundPool struct {
	*corruptPool
	mu2 sync.Mutex
	nf  map[string]bool // "addr|chunkID" → inject ErrNotFound on GetChunk
}

func newNotFoundPool(cp *corruptPool) *notFoundPool {
	return &notFoundPool{corruptPool: cp, nf: make(map[string]bool)}
}

func (p *notFoundPool) GetChunk(ctx context.Context, addr, id string) (io.ReadCloser, error) {
	p.mu2.Lock()
	missing := p.nf[p.key(addr, id)]
	p.mu2.Unlock()
	if missing {
		return nil, models.ErrNotFound
	}
	return p.corruptPool.GetChunk(ctx, addr, id)
}

func (p *notFoundPool) setNotFound(addr, chunkID string) {
	p.mu2.Lock()
	p.nf[p.key(addr, chunkID)] = true
	p.mu2.Unlock()
}

// uploadViaSrv uploads content through the server's current worker pool.
func uploadViaSrv(t *testing.T, srv *Server, filename, content string) uploadResponse {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	io.WriteString(fw, content) //nolint:errcheck
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, "upload failed: %s", rec.Body.String())
	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// uploadGetCode uploads content and returns only the HTTP status code.
func uploadGetCode(t *testing.T, h *testHarness, filename, content string) int {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	io.WriteString(fw, content) //nolint:errcheck
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return h.do(req).Code
}

// ── T1: download non-committed objects ────────────────────────────────────────

func TestDownload_StagedObject_Returns404(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.store.SaveObject(context.Background(), &models.ObjectManifest{
		ID: "staged-dl", Status: models.StatusStaged, CreatedAt: time.Now().UTC(),
	}))
	code, _ := h.download(t, "staged-dl")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestDownload_FailedObject_Returns404(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.store.SaveObject(context.Background(), &models.ObjectManifest{
		ID: "failed-dl", Status: models.StatusFailed, CreatedAt: time.Now().UTC(),
	}))
	code, _ := h.download(t, "failed-dl")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestDownload_DeletedObject_Returns404(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "todel.txt", "bye")
	h.deleteObject(t, resp.ObjectID)
	code, _ := h.download(t, resp.ObjectID)
	assert.Equal(t, http.StatusNotFound, code)
}

// ── T2: upload with insufficient workers ─────────────────────────────────────

func TestUpload_ZeroWorkers_Returns503(t *testing.T) {
	h := newHarness(t)
	h.srv.placement = &fakePlacement{workers: []string{}}
	code := uploadGetCode(t, h, "nope.txt", "data")
	assert.Equal(t, http.StatusServiceUnavailable, code,
		"upload with 0 workers must return 503")
}

func TestUpload_OneWorker_Returns503(t *testing.T) {
	h := newHarness(t)
	h.srv.placement = &fakePlacement{workers: []string{"w1"}}
	code := uploadGetCode(t, h, "nope.txt", "data")
	assert.Equal(t, http.StatusServiceUnavailable, code,
		"upload with 1 worker must return 503 (no secondary possible)")
}

// ── T3: worker data loss — primary returns ErrNotFound → fallback ─────────────

func TestWorkerDataLoss_PrimaryChunkMissing_FallsBackToSecondary(t *testing.T) {
	h, cp := newRepairHarness(t)
	pool := newNotFoundPool(cp)
	h.srv.workers = pool

	content := "worker lost this chunk"
	resp := uploadViaSrv(t, h.srv, "loss.txt", content)

	refs, err := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	require.NoError(t, err)
	require.Len(t, refs, 1)

	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))
	pool.setNotFound(primaryAddr, refs[0].ChunkID)

	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, content, rec.Body.String(),
		"content must be served from secondary when primary chunk is missing")
}

// ── T4: concurrent CAS-dedup uploads — same content, N goroutines ─────────────

func TestConcurrentCASDedup_RefCountCorrect(t *testing.T) {
	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ring := placement.NewRing()
	for _, rec := range []struct {
		id   string
		port int
	}{
		{"w1", 9001}, {"w2", 9002}, {"w3", 9003}, {"w4", 9004},
	} {
		ring.AddWorker(rec.id, fmt.Sprintf("localhost:%d", rec.port))
		store.SaveWorker(context.Background(), &models.WorkerRecord{
			WorkerID: rec.id, Address: "localhost", Port: rec.port,
			Status: models.WorkerOnline, VnodeCount: 50,
		})
	}
	pool := newCorruptPool()
	srv := New(":0", store, ring, pool, &fakeHealth{})

	content := strings.Repeat("dedup-me-", 100) // well under chunkSize → 1 chunk

	const n = 5
	type result struct {
		code     int
		objectID string
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			var buf bytes.Buffer
			mw := multipart.NewWriter(&buf)
			fw, _ := mw.CreateFormFile("file", "dup.txt")
			io.WriteString(fw, content) //nolint:errcheck
			mw.Close()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
			req.Header.Set("Content-Type", mw.FormDataContentType())
			rec := httptest.NewRecorder()
			srv.http.Handler.ServeHTTP(rec, req)
			var resp uploadResponse
			json.NewDecoder(rec.Body).Decode(&resp) //nolint:errcheck
			results[i] = result{rec.Code, resp.ObjectID}
		}()
	}
	wg.Wait()

	// Every upload must succeed
	for i, r := range results {
		assert.Equal(t, http.StatusCreated, r.code, "upload %d must succeed", i)
		assert.NotEmpty(t, r.objectID)
	}

	// All objects must reference the same single chunk (CAS dedup)
	chunkID := chunkIDFor(content)
	chunk, err := store.GetChunk(context.Background(), chunkID)
	require.NoError(t, err, "shared chunk must exist in bbolt")
	assert.Equal(t, n, chunk.RefCount,
		"ref_count must equal number of objects (one per upload)")

	// All n objects must be individually downloadable
	for _, r := range results {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+r.objectID, nil)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code, "object %s must be downloadable", r.objectID)
		assert.Equal(t, content, rec.Body.String(), "content must be identical for object %s", r.objectID)
	}
}

// ── T5: delete non-committed object is rejected ───────────────────────────────

func TestDelete_StagedObject_Returns404(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.store.SaveObject(context.Background(), &models.ObjectManifest{
		ID: "staged-del", Status: models.StatusStaged, CreatedAt: time.Now().UTC(),
	}))
	code := h.deleteObject(t, "staged-del")
	assert.Equal(t, http.StatusNotFound, code,
		"deleting a STAGED object must return 404 — it may still be uploading")

	// Object must still be STAGED in bbolt (upload can continue)
	manifest, err := h.store.GetObject(context.Background(), "staged-del")
	require.NoError(t, err)
	assert.Equal(t, models.StatusStaged, manifest.Status,
		"STAGED object must not be mutated by a rejected delete")
}

func TestDelete_FailedObject_Returns404(t *testing.T) {
	h := newHarness(t)
	require.NoError(t, h.store.SaveObject(context.Background(), &models.ObjectManifest{
		ID: "failed-del", Status: models.StatusFailed, CreatedAt: time.Now().UTC(),
	}))
	assert.Equal(t, http.StatusNotFound, h.deleteObject(t, "failed-del"),
		"deleting a FAILED object must return 404")
}

// ── T6: list never exposes non-committed objects ──────────────────────────────

func TestList_ExcludesNonCommittedObjects(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	committed := h.upload(t, "real.txt", "content")

	for _, tc := range []struct {
		id     string
		status models.ObjectStatus
	}{
		{"staged-id", models.StatusStaged},
		{"failed-id", models.StatusFailed},
		{"deleted-id", models.StatusDeleted},
	} {
		require.NoError(t, h.store.SaveObject(ctx, &models.ObjectManifest{
			ID: tc.id, Status: tc.status, CreatedAt: time.Now().UTC(),
		}))
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	rec := h.do(req)
	require.Equal(t, http.StatusOK, rec.Code)

	var objects []*models.ObjectManifest
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&objects))

	ids := make([]string, len(objects))
	for i, o := range objects {
		ids[i] = o.ID
		assert.Equal(t, models.StatusCommitted, o.Status,
			"list must only return COMMITTED objects, got %s for %s", o.Status, o.ID)
	}
	assert.Contains(t, ids, committed.ObjectID)
	assert.NotContains(t, ids, "staged-id")
	assert.NotContains(t, ids, "failed-id")
	assert.NotContains(t, ids, "deleted-id")
}

// ── T7: multi-chunk — primary loses the middle chunk → fallback, full 200 ─────

func TestMultiChunk_PrimaryLosesMiddleChunk_FullDownload200(t *testing.T) {
	h, cp := newRepairHarness(t)
	pool := newNotFoundPool(cp)
	h.srv.workers = pool

	chunk0 := bytes.Repeat([]byte("A"), chunkSize)
	chunk1 := bytes.Repeat([]byte("B"), chunkSize)
	chunk2 := bytes.Repeat([]byte("C"), chunkSize)
	content := append(append(chunk0, chunk1...), chunk2...)

	resp := uploadViaSrv(t, h.srv, "big.bin", string(content))
	require.Equal(t, 3, resp.Chunks)

	refs, err := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	require.NoError(t, err)

	midRef := refs[1]
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(midRef.PrimaryID))
	pool.setNotFound(primaryAddr, midRef.ChunkID)

	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code,
		"download must succeed when middle chunk falls back to secondary")
	assert.Equal(t, content, rec.Body.Bytes(),
		"full content must be byte-identical despite primary data loss on chunk 1")
}

// ── T8: delete removes manifest from bbolt entirely ───────────────────────────

func TestDelete_ManifestRemovedFromBbolt(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "rm.txt", "erase me")

	code := h.deleteObject(t, resp.ObjectID)
	require.Equal(t, http.StatusNoContent, code)

	_, err := h.store.GetObject(context.Background(), resp.ObjectID)
	assert.ErrorIs(t, err, models.ErrNotFound,
		"manifest must be fully removed from bbolt after delete, not just marked DELETED")
}
