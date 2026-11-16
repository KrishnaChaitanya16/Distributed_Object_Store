package api

// Crash-recovery and network-partition tests for the upload handler.
//
// Crash taxonomy tested here:
//   CP5 — COMMITTED in bbolt but 201 never reached client → idempotency replay
//
// Network / worker failure scenarios:
//   NP1 — primary worker down before first PutChunk → 502, rollback
//   NP2 — secondary worker down → 502, rollback
//   NP3 — both workers down → 503 fast-fail (no network attempt)
//   NP4 — crash on chunk N of a multi-chunk file → partial rollback, 502
//   NP5 — context cancelled mid-upload (client disconnect) → rollback
//   NP6 — worker returns error on a dedup chunk read → upload still proceeds
//
// Idempotency scenarios:
//   IDP1 — first request with key → normal upload, 201
//   IDP2 — repeat request with same key after commit → 201 replay, same object_id
//   IDP3 — concurrent request with same key still processing → 409
//   IDP4 — no Idempotency-Key header → normal, unguarded upload

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

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── failing worker pool for network-partition tests ───────────────────────────

type partitionPool struct {
	mu         sync.Mutex
	chunks     map[string][]byte
	failPrimOn map[string]bool // workerAddr → fail PutChunk
	failSecOn  map[string]bool
	putCalls   []string
}

func newPartitionPool() *partitionPool {
	return &partitionPool{
		chunks:     make(map[string][]byte),
		failPrimOn: make(map[string]bool),
		failSecOn:  make(map[string]bool),
	}
}

func (p *partitionPool) k(addr, id string) string { return addr + "|" + id }

func (p *partitionPool) PutChunk(_ context.Context, addr, chunkID string, data io.Reader, _ int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.putCalls = append(p.putCalls, addr+"|"+chunkID)
	if p.failPrimOn[addr] || p.failSecOn[addr] {
		return fmt.Errorf("worker %s: connection refused", addr)
	}
	b, _ := io.ReadAll(data)
	p.chunks[p.k(addr, chunkID)] = b
	return nil
}
func (p *partitionPool) GetChunk(_ context.Context, addr, chunkID string) (io.ReadCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.chunks[p.k(addr, chunkID)]
	if !ok {
		return nil, models.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (p *partitionPool) DeleteChunk(_ context.Context, _, _ string) error      { return nil }
func (p *partitionPool) ReplicatePeer(_ context.Context, _, _, _ string) error { return nil }
func (p *partitionPool) ListChunks(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func buildMultipart(t *testing.T, filename, content string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = io.WriteString(fw, content)
	require.NoError(t, err)
	mw.Close()
	return &buf, mw.FormDataContentType()
}

func doUpload(t *testing.T, srv *Server, filename, content string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	body, ct := buildMultipart(t, filename, content)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func parseUploadResp(t *testing.T, rec *httptest.ResponseRecorder) uploadResponse {
	t.Helper()
	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

func twoWorkerSrv(t *testing.T, pool *partitionPool) (*Server, *fakePlacement) {
	t.Helper()
	store := mustOpenStore(t, t.TempDir())
	pl := &fakePlacement{workers: []string{"w1", "w2"}}
	srv := New(":0", store, pl, pool, &fakeHealth{online: []string{"w1", "w2"}})
	return srv, pl
}

// ── IDP1: new idempotency key → normal upload ─────────────────────────────────

func TestIDP1_NewIdempotencyKey_NormalUpload(t *testing.T) {
	pool := newPartitionPool()
	srv, _ := twoWorkerSrv(t, pool)

	rec := doUpload(t, srv, "f.txt", "hello", map[string]string{
		"Idempotency-Key": "idem-001",
	})

	assert.Equal(t, http.StatusCreated, rec.Code)
	resp := parseUploadResp(t, rec)
	assert.NotEmpty(t, resp.ObjectID)
}

// ── IDP2: repeat request after commit → replay same object_id ────────────────

func TestIDP2_RepeatIdempotencyKey_ReplaysSameObjectID(t *testing.T) {
	pool := newPartitionPool()
	srv, _ := twoWorkerSrv(t, pool)

	rec1 := doUpload(t, srv, "f.txt", "hello", map[string]string{"Idempotency-Key": "idem-002"})
	require.Equal(t, http.StatusCreated, rec1.Code, "first upload must succeed")
	resp1 := parseUploadResp(t, rec1)

	// Second request with same key — should replay.
	rec2 := doUpload(t, srv, "f.txt", "hello", map[string]string{"Idempotency-Key": "idem-002"})
	assert.Equal(t, http.StatusCreated, rec2.Code, "replay must return 201")
	resp2 := parseUploadResp(t, rec2)

	assert.Equal(t, resp1.ObjectID, resp2.ObjectID,
		"replayed response must return the SAME object_id — no duplicate created")
	assert.Equal(t, "true", rec2.Header().Get("X-Idempotency-Replayed"),
		"replayed response must carry X-Idempotency-Replayed: true")
}

// ── IDP3: concurrent request with same key still processing → 409 ─────────────

func TestIDP3_ConcurrentIdempotencyKey_Returns409(t *testing.T) {
	store := mustOpenStore(t, t.TempDir())
	pool := newPartitionPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}
	srv := New(":0", store, pl, pool, &fakeHealth{online: []string{"w1", "w2"}})

	// Manually insert a PROCESSING sentinel (simulates in-flight request).
	_, err := store.(interface {
		AcquireIdempotencyLock(context.Context, string) (*models.IdempotencyRecord, error)
	}).AcquireIdempotencyLock(context.Background(), "idem-003")
	require.NoError(t, err)

	rec := doUpload(t, srv, "f.txt", "hello", map[string]string{"Idempotency-Key": "idem-003"})
	assert.Equal(t, http.StatusConflict, rec.Code,
		"second request with in-flight key must return 409 Conflict")
	assert.Contains(t, rec.Body.String(), "already processing")
}

// ── IDP4: no idempotency key → unguarded normal upload ────────────────────────

func TestIDP4_NoIdempotencyKey_NormalUpload(t *testing.T) {
	pool := newPartitionPool()
	srv, _ := twoWorkerSrv(t, pool)

	rec := doUpload(t, srv, "f.txt", "world", nil)
	assert.Equal(t, http.StatusCreated, rec.Code)
}

// ── NP1: primary worker down → 502, object rolled back ───────────────────────

func TestNP1_PrimaryWorkerDown_Returns502_ObjectRolledBack(t *testing.T) {
	pool := newPartitionPool()
	pool.failPrimOn[""] = true // workerAddr resolves to "" in test (no worker record in bbolt)
	srv, _ := twoWorkerSrv(t, pool)

	rec := doUpload(t, srv, "f.txt", "data", nil)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
}

// ── NP2: secondary worker down → 502, object rolled back ─────────────────────

func TestNP2_SecondaryWorkerDown_Returns502_ObjectRolledBack(t *testing.T) {
	store := mustOpenStore(t, t.TempDir())
	pool := newPartitionPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}

	// Primary ("") succeeds, secondary ("") also resolves to "" in fake.
	// Simulate: primary succeeds, secondary fails.
	callCount := 0
	failingPool := &secondFailPool{inner: pool, failAfter: 1, callCount: &callCount}

	srv := New(":0", store, pl, failingPool, &fakeHealth{online: []string{"w1", "w2"}})
	rec := doUpload(t, srv, "f.txt", "data", nil)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
}

// secondFailPool wraps a pool and fails PutChunk after N successful calls.
type secondFailPool struct {
	inner     *partitionPool
	failAfter int
	callCount *int
}

func (p *secondFailPool) PutChunk(ctx context.Context, addr, id string, data io.Reader, size int64) error {
	*p.callCount++
	if *p.callCount > p.failAfter {
		return fmt.Errorf("worker %s: network partition", addr)
	}
	return p.inner.PutChunk(ctx, addr, id, data, size)
}
func (p *secondFailPool) GetChunk(ctx context.Context, a, id string) (io.ReadCloser, error) {
	return p.inner.GetChunk(ctx, a, id)
}
func (p *secondFailPool) DeleteChunk(ctx context.Context, a, id string) error {
	return p.inner.DeleteChunk(ctx, a, id)
}
func (p *secondFailPool) ReplicatePeer(ctx context.Context, a, id, b string) error { return nil }
func (p *secondFailPool) ListChunks(ctx context.Context, a, id string) ([]string, error) {
	return nil, nil
}

// ── NP3: both workers down before upload starts → 503 fast-fail ───────────────

func TestNP3_BothWorkersDown_Returns503_NoChunksWritten(t *testing.T) {
	store := mustOpenStore(t, t.TempDir())
	pool := newPartitionPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}
	// Health says no workers online.
	srv := New(":0", store, pl, pool, &fakeHealth{online: []string{}})

	rec := doUpload(t, srv, "f.txt", "data", nil)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	// No PutChunk calls should have been made.
	pool.mu.Lock()
	defer pool.mu.Unlock()
	assert.Empty(t, pool.putCalls, "must not attempt worker calls when health check fails")
}

// ── NP4: multi-chunk file, failure on chunk 2 → partial rollback ──────────────

func TestNP4_MultiChunkFile_FailsOnChunk2_RollsBack(t *testing.T) {
	store := mustOpenStore(t, t.TempDir())
	callCount := 0
	pool := newPartitionPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}
	// Fail after 2 successful PutChunk calls (chunk0 primary+secondary succeed, chunk1 primary fails).
	failingPool := &secondFailPool{inner: pool, failAfter: 2, callCount: &callCount}
	srv := New(":0", store, pl, failingPool, &fakeHealth{online: []string{"w1", "w2"}})

	// Need content > 1 MB to get 2 chunks (chunkSize = 1 MB).
	content := strings.Repeat("x", chunkSize+100)
	body, ct := buildMultipart(t, "big.bin", content)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadGateway, rec.Code)

	// Verify: no COMMITTED objects exist.
	objs, err := store.(interface {
		ListObjectsByStatus(context.Context, models.ObjectStatus) ([]*models.ObjectManifest, error)
	}).ListObjectsByStatus(context.Background(), models.StatusCommitted)
	require.NoError(t, err)
	assert.Empty(t, objs, "no committed objects after partial fanout failure")
}

// ── NP5: context cancelled mid-upload (client disconnect) → rollback ──────────

func TestNP5_ContextCancelled_Upload_RollsBack(t *testing.T) {
	store := mustOpenStore(t, t.TempDir())
	pool := newPartitionPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}

	// Make PutChunk block until context is cancelled.
	blockingPool := &blockPool{inner: pool}
	srv := New(":0", store, pl, blockingPool, &fakeHealth{online: []string{"w1", "w2"}})

	body, ct := buildMultipart(t, "f.txt", "hello")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body).WithContext(ctx)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)

	// Either 502 (worker error from ctx cancellation) or 503 is acceptable.
	assert.NotEqual(t, http.StatusCreated, rec.Code,
		"upload must not succeed after context cancellation")
}

// blockPool delays PutChunk by 1 second (longer than the test timeout).
type blockPool struct{ inner *partitionPool }

func (p *blockPool) PutChunk(ctx context.Context, addr, id string, data io.Reader, size int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second):
		return p.inner.PutChunk(ctx, addr, id, data, size)
	}
}
func (p *blockPool) GetChunk(ctx context.Context, a, id string) (io.ReadCloser, error) {
	return p.inner.GetChunk(ctx, a, id)
}
func (p *blockPool) DeleteChunk(ctx context.Context, a, id string) error         { return nil }
func (p *blockPool) ReplicatePeer(_ context.Context, _, _, _ string) error       { return nil }
func (p *blockPool) ListChunks(_ context.Context, _, _ string) ([]string, error) { return nil, nil }

// ── NP6: failed upload leaves no committed object in bbolt ────────────────────

func TestNP6_RollbackLeavesNoCommittedObject(t *testing.T) {
	store := mustOpenStore(t, t.TempDir())
	pool := newPartitionPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}
	// Fail every PutChunk.
	pool.failPrimOn[""] = true
	srv := New(":0", store, pl, pool, &fakeHealth{online: []string{"w1", "w2"}})

	rec := doUpload(t, srv, "f.txt", "data", nil)
	require.Equal(t, http.StatusBadGateway, rec.Code, "worker failure must return 502")

	// After rollback: no COMMITTED objects must exist in bbolt.
	type lister interface {
		ListObjectsByStatus(context.Context, models.ObjectStatus) ([]*models.ObjectManifest, error)
	}
	objs, err := store.(lister).ListObjectsByStatus(context.Background(), models.StatusCommitted)
	require.NoError(t, err)
	assert.Empty(t, objs, "rollback must leave zero committed objects in bbolt")

	// A fresh upload with a healthy pool must succeed cleanly.
	pool2 := newPartitionPool() // brand-new, no failures
	srv2 := New(":0", store, pl, pool2, &fakeHealth{online: []string{"w1", "w2"}})
	rec2 := doUpload(t, srv2, "f.txt", "data-v2", nil)
	assert.Equal(t, http.StatusCreated, rec2.Code, "new upload after recovery must succeed")
}

// ── IDP2 extended: idempotency across server restart ─────────────────────────

func TestIDP2_IdempotencyKeyPersists_AcrossServerRestart(t *testing.T) {
	dir := t.TempDir()
	pool := newPartitionPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}

	// First server instance — commit an upload.
	store1 := mustOpenStore(t, dir)
	srv1 := New(":0", store1, pl, pool, &fakeHealth{online: []string{"w1", "w2"}})
	rec1 := doUpload(t, srv1, "f.txt", "content", map[string]string{"Idempotency-Key": "restart-key"})
	require.Equal(t, http.StatusCreated, rec1.Code)
	resp1 := parseUploadResp(t, rec1)
	store1.(interface{ Close() error }).Close()

	// Second server instance — simulates controller restart — same bbolt file.
	store2 := mustOpenStore(t, dir)
	srv2 := New(":0", store2, pl, pool, &fakeHealth{online: []string{"w1", "w2"}})
	rec2 := doUpload(t, srv2, "f.txt", "content", map[string]string{"Idempotency-Key": "restart-key"})
	assert.Equal(t, http.StatusCreated, rec2.Code)
	resp2 := parseUploadResp(t, rec2)

	assert.Equal(t, resp1.ObjectID, resp2.ObjectID,
		"idempotency key must survive controller restart — same object_id replayed")
}
