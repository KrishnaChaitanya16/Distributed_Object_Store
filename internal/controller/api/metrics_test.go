package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// gatherDelta returns how much a counter changed across fn().
func gatherDelta(t *testing.T, c prometheus.Counter, fn func()) float64 {
	t.Helper()
	before := testutil.ToFloat64(c)
	fn()
	return testutil.ToFloat64(c) - before
}

func TestMetrics_UploadIncrementsChunksNew(t *testing.T) {
	srv, _ := newTestServer(t)
	delta := gatherDelta(t, metrics.ChunksNewTotal, func() {
		body, ct := multipartBody(t, "new.txt", "unique content abc123")
		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", ct)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)
	})
	assert.Equal(t, float64(1), delta, "one new chunk should be recorded")
}

func TestMetrics_DedupIncrements(t *testing.T) {
	srv, _ := newTestServer(t)
	content := "deduplicated content xyz987"

	// First upload — new chunk.
	uploadOne := func() string {
		body, ct := multipartBody(t, "dup.txt", content)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", ct)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)
		var resp uploadResponse
		json.NewDecoder(rec.Body).Decode(&resp)
		return resp.ObjectID
	}
	uploadOne()

	delta := gatherDelta(t, metrics.ChunksDedupTotal, func() {
		uploadOne()
	})
	assert.Equal(t, float64(1), delta, "second identical upload should hit dedup counter")
}

func TestMetrics_UploadBytesAccumulate(t *testing.T) {
	srv, _ := newTestServer(t)
	content := "bytes accumulation test content"
	delta := gatherDelta(t, metrics.UploadBytesTotal, func() {
		body, ct := multipartBody(t, "bytes.txt", content)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", ct)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)
	})
	assert.Equal(t, float64(len(content)), delta)
}

func TestMetrics_DownloadBytesAccumulate(t *testing.T) {
	srv, _ := newTestServer(t)
	content := "download bytes test content"

	// Upload first.
	body, ct := multipartBody(t, "dl.txt", content)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp uploadResponse
	json.NewDecoder(rec.Body).Decode(&resp)

	delta := gatherDelta(t, metrics.DownloadBytesTotal, func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID, nil)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})
	assert.Equal(t, float64(len(content)), delta)
}

func TestMetrics_ReadRepairIncrementsOnHashMismatch(t *testing.T) {
	srv, pool := newTestServer(t)
	content := "read repair metric test payload"

	// Upload.
	body, ct := multipartBody(t, "rr.txt", content)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp uploadResponse
	json.NewDecoder(rec.Body).Decode(&resp)

	// Corrupt the primary replica so SHA-256 mismatch fires.
	objs, err := srv.meta.ListObjects(context.Background())
	require.NoError(t, err)
	require.Len(t, objs, 1)
	refs, err := srv.meta.GetChunkRefs(context.Background(), objs[0].ID)
	require.NoError(t, err)
	require.NotEmpty(t, refs)

	// w1 → "localhost:9001" (set up in newTestServer)
	primaryKey := key("localhost:9001", refs[0].ChunkID)
	original := pool.chunks[primaryKey]
	require.NotNil(t, original, "primary chunk should exist in pool")
	corrupted := make([]byte, len(original))
	copy(corrupted, original)
	corrupted[0] ^= 0xFF
	pool.chunks[primaryKey] = corrupted

	delta := gatherDelta(t, metrics.ReadRepairsTotal, func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID, nil)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		// Still 200 — served from secondary.
		assert.Equal(t, http.StatusOK, rec.Code)
	})
	assert.Equal(t, float64(1), delta, "read repair counter should increment on hash mismatch")
}

// TestMetrics_BloomFilterRebuildPreservesDedup verifies that after a controller
// is restarted (new Server wired to same bbolt store), the Bloom filter is
// rebuilt and a re-upload of the same content still hits dedup.
func TestMetrics_BloomFilterRebuildPreservesDedup(t *testing.T) {
	dir := t.TempDir()
	store := mustOpenStore(t, dir)
	pool := newFakeWorkerPool()
	placement := &fakePlacement{workers: []string{"w1", "w2"}}

	ctx := context.Background()
	_ = store.SaveWorker(ctx, &models.WorkerRecord{WorkerID: "w1", Address: "localhost", Port: 9001})
	_ = store.SaveWorker(ctx, &models.WorkerRecord{WorkerID: "w2", Address: "localhost", Port: 9002})

	srv1 := New(":0", store, placement, pool, &fakeHealth{})
	srv1.PopulateBloom(ctx)

	content := "bloom rebuild dedup test unique xyz"

	// Upload on server 1.
	uploadFn := func(srv *Server) {
		body, ct := multipartBody(t, "bloom.txt", content)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", ct)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)
	}
	uploadFn(srv1)

	// Simulate controller restart: same store, new Server, re-populate bloom.
	srv2 := New(":0", store, placement, pool, &fakeHealth{})
	srv2.PopulateBloom(ctx)

	// Re-upload same content on server 2 — should hit dedup.
	delta := gatherDelta(t, metrics.ChunksDedupTotal, func() {
		uploadFn(srv2)
	})
	assert.Equal(t, float64(1), delta, "dedup should fire after bloom rebuild from bbolt")
}
