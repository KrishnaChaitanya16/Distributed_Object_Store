package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// --- fakes ---

type fakeWorkerPool struct {
	mu     sync.Mutex
	chunks map[string][]byte // workerAddr+chunkID → data
}

func newFakeWorkerPool() *fakeWorkerPool {
	return &fakeWorkerPool{chunks: make(map[string][]byte)}
}

func key(addr, id string) string { return addr + "|" + id }

func (f *fakeWorkerPool) PutChunk(_ context.Context, workerAddr, chunkID string, data io.Reader, _ int64) error {
	b, _ := io.ReadAll(data)
	f.mu.Lock()
	f.chunks[key(workerAddr, chunkID)] = b
	f.mu.Unlock()
	return nil
}
func (f *fakeWorkerPool) GetChunk(_ context.Context, workerAddr, chunkID string) (io.ReadCloser, error) {
	f.mu.Lock()
	b, ok := f.chunks[key(workerAddr, chunkID)]
	f.mu.Unlock()
	if !ok {
		return nil, models.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (f *fakeWorkerPool) DeleteChunk(_ context.Context, _, _ string) error      { return nil }
func (f *fakeWorkerPool) ReplicatePeer(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeWorkerPool) ListChunks(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

type fakePlacement struct{ workers []string }

func (f *fakePlacement) Primary(_ string) (string, error) {
	if len(f.workers) == 0 {
		return "", models.ErrNoWorkers
	}
	return f.workers[0], nil
}
func (f *fakePlacement) Secondary(_ string, exclude string) (string, error) {
	for _, w := range f.workers {
		if w != exclude {
			return w, nil
		}
	}
	return "", models.ErrNoWorkers
}
func (f *fakePlacement) AddWorker(_, _ string) {}
func (f *fakePlacement) RemoveWorker(_ string) {}
func (f *fakePlacement) Workers() []string     { return f.workers }

// fakeHealth reports four online workers unless online is set; an empty non-nil slice means none are online.
type fakeHealth struct{ online []string }

func (f *fakeHealth) RecordHeartbeat(_ string)                     {}
func (f *fakeHealth) GetWorkerStatus(_ string) models.WorkerStatus { return models.WorkerOnline }
func (f *fakeHealth) ListOnlineWorkers() []string {
	if f.online == nil {
		return []string{"w1", "w2", "w3", "w4"}
	}
	return f.online
}
func (f *fakeHealth) Subscribe() <-chan models.WorkerOfflineEvent { return nil }

// --- helpers ---

func newTestServer(t *testing.T) (*Server, *fakeWorkerPool) {
	t.Helper()

	dir := t.TempDir()
	store := mustOpenStore(t, dir)

	pool := newFakeWorkerPool()
	placement := &fakePlacement{workers: []string{"w1", "w2"}}

	// register workers so workerAddr() resolves
	ctx := context.Background()
	store.SaveWorker(ctx, &models.WorkerRecord{WorkerID: "w1", Address: "localhost", Port: 9001})
	store.SaveWorker(ctx, &models.WorkerRecord{WorkerID: "w2", Address: "localhost", Port: 9002})

	srv := New(":0", store, placement, pool, &fakeHealth{})
	return srv, pool
}

func mustOpenStore(t *testing.T, dir string) models.MetadataRepository {
	t.Helper()
	// import the metadata package via its exported constructor
	store, err := openMetadataStore(dir + "/test.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.(io.Closer).Close() })
	return store
}

func multipartBody(t *testing.T, filename, content string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("file", filename)
	require.NoError(t, err)
	io.WriteString(fw, content)
	w.Close()
	return &buf, w.FormDataContentType()
}

// --- tests ---

func TestUpload_SmallFile(t *testing.T) {
	srv, _ := newTestServer(t)
	body, ct := multipartBody(t, "hello.txt", "hello world")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()

	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp.ObjectID)
	assert.Equal(t, 1, resp.Chunks)
	assert.Equal(t, int64(len("hello world")), resp.Bytes)
}

func TestUpload_Download_RoundTrip(t *testing.T) {
	srv, _ := newTestServer(t)
	content := strings.Repeat("abcdefgh", 100) // 800 bytes

	// upload
	body, ct := multipartBody(t, "data.bin", content)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)

	var upResp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&upResp))

	// download
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+upResp.ObjectID, nil)
	rec2 := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)

	got, _ := io.ReadAll(rec2.Body)
	assert.Equal(t, content, string(got))
}

func TestDownload_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/doesnotexist", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestList_ReturnsCommittedObjects(t *testing.T) {
	srv, _ := newTestServer(t)

	// upload two objects
	for _, name := range []string{"a.txt", "b.txt"} {
		body, ct := multipartBody(t, name, "content")
		req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
		req.Header.Set("Content-Type", ct)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var objects []*models.ObjectManifest
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&objects))
	assert.Len(t, objects, 2)
}

func TestDelete_RemovesObject(t *testing.T) {
	srv, _ := newTestServer(t)
	body, ct := multipartBody(t, "del.txt", "bye")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)

	var upResp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&upResp))

	// delete
	req2 := httptest.NewRequest(http.MethodDelete, "/api/v1/objects/"+upResp.ObjectID, nil)
	rec2 := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// download should now 404
	req3 := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+upResp.ObjectID, nil)
	rec3 := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec3, req3)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestDelete_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/objects/ghost", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUpload_InsufficientWorkers_Returns503(t *testing.T) {
	dir := t.TempDir()
	store := mustOpenStore(t, dir)
	pool := newFakeWorkerPool()
	placement := &fakePlacement{workers: []string{"w1", "w2"}}

	// Only 1 worker online — not enough for primary + secondary
	srv := New(":0", store, placement, pool, &fakeHealth{online: []string{"w1"}})

	body, ct := multipartBody(t, "f.txt", "hello")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "insufficient")
}

func TestUpload_NoWorkers_Returns503(t *testing.T) {
	dir := t.TempDir()
	store := mustOpenStore(t, dir)
	pool := newFakeWorkerPool()
	placement := &fakePlacement{workers: []string{}}

	srv := New(":0", store, placement, pool, &fakeHealth{online: []string{}})

	body, ct := multipartBody(t, "f.txt", "hello")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestUpload_LargeFileLeavesNoTempFilesBehind(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	srv, _ := newTestServer(t)
	body, ct := multipartBody(t, "big.bin", strings.Repeat("x", 33<<20))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)

	left, err := os.ReadDir(tmp)
	require.NoError(t, err)
	assert.Empty(t, left, "a file over 32 MiB is spooled to disk and must be removed when the request ends")
}

func TestUpload_RejectedLargeFileLeavesNoTempFilesBehind(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	srv, _ := newTestServer(t)
	body, ct := multipartBody(t, "big.bin", strings.Repeat("x", 33<<20))
	srv.health = &fakeHealth{online: []string{}}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	left, err := os.ReadDir(tmp)
	require.NoError(t, err)
	assert.Empty(t, left, "an upload turned away after parsing must not leave its spooled file")
}
