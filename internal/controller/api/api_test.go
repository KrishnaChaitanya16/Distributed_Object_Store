package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// --- fakes ---

type fakeWorkerPool struct {
	chunks map[string][]byte // workerAddr+chunkID → data
}

func newFakeWorkerPool() *fakeWorkerPool {
	return &fakeWorkerPool{chunks: make(map[string][]byte)}
}

func key(addr, id string) string { return addr + "|" + id }

func (f *fakeWorkerPool) PutChunk(_ context.Context, workerAddr, chunkID string, data io.Reader, _ int64) error {
	b, _ := io.ReadAll(data)
	f.chunks[key(workerAddr, chunkID)] = b
	return nil
}
func (f *fakeWorkerPool) GetChunk(_ context.Context, workerAddr, chunkID string) (io.ReadCloser, error) {
	b, ok := f.chunks[key(workerAddr, chunkID)]
	if !ok {
		return nil, models.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (f *fakeWorkerPool) DeleteChunk(_ context.Context, _, _ string) error { return nil }
func (f *fakeWorkerPool) ReplicatePeer(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeWorkerPool) ListChunks(_ context.Context, _ string) ([]string, error) { return nil, nil }

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
func (f *fakePlacement) AddWorker(_, _ string)  {}
func (f *fakePlacement) RemoveWorker(_ string)  {}
func (f *fakePlacement) Workers() []string       { return f.workers }

type fakeHealth struct{}

func (f *fakeHealth) RecordHeartbeat(_ string)                              {}
func (f *fakeHealth) GetWorkerStatus(_ string) models.WorkerStatus         { return models.WorkerOnline }
func (f *fakeHealth) ListOnlineWorkers() []string                          { return nil }
func (f *fakeHealth) Subscribe() <-chan models.WorkerOfflineEvent           { return nil }

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

	req := httptest.NewRequest(http.MethodPost, "/objects", body)
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
	req := httptest.NewRequest(http.MethodPost, "/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)

	var upResp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&upResp))

	// download
	req2 := httptest.NewRequest(http.MethodGet, "/objects/"+upResp.ObjectID, nil)
	rec2 := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)

	got, _ := io.ReadAll(rec2.Body)
	assert.Equal(t, content, string(got))
}

func TestDownload_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/objects/doesnotexist", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestList_ReturnsCommittedObjects(t *testing.T) {
	srv, _ := newTestServer(t)

	// upload two objects
	for _, name := range []string{"a.txt", "b.txt"} {
		body, ct := multipartBody(t, name, "content")
		req := httptest.NewRequest(http.MethodPost, "/objects", body)
		req.Header.Set("Content-Type", ct)
		rec := httptest.NewRecorder()
		srv.http.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects", nil)
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
	req := httptest.NewRequest(http.MethodPost, "/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)

	var upResp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&upResp))

	// delete
	req2 := httptest.NewRequest(http.MethodDelete, "/objects/"+upResp.ObjectID, nil)
	rec2 := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// download should now 404
	req3 := httptest.NewRequest(http.MethodGet, "/objects/"+upResp.ObjectID, nil)
	rec3 := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec3, req3)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestDelete_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodDelete, "/objects/ghost", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
