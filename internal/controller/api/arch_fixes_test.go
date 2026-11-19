package api

// Tests for the three architectural gap fixes:
//   Fix 1 — both replica workers offline → download returns 503 (not hang/502)
//   Fix 2 — rebalancer prefers ring-preferred secondary (tested in rebalancer pkg)
//   Fix 3 — stale workers with WorkerOffline status in bbolt are skipped on
//            ring pre-population (tested via metadata store + filter logic)

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── Fix 1: two offline replicas → 503 ─────────────────────────────────────────

// bothOfflineHealth marks every worker as OFFLINE.
type bothOfflineHealth struct{}

func (bothOfflineHealth) RecordHeartbeat(_ string)                     {}
func (bothOfflineHealth) GetWorkerStatus(_ string) models.WorkerStatus { return models.WorkerOffline }
func (bothOfflineHealth) ListOnlineWorkers() []string                  { return []string{"w1", "w2"} }
func (bothOfflineHealth) Subscribe() <-chan models.WorkerOfflineEvent  { return nil }

// uploadObjectForTest uploads a small file through the full HTTP stack and
// returns the object ID. Requires at least 2 workers in the fake pool.
func uploadObjectForTest(t *testing.T, srv *Server, pool *fakeWorkerPool, workerAddr string) string {
	t.Helper()
	content := []byte("hello world")
	chunkID := sha256Hex(content)
	pool.chunks[key(workerAddr, chunkID)] = content

	var buf strings.Builder
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "test.txt")
	require.NoError(t, err)
	_, err = io.WriteString(fw, "hello world")
	require.NoError(t, err)
	mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects",
		strings.NewReader(buf.String()))
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, "upload must succeed: %s", rec.Body)

	var resp struct {
		ObjectID string `json:"object_id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.ObjectID, "upload response must contain object_id")
	return resp.ObjectID
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestDownload_BothReplicasOffline_Returns503(t *testing.T) {
	dir := t.TempDir()
	store := mustOpenStore(t, dir)
	pool := newFakeWorkerPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}

	// Upload with normal health so the object is committed.
	uploadSrv := New(":0", store, pl, pool, &fakeHealth{online: []string{"w1", "w2"}})
	objID := uploadObjectForTest(t, uploadSrv, pool, "w1")

	// Now create a download server whose health monitor says both workers are offline.
	dlSrv := New(":0", store, pl, pool, bothOfflineHealth{})

	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/api/v1/objects/%s", objID), nil)
	rec := httptest.NewRecorder()
	dlSrv.http.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"should return 503 when both replica workers are offline, got: %s", rec.Body)
	assert.Contains(t, rec.Body.String(), "offline")
}

func TestDownload_OneReplicaOffline_FallsBackToSecondary(t *testing.T) {
	dir := t.TempDir()
	store := mustOpenStore(t, dir)
	pool := newFakeWorkerPool()
	pl := &fakePlacement{workers: []string{"w1", "w2"}}

	// Upload through normal health.
	uploadSrv := New(":0", store, pl, pool, &fakeHealth{online: []string{"w1", "w2"}})
	objID := uploadObjectForTest(t, uploadSrv, pool, "w1")

	// w1 (primary) is offline; w2 (secondary) is online and holds the chunk.
	// We need to also put the chunk on w2 for fallback to succeed.
	for k, v := range pool.chunks {
		if strings.HasPrefix(k, "w1|") {
			pool.chunks["w2|"+k[3:]] = v
		}
	}

	partialOffline := &fakeHealth{online: []string{"w2"}}
	dlSrv := New(":0", store, pl, pool, partialOffline)

	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/api/v1/objects/%s", objID), nil)
	rec := httptest.NewRecorder()
	dlSrv.http.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code,
		"should still succeed via secondary when only primary is offline")
}

// ── Fix 3: stale offline workers skipped on ring pre-population ───────────────

func TestMetadata_UpdateWorkerStatus_PersistsOffline(t *testing.T) {
	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ctx := t.Context()
	worker := &models.WorkerRecord{
		WorkerID: "w1",
		Address:  "localhost",
		Port:     9001,
		Status:   models.WorkerOnline,
	}
	require.NoError(t, store.SaveWorker(ctx, worker))

	// Simulate watchOffline persisting status.
	require.NoError(t, store.UpdateWorkerStatus(ctx, "w1", models.WorkerOffline))

	got, err := store.GetWorker(ctx, "w1")
	require.NoError(t, err)
	assert.Equal(t, models.WorkerOffline, got.Status,
		"offline status must be persisted to bbolt")
}

func TestMetadata_ListWorkers_OfflineFilterSkipsStaleWorkers(t *testing.T) {
	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ctx := t.Context()

	for i, status := range []models.WorkerStatus{
		models.WorkerOnline,
		models.WorkerOffline, // stale — was never seen again after last controller restart
		models.WorkerOnline,
	} {
		w := &models.WorkerRecord{
			WorkerID: fmt.Sprintf("w%d", i+1),
			Address:  "localhost",
			Port:     9000 + i + 1,
			Status:   status,
		}
		require.NoError(t, store.SaveWorker(ctx, w))
	}

	workers, err := store.ListWorkers(ctx)
	require.NoError(t, err)

	// Reproduce the ring pre-population filter from main.go.
	var added []string
	for _, w := range workers {
		if w.Status == models.WorkerOffline {
			continue
		}
		added = append(added, w.WorkerID)
	}

	assert.ElementsMatch(t, []string{"w1", "w3"}, added,
		"only online workers should be added to the ring on restart")
	assert.NotContains(t, added, "w2",
		"worker with persisted offline status must be skipped")
}
