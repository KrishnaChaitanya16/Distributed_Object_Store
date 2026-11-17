// Controller dead / crash e2e tests.
// All scenarios use real nodes (real HTTP, real bbolt, real workers on disk).
// The controller is "killed" by closing its httptest.Server and "restarted"
// by wiring up a brand-new controllerapi.Server to the same bbolt DB, same
// hash ring, same worker pool, and same health table.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	controllerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// crashCluster wraps cluster and adds the ability to kill and restart the
// controller, plus a GC reconciler for direct-pass testing.
type crashCluster struct {
	*cluster
	recon *gc.Reconciler
	cpool *workerclient.Pool
}

func newCrashCluster(t *testing.T, workerCount int) *crashCluster {
	t.Helper()
	c := newCluster(t, workerCount)

	cpool := workerclient.New()
	col := gc.NewCollector(c.meta, cpool, c.table)
	recon, err := gc.NewReconciler(c.meta, cpool, c.table, col, gc.Config{
		Interval:      24 * time.Hour, // never fires automatically
		StagedTimeout: 10 * time.Minute,
		OrphanGrace:   0,
	})
	require.NoError(t, err)

	return &crashCluster{cluster: c, recon: recon, cpool: cpool}
}

// killController simulates the controller process dying: closes the HTTP
// server but keeps bbolt, ring, health table, and workers alive.
func (cc *crashCluster) killController(t *testing.T) {
	t.Helper()
	cc.cluster.controller.Close()
}

// restartController brings a new controller HTTP server online using the
// same persistent state (bbolt, ring, health table, worker pool).
func (cc *crashCluster) restartController(t *testing.T) {
	t.Helper()
	newSrv := controllerapi.New(":0", cc.meta, cc.ring, cc.cpool, cc.table)
	ctrl := httptest.NewServer(newSrv.Handler())
	t.Cleanup(func() { ctrl.Close() })
	cc.cluster.controller = ctrl
	t.Logf("controller restarted at %s", ctrl.URL)
}

// triggerRecon runs one reconciler pass synchronously.
func (cc *crashCluster) triggerRecon(t *testing.T) gc.Report {
	t.Helper()
	report := cc.recon.Pass(context.Background())
	t.Logf("reconciler: workers_scanned=%d replicas_restored=%d stale_uploads=%d lost=%d",
		report.WorkersScanned, report.ReplicasRestored, report.StaleUploadsFailed, report.LostChunks)
	return report
}

// uploadWithHeaderFull posts a multipart upload with extra HTTP headers and
// returns the full *http.Response (caller must close the body).
func (cc *crashCluster) uploadWithHeaderFull(t *testing.T, filename string, data []byte, headers map[string]string) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	fw.Write(data)
	mw.Close()

	req, err := http.NewRequest(http.MethodPost, cc.controller.URL+"/api/v1/objects", &buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestE2E_ControllerDead_DownloadAfterRestart verifies that after the controller
// process is killed and restarted with the same bbolt DB, all uploaded objects
// are still downloadable with correct content.
func TestE2E_ControllerDead_DownloadAfterRestart(t *testing.T) {
	cc := newCrashCluster(t, 4)

	content := bytes.Repeat([]byte("hello-restart"), 50)
	up := cc.upload(t, "persist.bin", content)
	require.NotEmpty(t, up.ObjectID)

	cc.killController(t)
	cc.restartController(t)

	status, got := cc.download(t, up.ObjectID)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, content, got, "content must survive controller restart")
}

// TestE2E_ControllerDead_MultipleObjectsSurviveRestart uploads several objects,
// kills the controller, restarts it, and verifies every object is still
// downloadable and listed.
func TestE2E_ControllerDead_MultipleObjectsSurviveRestart(t *testing.T) {
	cc := newCrashCluster(t, 4)

	type entry struct {
		id   string
		data []byte
	}
	var saved []entry
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("object-data-%d-padding-padding-padding-padding", i))
		up := cc.upload(t, fmt.Sprintf("file%d.txt", i), data)
		saved = append(saved, entry{id: up.ObjectID, data: data})
	}

	cc.killController(t)
	cc.restartController(t)

	for _, e := range saved {
		status, got := cc.download(t, e.id)
		assert.Equal(t, http.StatusOK, status, "object %s should survive restart", e.id)
		assert.Equal(t, e.data, got)
	}

	resp, err := http.Get(cc.controller.URL + "/api/v1/objects")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var objects []*models.ObjectManifest
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&objects))
	assert.Len(t, objects, 5)
}

// TestE2E_ControllerDead_StagedManifestExpiredOnRestart injects a STAGED
// manifest with a creation time 15 minutes in the past (older than the
// StagedTimeout of 10m), simulating a crash during upload that left the
// manifest dangling. After a reconciler pass the object must return 404.
func TestE2E_ControllerDead_StagedManifestExpiredOnRestart(t *testing.T) {
	cc := newCrashCluster(t, 4)

	staleID := "stale-staged-object-id"
	stale := &models.ObjectManifest{
		ID:         staleID,
		Name:       "stale.txt",
		SizeBytes:  100,
		Status:     models.StatusStaged,
		ChunkCount: 1,
		CreatedAt:  time.Now().Add(-15 * time.Minute),
		ModifiedAt: time.Now().Add(-15 * time.Minute),
	}
	require.NoError(t, cc.meta.SaveObject(context.Background(), stale))

	got, err := cc.meta.GetObject(context.Background(), staleID)
	require.NoError(t, err)
	require.Equal(t, models.StatusStaged, got.Status)

	cc.killController(t)
	cc.restartController(t)

	report := cc.triggerRecon(t)
	assert.Equal(t, 1, report.StaleUploadsFailed,
		"reconciler must expire 1 stale STAGED upload")

	// Stale STAGED manifest must be FAILED or purged — download returns 404.
	status, _ := cc.download(t, staleID)
	assert.Equal(t, http.StatusNotFound, status,
		"stale STAGED manifest must not be downloadable after reconciler pass")
}

// TestE2E_ControllerDead_DeletedChunksCleaned simulates a controller crash
// after the manifest was flipped to DELETED but before the GC sweep ran.
// After restart + reconciler pass, the object must remain 404.
func TestE2E_ControllerDead_DeletedChunksCleaned(t *testing.T) {
	cc := newCrashCluster(t, 4)

	content := []byte("delete-and-crash-test-content-here")
	up := cc.upload(t, "tobedeleted.txt", content)

	// Mark DELETED without sweeping chunks (simulates mid-delete crash).
	require.NoError(t, cc.meta.MarkObjectDeleted(context.Background(), up.ObjectID))

	cc.killController(t)
	cc.restartController(t)

	// DELETED → 404 immediately after restart.
	status, _ := cc.download(t, up.ObjectID)
	assert.Equal(t, http.StatusNotFound, status)

	// Reconciler finishes the cleanup.
	cc.triggerRecon(t)

	status2, _ := cc.download(t, up.ObjectID)
	assert.Equal(t, http.StatusNotFound, status2)
}

// TestE2E_ControllerDead_IdempotencyKeyReplayAfterRestart uploads with an
// idempotency key, kills the controller, restarts it, then re-sends the same
// upload — the response must carry the same object_id and
// X-Idempotency-Replayed: true.
func TestE2E_ControllerDead_IdempotencyKeyReplayAfterRestart(t *testing.T) {
	cc := newCrashCluster(t, 4)

	content := []byte("idempotent-upload-data-padding-padding")
	const ikey = "restart-idempotency-key-abc123"

	resp1 := cc.uploadWithHeaderFull(t, "idem.txt", content, map[string]string{
		"Idempotency-Key": ikey,
	})
	defer resp1.Body.Close()
	require.Equal(t, http.StatusCreated, resp1.StatusCode)
	var up1 uploadResp
	require.NoError(t, json.NewDecoder(resp1.Body).Decode(&up1))

	cc.killController(t)
	cc.restartController(t)

	resp2 := cc.uploadWithHeaderFull(t, "idem.txt", content, map[string]string{
		"Idempotency-Key": ikey,
	})
	defer resp2.Body.Close()
	require.Equal(t, http.StatusCreated, resp2.StatusCode)
	var up2 uploadResp
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&up2))

	assert.Equal(t, up1.ObjectID, up2.ObjectID,
		"idempotency key must survive controller restart — same object_id on replay")
	assert.Equal(t, "true", resp2.Header.Get("X-Idempotency-Replayed"),
		"replayed response must carry X-Idempotency-Replayed: true")
}

// TestE2E_ControllerDead_WorkersKeepChunks_AfterRestart confirms that after a
// controller restart the chunk-to-worker mapping in bbolt is unchanged and
// download still returns the correct bytes.
func TestE2E_ControllerDead_WorkersKeepChunks_AfterRestart(t *testing.T) {
	cc := newCrashCluster(t, 4)

	content := bytes.Repeat([]byte("chunk-persistence-check"), 100)
	up := cc.upload(t, "chunks.bin", content)

	refs, err := cc.meta.GetChunkRefs(context.Background(), up.ObjectID)
	require.NoError(t, err)
	require.NotEmpty(t, refs)
	primaryBefore := refs[0].PrimaryID
	secondaryBefore := refs[0].SecondaryID

	cc.killController(t)
	cc.restartController(t)

	status, got := cc.download(t, up.ObjectID)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, content, got)

	refsAfter, err := cc.meta.GetChunkRefs(context.Background(), up.ObjectID)
	require.NoError(t, err)
	require.NotEmpty(t, refsAfter)
	assert.Equal(t, primaryBefore, refsAfter[0].PrimaryID, "primary worker unchanged after restart")
	assert.Equal(t, secondaryBefore, refsAfter[0].SecondaryID, "secondary worker unchanged after restart")
}

// TestE2E_ControllerDead_MultipleRestarts verifies that the system stays
// consistent across several successive kill-and-restart cycles.
func TestE2E_ControllerDead_MultipleRestarts(t *testing.T) {
	cc := newCrashCluster(t, 4)

	content := []byte("multi-restart-resilience-test-padding")
	up := cc.upload(t, "resilient.txt", content)

	for i := 0; i < 3; i++ {
		cc.killController(t)
		cc.restartController(t)

		status, got := cc.download(t, up.ObjectID)
		assert.Equal(t, http.StatusOK, status,
			"download must work after restart #%d", i+1)
		assert.Equal(t, content, got,
			"content must be correct after restart #%d", i+1)
	}
}

// TestE2E_ControllerDead_UploadWhileDown_ThenRestartAndDownload confirms that
// objects committed before the crash are intact after the controller comes back.
func TestE2E_ControllerDead_UploadWhileDown_ThenRestartAndDownload(t *testing.T) {
	cc := newCrashCluster(t, 4)

	content := []byte("before-crash-content-padding-padding")
	up := cc.upload(t, "before.txt", content)

	deadURL := cc.controller.URL
	cc.killController(t)

	// Upload attempt against dead controller must fail or return non-201.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", "fail.txt")
	fw.Write([]byte("should-fail"))
	mw.Close()
	resp, err := http.Post(deadURL+"/api/v1/objects", mw.FormDataContentType(), &buf)
	if err == nil {
		resp.Body.Close()
		assert.NotEqual(t, http.StatusCreated, resp.StatusCode,
			"upload to dead controller must not return 201")
	}
	// Connection refused is the normal case — either outcome is fine.

	cc.restartController(t)
	status, got := cc.download(t, up.ObjectID)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, content, got, "pre-crash object must survive controller death")
}

// TestE2E_ControllerDead_ReconcilerRestoresAfterRestart uploads a file, manually
// deletes the chunk file from one worker's disk, kills the controller, restarts
// it, and verifies the reconciler can repair the missing replica.
func TestE2E_ControllerDead_ReconcilerRestoresAfterRestart(t *testing.T) {
	cc := newCrashCluster(t, 4)

	content := bytes.Repeat([]byte("repair-after-restart-test"), 50)
	up := cc.upload(t, "repair.bin", content)

	refs, err := cc.meta.GetChunkRefs(context.Background(), up.ObjectID)
	require.NoError(t, err)
	require.NotEmpty(t, refs)

	// Find the primary worker node and delete the chunk file from disk.
	var primaryNode *workerNode
	for _, w := range cc.workers {
		if w.id == refs[0].PrimaryID {
			primaryNode = w
			break
		}
	}
	require.NotNil(t, primaryNode, "primary worker not found")

	chunkFile := primaryNode.dataDir + "/chunks/" + refs[0].ChunkID + ".bin"
	require.NoError(t, os.Remove(chunkFile), "must remove chunk file to simulate disk loss")

	cc.killController(t)
	cc.restartController(t)

	// Reconciler should detect the missing chunk on the online primary worker
	// and replicate from the secondary.
	report := cc.triggerRecon(t)
	assert.GreaterOrEqual(t, report.ReplicasRestored, 1,
		"reconciler must repair at least 1 chunk after restart")

	// Download must still work (served from repaired replica).
	status, got := cc.download(t, up.ObjectID)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, content, got)
}
