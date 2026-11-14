// End-to-end tests for MiniDFS.
// Every component is real: controller (bbolt + placement + health), 4 worker
// HTTP servers (real chunk storage on disk), and the workerclient pool.
// Chunks travel over real HTTP — nothing is mocked.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	controllerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/health"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	workerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── cluster setup ─────────────────────────────────────────────────────────────

type workerNode struct {
	id      string
	srv     *httptest.Server
	store   *storage.Store
	dataDir string
}

type cluster struct {
	controller *httptest.Server
	workers    []*workerNode
	meta       *metadata.MetadataStore
	ring       *placement.Ring
	table      *health.LeaseTable
	cancel     context.CancelFunc
}

func newCluster(t *testing.T, workerCount int) *cluster {
	t.Helper()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)

	ring := placement.NewRing()
	pool := workerclient.New()
	table := health.NewLeaseTable()
	mon := health.NewMonitor(table)

	ctx, cancel := context.WithCancel(context.Background())
	mon.Start(ctx)
	go watchOffline(ctx, table, ring)

	ctrlSrv := controllerapi.New(":0", store, ring, pool, table)
	ctrl := httptest.NewServer(ctrlSrv.Handler())

	t.Cleanup(func() {
		cancel()
		ctrl.Close()
		store.Close()
	})

	c := &cluster{
		controller: ctrl,
		meta:       store,
		ring:       ring,
		table:      table,
		cancel:     cancel,
	}

	for i := 1; i <= workerCount; i++ {
		c.addWorker(t, fmt.Sprintf("worker%d", i))
	}
	return c
}

func (c *cluster) addWorker(t *testing.T, id string) *workerNode {
	t.Helper()

	dataDir := t.TempDir()
	chunkStore, err := storage.New(dataDir)
	require.NoError(t, err)

	wSrv := workerapi.New(chunkStore, id, "", &http.Client{Timeout: 10 * time.Second})
	srv := httptest.NewServer(wSrv.Handler())

	addr := strings.TrimPrefix(srv.URL, "http://")
	host := addr[:strings.LastIndex(addr, ":")]
	portStr := addr[strings.LastIndex(addr, ":")+1:]
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	t.Cleanup(func() { srv.Close() })

	regBody, _ := json.Marshal(models.RegisterRequest{
		WorkerID: id, Address: host, Port: port, CapacityBytes: 10 << 30,
	})
	resp, err := http.Post(c.controller.URL+"/internal/workers/register",
		"application/json", bytes.NewReader(regBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "worker %s registration failed", id)
	resp.Body.Close()

	node := &workerNode{id: id, srv: srv, store: chunkStore, dataDir: dataDir}
	c.workers = append(c.workers, node)
	t.Logf("worker %s registered at %s", id, addr)
	return node
}

func (c *cluster) upload(t *testing.T, filename string, data []byte) uploadResp {
	t.Helper()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	fw.Write(data)
	mw.Close()

	resp, err := http.Post(c.controller.URL+"/api/v1/objects", mw.FormDataContentType(), &buf)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "upload failed")

	var r uploadResp
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	t.Logf("uploaded %s → object_id=%s chunks=%d bytes=%d", filename, r.ObjectID, r.Chunks, r.Bytes)
	return r
}

func (c *cluster) download(t *testing.T, objectID string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(c.controller.URL + "/api/v1/objects/" + objectID)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	t.Logf("download object_id=%s status=%d bytes=%d", objectID, resp.StatusCode, len(body))
	return resp.StatusCode, body
}

type uploadResp struct {
	ObjectID string `json:"object_id"`
	Chunks   int    `json:"chunks"`
	Bytes    int64  `json:"size_bytes"`
}

func watchOffline(ctx context.Context, table *health.LeaseTable, ring *placement.Ring) {
	ch := table.Subscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			slog.Warn("e2e: removing offline worker", "worker_id", event.WorkerID)
			ring.RemoveWorker(event.WorkerID)
		}
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestE2E_Upload_Download_SmallFile(t *testing.T) {
	c := newCluster(t, 4)
	content := []byte("hello from real end-to-end test")

	r := c.upload(t, "hello.txt", content)
	assert.Equal(t, 1, r.Chunks)
	assert.Equal(t, int64(len(content)), r.Bytes)

	code, body := c.download(t, r.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, body)
}

func TestE2E_Upload_Download_MultiChunk(t *testing.T) {
	c := newCluster(t, 4)

	// 3 MB = 3 chunks of 1 MB each
	content := make([]byte, 3*1024*1024)
	rand.Read(content)

	r := c.upload(t, "big.bin", content)
	assert.Equal(t, 3, r.Chunks)

	code, body := c.download(t, r.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, body, "downloaded content must exactly match uploaded bytes")
}

func TestE2E_ChunksStoredOnTwoWorkers(t *testing.T) {
	c := newCluster(t, 4)
	content := []byte("replication check")

	r := c.upload(t, "rep.txt", content)

	refs, err := c.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)
	require.Len(t, refs, 1)

	ref := refs[0]
	t.Logf("chunk_id=%s primary=%s secondary=%s", ref.ChunkID, ref.PrimaryID, ref.SecondaryID)

	assert.NotEmpty(t, ref.PrimaryID)
	assert.NotEmpty(t, ref.SecondaryID)
	assert.NotEqual(t, ref.PrimaryID, ref.SecondaryID, "primary and secondary must be different workers")
}

func TestE2E_Download_AfterPrimaryWorkerStopped(t *testing.T) {
	c := newCluster(t, 4)
	content := []byte("survive worker failure")

	r := c.upload(t, "failover.txt", content)

	refs, err := c.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)
	primary := refs[0].PrimaryID

	for _, w := range c.workers {
		if w.id == primary {
			t.Logf("stopping primary worker %s", w.id)
			w.srv.Close()
			break
		}
	}

	code, body := c.download(t, r.ObjectID)
	require.Equal(t, http.StatusOK, code, "download must succeed after primary worker stops")
	assert.Equal(t, content, body)
	t.Log("failover to secondary confirmed")
}

func TestE2E_CAS_Dedup_SameContent(t *testing.T) {
	c := newCluster(t, 4)
	content := []byte("deduplicated bytes")

	r1 := c.upload(t, "file1.txt", content)
	r2 := c.upload(t, "file2.txt", content)

	assert.NotEqual(t, r1.ObjectID, r2.ObjectID, "different objects even with same content")

	_, b1 := c.download(t, r1.ObjectID)
	_, b2 := c.download(t, r2.ObjectID)
	assert.Equal(t, content, b1)
	assert.Equal(t, content, b2)

	refs1, _ := c.meta.GetChunkRefs(context.Background(), r1.ObjectID)
	refs2, _ := c.meta.GetChunkRefs(context.Background(), r2.ObjectID)
	assert.Equal(t, refs1[0].ChunkID, refs2[0].ChunkID, "same content → same chunk_id (CAS dedup)")
	t.Logf("both objects share chunk_id=%s", refs1[0].ChunkID)
}

func TestE2E_Delete_ObjectReturns404(t *testing.T) {
	c := newCluster(t, 4)
	content := []byte("delete me")

	r := c.upload(t, "del.txt", content)

	req, _ := http.NewRequest(http.MethodDelete, c.controller.URL+"/api/v1/objects/"+r.ObjectID, nil)
	delResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	delResp.Body.Close()
	require.Equal(t, http.StatusNoContent, delResp.StatusCode)

	code, _ := c.download(t, r.ObjectID)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestE2E_Metadata_GetAndPatch(t *testing.T) {
	c := newCluster(t, 4)
	content := []byte("metadata test")

	r := c.upload(t, "meta.txt", content)

	resp, err := http.Get(c.controller.URL + "/api/v1/objects/" + r.ObjectID + "/metadata")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var manifest models.ObjectManifest
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&manifest))
	assert.Equal(t, "meta.txt", manifest.Name)
	assert.Equal(t, models.StatusCommitted, manifest.Status)

	patch, _ := json.Marshal(map[string]string{"env": "test", "owner": "krishna"})
	req, _ := http.NewRequest(http.MethodPatch,
		c.controller.URL+"/api/v1/objects/"+r.ObjectID+"/metadata",
		bytes.NewReader(patch))
	req.Header.Set("Content-Type", "application/json")
	patchResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer patchResp.Body.Close()
	require.Equal(t, http.StatusOK, patchResp.StatusCode)

	var updated models.ObjectManifest
	require.NoError(t, json.NewDecoder(patchResp.Body).Decode(&updated))
	assert.Equal(t, "test", updated.CustomMetadata["env"])
	assert.Equal(t, "krishna", updated.CustomMetadata["owner"])
}

func TestE2E_List_Objects(t *testing.T) {
	c := newCluster(t, 4)

	c.upload(t, "a.txt", []byte("aaa"))
	c.upload(t, "b.txt", []byte("bbb"))
	c.upload(t, "c.txt", []byte("ccc"))

	resp, err := http.Get(c.controller.URL + "/api/v1/objects")
	require.NoError(t, err)
	defer resp.Body.Close()

	var objects []*models.ObjectManifest
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&objects))
	assert.Len(t, objects, 3)
}

func TestE2E_LargeFile_5MB(t *testing.T) {
	c := newCluster(t, 4)

	content := make([]byte, 5*1024*1024)
	rand.Read(content)

	r := c.upload(t, "large.bin", content)
	assert.Equal(t, 5, r.Chunks)

	code, body := c.download(t, r.ObjectID)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, content, body, "5 MB round-trip must be byte-perfect")
}

func TestE2E_Download_NonExistent_Returns404(t *testing.T) {
	c := newCluster(t, 4)
	code, _ := c.download(t, "doesnotexist")
	assert.Equal(t, http.StatusNotFound, code)
}
