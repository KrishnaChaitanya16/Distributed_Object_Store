package api

// Tests for SHA-256 inline verification and read-repair (DESIGN §2.9, §2.10).
// Tests for chunk locations in the metadata endpoint (PDF §3).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── corruptable fake worker pool ─────────────────────────────────────────────

// corruptPool is a fake WorkerClientPool that can inject corrupt or error
// responses for specific worker+chunk pairs.
type corruptPool struct {
	mu      sync.Mutex
	chunks  map[string][]byte // "addr|chunkID" → stored bytes
	corrupt map[string][]byte // "addr|chunkID" → bytes to return instead (corrupt)
	errOn   map[string]error  // "addr|chunkID" → error to return
	repairs []repairCall
	calls   []string
}

type repairCall struct{ source, chunkID, target string }

func newCorruptPool() *corruptPool {
	return &corruptPool{
		chunks:  make(map[string][]byte),
		corrupt: make(map[string][]byte),
		errOn:   make(map[string]error),
	}
}

func (p *corruptPool) key(addr, id string) string { return addr + "|" + id }

func (p *corruptPool) PutChunk(_ context.Context, addr, id string, data io.Reader, _ int64) error {
	b, _ := io.ReadAll(data)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chunks[p.key(addr, id)] = b
	p.calls = append(p.calls, fmt.Sprintf("PUT %s %s", addr, id))
	return nil
}

func (p *corruptPool) GetChunk(_ context.Context, addr, id string) (io.ReadCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.key(addr, id)
	p.calls = append(p.calls, fmt.Sprintf("GET %s %s", addr, id))
	if err, ok := p.errOn[k]; ok {
		return nil, err
	}
	if corrupt, ok := p.corrupt[k]; ok {
		return io.NopCloser(bytes.NewReader(corrupt)), nil
	}
	if b, ok := p.chunks[k]; ok {
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	return nil, models.ErrNotFound
}

func (p *corruptPool) DeleteChunk(_ context.Context, addr, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.chunks, p.key(addr, id))
	return nil
}

func (p *corruptPool) ReplicatePeer(_ context.Context, sourceAddr, chunkID, targetAddr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repairs = append(p.repairs, repairCall{source: sourceAddr, chunkID: chunkID, target: targetAddr})
	return nil
}

func (p *corruptPool) ListChunks(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

// setCorrupt replaces what addr returns for chunkID with scrambled bytes of the same size.
func (p *corruptPool) setCorrupt(addr, chunkID string, size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	bad := make([]byte, size)
	for i := range bad {
		bad[i] = 0xFF
	}
	p.corrupt[p.key(addr, chunkID)] = bad
}

// setTruncated makes addr return fewer bytes than expected for chunkID.
func (p *corruptPool) setTruncated(addr, chunkID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.corrupt[p.key(addr, chunkID)] = []byte("short")
}

// setErr makes addr return an error for chunkID.
func (p *corruptPool) setErr(addr, chunkID string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errOn[p.key(addr, chunkID)] = err
}

func (p *corruptPool) repairCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.repairs)
}

// ── harness helpers ───────────────────────────────────────────────────────────

func newRepairHarness(t *testing.T) (*testHarness, *corruptPool) {
	t.Helper()
	cap := &logCapture{}

	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	ring := placement.NewRing()
	ring.AddWorker("w1", "localhost:9001")
	ring.AddWorker("w2", "localhost:9002")
	ring.AddWorker("w3", "localhost:9003")
	ring.AddWorker("w4", "localhost:9004")

	ctx := context.Background()
	for _, rec := range []struct {
		id   string
		port int
	}{
		{"w1", 9001}, {"w2", 9002}, {"w3", 9003}, {"w4", 9004},
	} {
		store.SaveWorker(ctx, &models.WorkerRecord{
			WorkerID: rec.id, Address: "localhost", Port: rec.port,
			Status: models.WorkerOnline, VnodeCount: 50,
		})
	}

	pool := newCorruptPool()
	srv := New(":0", store, ring, pool, &fakeHealth{})
	h := &testHarness{srv: srv, store: store, ring: ring, workers: nil, logs: cap}
	return h, pool
}

// uploadViaPool uploads content through the full upload handler using pool as the worker pool.
func uploadViaPool(t *testing.T, srv *Server, pool *corruptPool, filename, content string) uploadResponse {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	io.WriteString(fw, content)
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

func downloadViaPool(srv *Server, objectID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+objectID, nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

// chunkIDFor computes the expected chunk_id (SHA-256) for given content.
func chunkIDFor(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// ── SHA-256 verification + read-repair tests ─────────────────────────────────

func TestReadRepair_PrimaryCorrupt_FallsBackToSecondary(t *testing.T) {
	h, pool := newRepairHarness(t)
	content := "hello integrity check"
	resp := uploadViaPool(t, h.srv, pool, "f.txt", content)
	require.Equal(t, 1, resp.Chunks)

	refs, _ := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))
	pool.setCorrupt(primaryAddr, refs[0].ChunkID, refs[0].Size)

	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []byte(content), rec.Body.Bytes(), "downloaded bytes must be identical")

	// Wait for async repair goroutine
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, pool.repairCount(), "read-repair must be triggered once")
}

func TestReadRepair_PrimaryTruncated_FallsBackToSecondary(t *testing.T) {
	h, pool := newRepairHarness(t)
	content := "truncation test payload"
	resp := uploadViaPool(t, h.srv, pool, "f.txt", content)

	refs, _ := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))
	pool.setTruncated(primaryAddr, refs[0].ChunkID)

	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []byte(content), rec.Body.Bytes())

	// Truncation is a size error, not hash mismatch — no repair expected
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, pool.repairCount(), "size error must not trigger repair")
}

func TestReadRepair_Primary500_FallsBack_NoRepair(t *testing.T) {
	h, pool := newRepairHarness(t)
	content := "primary error test"
	resp := uploadViaPool(t, h.srv, pool, "f.txt", content)

	refs, _ := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))
	pool.setErr(primaryAddr, refs[0].ChunkID, fmt.Errorf("simulated 500"))

	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, []byte(content), rec.Body.Bytes())

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, pool.repairCount(), "network error must not trigger repair")
}

func TestReadRepair_BothChunk0Corrupt_Returns502(t *testing.T) {
	h, pool := newRepairHarness(t)
	content := "both replicas dead"
	resp := uploadViaPool(t, h.srv, pool, "f.txt", content)

	refs, _ := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))
	secondaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].SecondaryID))
	pool.setCorrupt(primaryAddr, refs[0].ChunkID, refs[0].Size)
	pool.setCorrupt(secondaryAddr, refs[0].ChunkID, refs[0].Size)

	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	// Error body must contain the error key, not the file's content
	assert.Contains(t, rec.Body.String(), "no valid replica", "error body must name the problem")
	assert.NotContains(t, rec.Body.String(), content, "file content must not be in the response")
}

func TestReadRepair_ZeroByteObject_Downloads(t *testing.T) {
	h, pool := newRepairHarness(t)
	resp := uploadViaPool(t, h.srv, pool, "empty.txt", "")
	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Body.Bytes())
}

func TestReadRepair_MultiChunk_MiddleChunkPrimaryCorrupt(t *testing.T) {
	h, pool := newRepairHarness(t)

	// Build a 3-chunk payload (each chunk exactly 1 MiB so placement is deterministic per chunk_id)
	chunk0 := bytes.Repeat([]byte("A"), chunkSize)
	chunk1 := bytes.Repeat([]byte("B"), chunkSize)
	chunk2 := bytes.Repeat([]byte("C"), chunkSize)
	content := append(append(chunk0, chunk1...), chunk2...)

	resp := uploadViaPool(t, h.srv, pool, "big.bin", string(content))
	require.Equal(t, 3, resp.Chunks)

	refs, _ := h.store.GetChunkRefs(context.Background(), resp.ObjectID)

	// Corrupt only the middle chunk (index 1) on its primary
	midRef := refs[1]
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(midRef.PrimaryID))
	pool.setCorrupt(primaryAddr, midRef.ChunkID, midRef.Size)

	rec := downloadViaPool(h.srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, content, rec.Body.Bytes(), "full multi-chunk download must be byte-identical")

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, pool.repairCount(), "exactly one repair call for the corrupted middle chunk")
	assert.Equal(t, midRef.ChunkID, pool.repairs[0].chunkID)
}

func TestReadRepair_BothLaterChunkCorrupt_ConnectionCut(t *testing.T) {
	h, pool := newRepairHarness(t)

	chunk0 := bytes.Repeat([]byte("A"), chunkSize)
	chunk1 := bytes.Repeat([]byte("B"), chunkSize)
	content := append(chunk0, chunk1...)

	resp := uploadViaPool(t, h.srv, pool, "two.bin", string(content))
	require.Equal(t, 2, resp.Chunks)

	refs, _ := h.store.GetChunkRefs(context.Background(), resp.ObjectID)

	// Corrupt chunk 1 (index 1) on BOTH replicas
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[1].PrimaryID))
	secondaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[1].SecondaryID))
	pool.setCorrupt(primaryAddr, refs[1].ChunkID, refs[1].Size)
	pool.setCorrupt(secondaryAddr, refs[1].ChunkID, refs[1].Size)

	rec := downloadViaPool(h.srv, resp.ObjectID)
	// Headers already sent with 200 — just connection cut, no corrupt bytes appended
	require.Equal(t, http.StatusOK, rec.Code)
	// Only chunk 0 bytes should be in the response body
	assert.Equal(t, chunkSize, rec.Body.Len(), "only chunk 0 bytes sent before connection cut")
	assert.Equal(t, chunk0, rec.Body.Bytes(), "chunk 0 must be intact and correct")
}

// ── metadata chunk locations tests ───────────────────────────────────────────

func TestMetadata_IncludesChunkLocations_3Chunks(t *testing.T) {
	h, pool := newRepairHarness(t)

	chunk0 := bytes.Repeat([]byte("A"), chunkSize)
	chunk1 := bytes.Repeat([]byte("B"), chunkSize)
	chunk2 := bytes.Repeat([]byte("C"), 512)
	content := append(append(chunk0, chunk1...), chunk2...)

	resp := uploadViaPool(t, h.srv, pool, "three.bin", string(content))
	require.Equal(t, 3, resp.Chunks)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID+"/metadata", nil)
	rec := httptest.NewRecorder()
	h.srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var body metadataResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))

	require.Len(t, body.Chunks, 3, "must have 3 chunk entries")
	for i, cl := range body.Chunks {
		assert.Equal(t, i, cl.Index, "chunk index must match")
		assert.Len(t, cl.Locations, 2, "each chunk must have 2 locations")
		assert.NotEqual(t, cl.Locations[0], cl.Locations[1], "primary and secondary must differ")
		assert.NotEmpty(t, cl.ChunkID)
	}
	// Verify offsets are cumulative
	assert.Equal(t, int64(0), body.Chunks[0].Offset)
	assert.Equal(t, int64(chunkSize), body.Chunks[1].Offset)
	assert.Equal(t, int64(2*chunkSize), body.Chunks[2].Offset)
}

func TestMetadata_ZeroByteObject_EmptyChunksArray(t *testing.T) {
	h, pool := newRepairHarness(t)
	resp := uploadViaPool(t, h.srv, pool, "empty.txt", "")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID+"/metadata", nil)
	rec := httptest.NewRecorder()
	h.srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Chunks []chunkLocation `json:"chunks"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.NotNil(t, body.Chunks, "chunks must be [] not null")
	assert.Empty(t, body.Chunks)
}

func TestMetadata_UnknownObject_Returns404(t *testing.T) {
	h, _ := newRepairHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/doesnotexist/metadata", nil)
	rec := httptest.NewRecorder()
	h.srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMetadata_TopLevelFieldsPresent(t *testing.T) {
	h, pool := newRepairHarness(t)
	resp := uploadViaPool(t, h.srv, pool, "check.txt", "field check")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID+"/metadata", nil)
	rec := httptest.NewRecorder()
	h.srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	for _, field := range []string{"object_id", "name", "size_bytes", "status", "created_at", "chunks"} {
		assert.Contains(t, body, field, "field %q must be present", field)
	}
	assert.Equal(t, "check.txt", body["name"])
	assert.Equal(t, "COMMITTED", body["status"])
}
