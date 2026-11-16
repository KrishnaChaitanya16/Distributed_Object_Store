package api

// Tests covering the 5 spec gaps fixed in fix/controller-spec-gaps.

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// uploadWithMeta posts a multipart form with an optional JSON metadata field.
func uploadWithMeta(t *testing.T, h *testHarness, filename, content, metadata string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	require.NoError(t, err)
	io.WriteString(fw, content)
	if metadata != "" {
		require.NoError(t, mw.WriteField("metadata", metadata))
	}
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return h.do(req)
}

// getMetadata returns the parsed metadata response for objectID.
func getMetadata(h *testHarness, objectID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+objectID+"/metadata", nil)
	return h.do(req)
}

// patchMeta sends a PATCH with the given patch body and optional If-Match header.
func patchMeta(h *testHarness, objectID string, patch map[string]string, ifMatch string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(patch)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+objectID+"/metadata",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	return h.do(req)
}

// listObjects returns all committed objects from the server.
func listObjects(h *testHarness) []*models.ObjectManifest {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	rec := h.do(req)
	var out []*models.ObjectManifest
	json.NewDecoder(rec.Body).Decode(&out) //nolint:errcheck
	return out
}

// ── Fix 1: custom metadata at upload ─────────────────────────────────────────

func TestUploadMetadata_StoredAndReturned(t *testing.T) {
	h := newHarness(t)
	rec := uploadWithMeta(t, h, "f.txt", "hello", `{"owner":"alice","env":"prod"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	getRec := getMetadata(h, resp.ObjectID)
	require.Equal(t, http.StatusOK, getRec.Code)

	var meta map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&meta))
	cm, ok := meta["custom_metadata"].(map[string]any)
	require.True(t, ok, "custom_metadata key must be present")
	assert.Equal(t, "alice", cm["owner"])
	assert.Equal(t, "prod", cm["env"])
}

func TestUploadMetadata_NoMetadataField_Succeeds(t *testing.T) {
	h := newHarness(t)
	rec := uploadWithMeta(t, h, "f.txt", "hello", "")
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestUploadMetadata_InvalidJSON_Returns400(t *testing.T) {
	h := newHarness(t)
	for _, bad := range []string{`{bad`, `[1,2]`, `{"a":5}`} {
		rec := uploadWithMeta(t, h, "f.txt", "hello", bad)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 for metadata=%q", bad)
	}
}

func TestUploadMetadata_InvalidJSON_NoObjectStored(t *testing.T) {
	h := newHarness(t)
	before := listObjects(h)
	uploadWithMeta(t, h, "f.txt", "hello", `{bad`)
	after := listObjects(h)
	assert.Len(t, after, len(before), "no object should be stored after a 400")
	// No chunks pushed either
	assert.Empty(t, h.workers.callsFor("PUT", ""), "no worker PUTs should occur on bad metadata")
}

func TestUploadMetadata_SurvivesPatch(t *testing.T) {
	h := newHarness(t)
	rec := uploadWithMeta(t, h, "f.txt", "hello", `{"owner":"alice"}`)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	// PATCH adds a new key
	pRec := patchMeta(h, resp.ObjectID, map[string]string{"env": "prod"}, "")
	require.Equal(t, http.StatusOK, pRec.Code)

	getRec := getMetadata(h, resp.ObjectID)
	var meta map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&meta))
	cm := meta["custom_metadata"].(map[string]any)
	assert.Equal(t, "alice", cm["owner"], "owner must survive PATCH")
	assert.Equal(t, "prod", cm["env"], "env must be added by PATCH")
}

// ── Fix 2: atomic PATCH / concurrent version safety ───────────────────────────

func TestAtomicPatch_ConcurrentSameIfMatch_ExactlyOneWins(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	// Get current ETag (version 0)
	getRec := getMetadata(h, resp.ObjectID)
	etag := getRec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	const n = 10
	codes := make([]int, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		wg.Done() // pre-decrement; we'll re-add after gate opens
		_ = i
	}
	// reset
	var mu sync.Mutex
	for i := range codes {
		codes[i] = 0
	}

	// Use a gate so all goroutines fire simultaneously
	gate := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-gate
			rec := patchMeta(h, resp.ObjectID,
				map[string]string{fmt.Sprintf("key%d", i): "val"},
				etag)
			mu.Lock()
			codes[i] = rec.Code
			mu.Unlock()
		}()
	}
	close(gate)
	wg.Wait()

	successes, conflicts := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			successes++
		case http.StatusConflict:
			conflicts++
		}
	}
	assert.Equal(t, 1, successes, "exactly one PATCH must succeed")
	assert.Equal(t, n-1, conflicts, "all others must get 409")

	// Version must have advanced by exactly 1
	getRec2 := getMetadata(h, resp.ObjectID)
	var meta map[string]any
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&meta))
	assert.Equal(t, float64(1), meta["version"])
}

func TestAtomicPatch_Sequential_VersionBumps(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	for i := int64(0); i < 3; i++ {
		rec := patchMeta(h, resp.ObjectID, map[string]string{"i": fmt.Sprintf("%d", i)}, "")
		require.Equal(t, http.StatusOK, rec.Code)
		var m map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&m))
		assert.Equal(t, float64(i+1), m["version"])
	}
}

func TestAtomicPatch_UnknownID_Returns404(t *testing.T) {
	h := newHarness(t)
	rec := patchMeta(h, "does-not-exist", map[string]string{"k": "v"}, "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAtomicPatch_StagedObject_Returns404(t *testing.T) {
	h := newHarness(t)
	// Save a STAGED manifest directly
	ctx := context.Background()
	err := h.store.SaveObject(ctx, &models.ObjectManifest{
		ID: "staged-id", Status: models.StatusStaged,
	})
	require.NoError(t, err)
	rec := patchMeta(h, "staged-id", map[string]string{"k": "v"}, "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAtomicPatch_NoIfMatch_Succeeds(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")
	rec := patchMeta(h, resp.ObjectID, map[string]string{"k": "v"}, "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ── Fix 3: DELETE already-deleted → 404 ──────────────────────────────────────

func TestDelete_Twice_SecondReturns404(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "bye")
	assert.Equal(t, http.StatusNoContent, h.deleteObject(t, resp.ObjectID))
	assert.Equal(t, http.StatusNotFound, h.deleteObject(t, resp.ObjectID))
}

func TestDelete_UnknownID_Returns404(t *testing.T) {
	h := newHarness(t)
	assert.Equal(t, http.StatusNotFound, h.deleteObject(t, "ghost"))
}

func TestDelete_AfterDelete_GetReturns404(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "data")
	h.deleteObject(t, resp.ObjectID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID, nil)
	assert.Equal(t, http.StatusNotFound, h.do(req).Code)

	pRec := patchMeta(h, resp.ObjectID, map[string]string{"k": "v"}, "")
	assert.Equal(t, http.StatusNotFound, pRec.Code)
}

func TestDelete_SharedChunk_OtherObjectStillDownloadable(t *testing.T) {
	// Two objects with identical content share the same chunk (CAS dedup).
	// Deleting one must not remove the chunk from workers; the other must remain downloadable.
	h := newHarness(t)
	content := strings.Repeat("Z", 1024)
	resp1 := h.upload(t, "a.txt", content)
	resp2 := h.upload(t, "b.txt", content)

	// Delete first object
	require.Equal(t, http.StatusNoContent, h.deleteObject(t, resp1.ObjectID))

	// Second object must still be downloadable
	code, body := h.download(t, resp2.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
}

// ── Fix 4: HEDGE_THRESHOLD_MS=0 must NOT hedge ───────────────────────────────

// delayPool injects a per-addr delay before returning the chunk.
type delayPool struct {
	*chunkStore
	mu         sync.Mutex
	addrDelays map[string]time.Duration
	getCalls   map[string]int32
}

func newDelayPool(cs *chunkStore) *delayPool {
	return &delayPool{
		chunkStore: cs,
		addrDelays: make(map[string]time.Duration),
		getCalls:   make(map[string]int32),
	}
}

func (p *delayPool) setDelay(addrPrefix string, d time.Duration) {
	p.mu.Lock()
	p.addrDelays[addrPrefix] = d
	p.mu.Unlock()
}

func (p *delayPool) getCallCount(addrPrefix string) int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.getCalls[addrPrefix]
}

func (p *delayPool) GetChunk(ctx context.Context, addr, id string) (io.ReadCloser, error) {
	p.mu.Lock()
	var delay time.Duration
	for prefix, d := range p.addrDelays {
		if strings.HasPrefix(addr, prefix) {
			delay = d
			break
		}
	}
	p.getCalls[addr]++
	p.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.chunkStore.GetChunk(ctx, addr, id)
}

func TestHedge_Zero_SlowPrimary_SecondaryNotHedged(t *testing.T) {
	h := newHarness(t)
	h.srv.hedgeMs = 0

	content := "hedge disabled test"
	resp := h.upload(t, "noh.txt", content)

	refs, err := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	require.NoError(t, err)
	require.Len(t, refs, 1)

	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))

	dp := newDelayPool(h.workers)
	dp.setDelay(primaryAddr, 80*time.Millisecond)
	h.srv.workers = dp

	start := time.Now()
	code, body := h.download(t, resp.ObjectID)
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))

	// Must have waited for the full primary delay (no hedge)
	assert.GreaterOrEqual(t, elapsed, 70*time.Millisecond,
		"hedgeMs=0 must wait for primary; got %v", elapsed)

	// Secondary must have received exactly 0 GetChunk calls during primary wait.
	// (may receive 1 if primary then fails, but it succeeded here so 0)
	secAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].SecondaryID))
	assert.Zero(t, dp.getCallCount(secAddr),
		"secondary must not be called when hedgeMs=0 and primary succeeds")
}

func TestHedge_NonZero_SlowPrimary_SecondaryHedged(t *testing.T) {
	h := newHarness(t)
	h.srv.hedgeMs = 10

	content := "hedge enabled test"
	resp := h.upload(t, "yesh.txt", content)

	refs, err := h.store.GetChunkRefs(context.Background(), resp.ObjectID)
	require.NoError(t, err)
	require.Len(t, refs, 1)

	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))

	dp := newDelayPool(h.workers)
	dp.setDelay(primaryAddr, 150*time.Millisecond)
	h.srv.workers = dp

	start := time.Now()
	code, body := h.download(t, resp.ObjectID)
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
	assert.Less(t, elapsed, 100*time.Millisecond,
		"hedged download must finish before primary delay; got %v", elapsed)

	secAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].SecondaryID))
	assert.Equal(t, int32(1), dp.getCallCount(secAddr),
		"secondary must be called when hedge fires")
}

// ── Fix 5: If-Match: * and W/"n" support ──────────────────────────────────────

func TestIfMatch_Star_Succeeds(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")
	rec := patchMeta(h, resp.ObjectID, map[string]string{"k": "v"}, "*")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestIfMatch_QuotedVersion_Succeeds(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")
	getRec := getMetadata(h, resp.ObjectID)
	etag := getRec.Header().Get("ETag") // e.g. "0"
	rec := patchMeta(h, resp.ObjectID, map[string]string{"k": "v"}, etag)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestIfMatch_WeakTag_Succeeds(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")
	getRec := getMetadata(h, resp.ObjectID)
	etag := getRec.Header().Get("ETag") // "0"
	weak := "W/" + etag                 // W/"0"
	rec := patchMeta(h, resp.ObjectID, map[string]string{"k": "v"}, weak)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestIfMatch_GarbageValue_Returns400(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")
	rec := patchMeta(h, resp.ObjectID, map[string]string{"k": "v"}, `"not-a-number"`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestIfMatch_StaleValue_Returns409(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")
	// PATCH once to advance version to 1
	patchMeta(h, resp.ObjectID, map[string]string{"x": "y"}, "")
	// Now try with the original version 0
	rec := patchMeta(h, resp.ObjectID, map[string]string{"k": "v"}, `"0"`)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// ── Fix 2 extra: only winner's key present ────────────────────────────────────

func TestAtomicPatch_OnlyWinnerKeyPresent(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	getRec := getMetadata(h, resp.ObjectID)
	etag := getRec.Header().Get("ETag")

	const n = 10
	var mu sync.Mutex
	var winnerKey string
	gate := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-gate
			k := fmt.Sprintf("winner%d", i)
			rec := patchMeta(h, resp.ObjectID, map[string]string{k: "1"}, etag)
			if rec.Code == http.StatusOK {
				mu.Lock()
				winnerKey = k
				mu.Unlock()
			}
		}()
	}
	close(gate)
	wg.Wait()

	getRec2 := getMetadata(h, resp.ObjectID)
	var meta map[string]any
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&meta))
	cm, ok := meta["custom_metadata"].(map[string]any)
	require.True(t, ok)

	mu.Lock()
	wk := winnerKey
	mu.Unlock()

	require.NotEmpty(t, wk, "one goroutine must have won")
	assert.Equal(t, "1", cm[wk], "winner's key must be in metadata")
	assert.Len(t, cm, 1, "only the winner's key must be present")

	// Version advanced by exactly 1
	assert.Equal(t, float64(1), meta["version"])
}

// ── Fix 1 extra: bad metadata returns non-zero body hint ─────────────────────

func TestUploadMetadata_BadJSON_ResponseBodyContainsHint(t *testing.T) {
	h := newHarness(t)
	rec := uploadWithMeta(t, h, "f.txt", "hello", `[1,2]`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "metadata")
}

// reuse atomic.Int32 to avoid import lint
var _ = atomic.Int32{}
