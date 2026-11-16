package api

// Tests for ETag / If-Match on PATCH (§2.17) and hedged reads (§2.8).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── ETag / If-Match tests ─────────────────────────────────────────────────────

func TestETag_GetMetadata_ReturnsETagHeader(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID+"/metadata", nil)
	rec := h.do(req)
	require.Equal(t, http.StatusOK, rec.Code)

	etag := rec.Header().Get("ETag")
	require.NotEmpty(t, etag, "ETag header must be present on GET metadata")
	assert.True(t, strings.HasPrefix(etag, `"`), "ETag must be a quoted string")
}

func TestETag_Patch_NoIfMatch_Succeeds(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	body, _ := json.Marshal(map[string]string{"env": "prod"})
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+resp.ObjectID+"/metadata",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := h.do(req)

	require.Equal(t, http.StatusOK, rec.Code)
	newEtag := rec.Header().Get("ETag")
	require.NotEmpty(t, newEtag, "ETag must be returned after PATCH")
}

func TestETag_Patch_CorrectIfMatch_Succeeds(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	// Get current ETag
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID+"/metadata", nil)
	getRec := h.do(getReq)
	currentEtag := getRec.Header().Get("ETag")
	require.NotEmpty(t, currentEtag)

	// PATCH with correct ETag
	body, _ := json.Marshal(map[string]string{"env": "prod"})
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+resp.ObjectID+"/metadata",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", currentEtag)
	rec := h.do(req)

	require.Equal(t, http.StatusOK, rec.Code)

	// ETag must have changed (version incremented)
	newEtag := rec.Header().Get("ETag")
	assert.NotEqual(t, currentEtag, newEtag, "ETag must change after a successful PATCH")

	// Returned manifest must include the new custom metadata
	var manifest map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&manifest))
	cm, ok := manifest["custom_metadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", cm["env"])
}

func TestETag_Patch_StaleIfMatch_Returns409(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	// First PATCH — succeeds, bumps version
	body, _ := json.Marshal(map[string]string{"k": "v1"})
	req1 := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+resp.ObjectID+"/metadata",
		bytes.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := h.do(req1)
	require.Equal(t, http.StatusOK, rec1.Code)
	firstEtag := rec1.Header().Get("ETag")

	// Second PATCH — fails because stale ETag (still at version 0)
	staleEtag := `"0"`
	body2, _ := json.Marshal(map[string]string{"k": "v2"})
	req2 := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+resp.ObjectID+"/metadata",
		bytes.NewReader(body2))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("If-Match", staleEtag)
	rec2 := h.do(req2)

	require.Equal(t, http.StatusConflict, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "version conflict")

	_ = firstEtag
}

func TestETag_Patch_Sequential_ETagsMonotonicallyIncrease(t *testing.T) {
	h := newHarness(t)
	resp := h.upload(t, "f.txt", "hello")

	var lastEtag string
	for i := 0; i < 3; i++ {
		getReq := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID+"/metadata", nil)
		currentEtag := h.do(getReq).Header().Get("ETag")

		body, _ := json.Marshal(map[string]string{"step": fmt.Sprintf("%d", i)})
		req := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+resp.ObjectID+"/metadata",
			bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("If-Match", currentEtag)
		rec := h.do(req)
		require.Equal(t, http.StatusOK, rec.Code, "PATCH step %d failed", i)

		newEtag := rec.Header().Get("ETag")
		assert.NotEqual(t, currentEtag, newEtag, "ETag must change on step %d", i)
		if lastEtag != "" {
			assert.NotEqual(t, lastEtag, newEtag, "ETag must be unique across steps")
		}
		lastEtag = newEtag
	}
}

// ── hedged reads tests ────────────────────────────────────────────────────────

// slowPool wraps a chunkStore and injects a delay on GetChunk for specific workers.
type slowPool struct {
	*chunkStore
	slowWorker string
	delay      time.Duration
	hedgeSeen  atomic.Bool
}

func (p *slowPool) GetChunk(ctx context.Context, addr, id string) (io.ReadCloser, error) {
	if strings.HasPrefix(addr, p.slowWorker) {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.chunkStore.GetChunk(ctx, addr, id)
}

func TestHedgedReads_SlowPrimary_SecondaryWins(t *testing.T) {
	// Set hedgeMs very low (1 ms) so the hedge fires before the primary responds.
	h := newHarness(t)
	h.srv.hedgeMs = 1

	content := "hedged read test content"
	resp := h.upload(t, "hedge.txt", content)

	// Make the primary worker artificially slow
	refs, _ := h.store.GetChunkRefs(nil, resp.ObjectID) //nolint:staticcheck
	primaryWorkerAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))

	slow := &slowPool{
		chunkStore: h.workers,
		slowWorker: primaryWorkerAddr,
		delay:      50 * time.Millisecond,
	}
	h.srv.workers = slow

	start := time.Now()
	code, body := h.download(t, resp.ObjectID)
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body), "downloaded bytes must be identical")
	assert.Less(t, elapsed, 40*time.Millisecond,
		"hedged download must complete faster than primary delay (%v)", elapsed)
}

func TestHedgedReads_BothWorkersFast_PrimaryUsed(t *testing.T) {
	h := newHarness(t)
	h.srv.hedgeMs = 100 // threshold higher than test latency → primary always wins

	content := "fast primary content"
	resp := h.upload(t, "fast.txt", content)
	code, body := h.download(t, resp.ObjectID)

	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
	assert.True(t, h.logs.hasMessage("chunk fetched from primary"), "primary should be used when fast")
}

func TestHedgedReads_PrimaryDown_SecondaryServes(t *testing.T) {
	h := newHarness(t)
	h.srv.hedgeMs = 25

	content := "failover content"
	resp := h.upload(t, "fail.txt", content)

	refs, _ := h.store.GetChunkRefs(nil, resp.ObjectID) //nolint:staticcheck
	primaryAddr := fmt.Sprintf("localhost:%d", 9000+workerNum(refs[0].PrimaryID))

	// Delete from primary to simulate it being down
	h.workers.mu.Lock()
	delete(h.workers.chunks, h.workers.key(primaryAddr, refs[0].ChunkID))
	h.workers.mu.Unlock()

	code, body := h.download(t, resp.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, string(body))
}

func TestDownload_ContentLengthHeader_Present(t *testing.T) {
	h := newHarness(t)
	content := strings.Repeat("X", 1234)
	resp := h.upload(t, "size.txt", content)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID, nil)
	rec := h.do(req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, fmt.Sprintf("%d", len(content)), rec.Header().Get("Content-Length"))
}
