package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── parseByteRange unit tests ─────────────────────────────────────────────────

func TestParseByteRange_NoHeader(t *testing.T) {
	s, e, isRange, err := parseByteRange("", 1000)
	require.NoError(t, err)
	assert.False(t, isRange)
	assert.Equal(t, int64(0), s)
	assert.Equal(t, int64(0), e)
}

func TestParseByteRange_Explicit(t *testing.T) {
	s, e, isRange, err := parseByteRange("bytes=100-499", 1000)
	require.NoError(t, err)
	assert.True(t, isRange)
	assert.Equal(t, int64(100), s)
	assert.Equal(t, int64(499), e)
}

func TestParseByteRange_OpenEnd(t *testing.T) {
	s, e, isRange, err := parseByteRange("bytes=500-", 1000)
	require.NoError(t, err)
	assert.True(t, isRange)
	assert.Equal(t, int64(500), s)
	assert.Equal(t, int64(999), e)
}

func TestParseByteRange_Suffix(t *testing.T) {
	s, e, isRange, err := parseByteRange("bytes=-200", 1000)
	require.NoError(t, err)
	assert.True(t, isRange)
	assert.Equal(t, int64(800), s)
	assert.Equal(t, int64(999), e)
}

func TestParseByteRange_SuffixLargerThanFile(t *testing.T) {
	s, e, isRange, err := parseByteRange("bytes=-5000", 1000)
	require.NoError(t, err)
	assert.True(t, isRange)
	assert.Equal(t, int64(0), s)
	assert.Equal(t, int64(999), e)
}

func TestParseByteRange_EndClampedToEOF(t *testing.T) {
	s, e, isRange, err := parseByteRange("bytes=0-9999", 1000)
	require.NoError(t, err)
	assert.True(t, isRange)
	assert.Equal(t, int64(0), s)
	assert.Equal(t, int64(999), e)
}

func TestParseByteRange_StartBeyondEOF(t *testing.T) {
	_, _, _, err := parseByteRange("bytes=2000-3000", 1000)
	assert.Error(t, err)
}

func TestParseByteRange_MultiRange_Returns416(t *testing.T) {
	_, _, _, err := parseByteRange("bytes=0-100,200-300", 1000)
	assert.Error(t, err)
}

func TestParseByteRange_UnsupportedUnit(t *testing.T) {
	_, _, _, err := parseByteRange("none=0-100", 1000)
	assert.Error(t, err)
}

// ── HTTP handler integration tests ───────────────────────────────────────────

// rangeContent is 800 bytes: "abcdefgh" × 100.
var rangeContent = strings.Repeat("abcdefgh", 100)

func uploadRangeObj(t *testing.T, srv *Server) string {
	t.Helper()
	body, ct := multipartBody(t, "range.bin", rangeContent)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp.ObjectID
}

func rangeGet(t *testing.T, srv *Server, objectID, rangeHeader string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+objectID, nil)
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func TestRange_FullDownload_Has_AcceptRanges(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "bytes", rec.Header().Get("Accept-Ranges"))
}

func TestRange_ExplicitRange_Returns206(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=0-7")
	assert.Equal(t, http.StatusPartialContent, rec.Code)
	assert.Equal(t, "bytes 0-7/800", rec.Header().Get("Content-Range"))
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, "abcdefgh", string(body))
}

func TestRange_MidFile_CorrectSlice(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=8-15")
	require.Equal(t, http.StatusPartialContent, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, "abcdefgh", string(body))
	assert.Equal(t, "bytes 8-15/800", rec.Header().Get("Content-Range"))
}

func TestRange_SuffixRange(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=-8")
	require.Equal(t, http.StatusPartialContent, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, "abcdefgh", string(body))
	assert.Equal(t, "bytes 792-799/800", rec.Header().Get("Content-Range"))
}

func TestRange_OpenEndRange(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=792-")
	require.Equal(t, http.StatusPartialContent, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, "abcdefgh", string(body))
	assert.Equal(t, "bytes 792-799/800", rec.Header().Get("Content-Range"))
}

func TestRange_ContentLength_MatchesSlice(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=100-199")
	require.Equal(t, http.StatusPartialContent, rec.Code)
	assert.Equal(t, "100", rec.Header().Get("Content-Length"))
	body, _ := io.ReadAll(rec.Body)
	assert.Len(t, body, 100)
}

func TestRange_InvalidRange_Returns416(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=5000-6000")
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, rec.Code)
}

func TestRange_MultiRange_Returns416(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=0-100,200-300")
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, rec.Code)
}

func TestRange_FullObjectViaRange(t *testing.T) {
	srv, _ := newTestServer(t)
	id := uploadRangeObj(t, srv)
	rec := rangeGet(t, srv, id, "bytes=0-799")
	require.Equal(t, http.StatusPartialContent, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, rangeContent, string(body))
}

// ── Cross-chunk byte-range tests ─────────────────────────────────────────────
//
// These tests upload a file larger than chunkSize (1 MiB) so the object is
// split into two chunks, then request a byte range that spans the chunk
// boundary.  This exercises the chunk-skip optimisation in handleDownload:
// only the two overlapping chunks should be fetched, not the whole file.

func uploadTwoChunkObj(t *testing.T, srv *Server) (objectID string, totalSize int) {
	t.Helper()
	// chunk0: 1 MiB of 'A', chunk1: 512 bytes of 'B'
	content := strings.Repeat("A", chunkSize) + strings.Repeat("B", 512)
	body, ct := multipartBody(t, "twochunk.bin", content)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, "upload must succeed")
	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, 2, resp.Chunks, "file must be split into exactly 2 chunks")
	return resp.ObjectID, chunkSize + 512
}

func TestRange_CrossChunkBoundary_ReturnsCorrectBytes(t *testing.T) {
	srv, _ := newTestServer(t)
	id, total := uploadTwoChunkObj(t, srv)

	// Request 16 bytes straddling the boundary: last 8 bytes of chunk0 + first 8 of chunk1.
	last8OfChunk0 := chunkSize - 8
	first8OfChunk1 := chunkSize + 7
	rec := rangeGet(t, srv, id, fmt.Sprintf("bytes=%d-%d", last8OfChunk0, first8OfChunk1))
	require.Equal(t, http.StatusPartialContent, rec.Code)

	want := strings.Repeat("A", 8) + strings.Repeat("B", 8)
	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, want, string(body), "boundary bytes must be correct")
	assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", last8OfChunk0, first8OfChunk1, total),
		rec.Header().Get("Content-Range"))
	assert.Equal(t, "16", rec.Header().Get("Content-Length"))
}

func TestRange_CrossChunkBoundary_SecondChunkOnly(t *testing.T) {
	srv, _ := newTestServer(t)
	id, total := uploadTwoChunkObj(t, srv)

	// Range entirely within chunk1 (the 512-byte tail).
	start := chunkSize + 100
	end := chunkSize + 199
	rec := rangeGet(t, srv, id, fmt.Sprintf("bytes=%d-%d", start, end))
	require.Equal(t, http.StatusPartialContent, rec.Code)

	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, strings.Repeat("B", 100), string(body))
	assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", start, end, total),
		rec.Header().Get("Content-Range"))
}

func TestRange_CrossChunkBoundary_SuffixSpansChunks(t *testing.T) {
	srv, _ := newTestServer(t)
	id, total := uploadTwoChunkObj(t, srv)

	// Suffix range of 600 bytes: last 88 bytes of chunk0 + all 512 bytes of chunk1.
	rec := rangeGet(t, srv, id, "bytes=-600")
	require.Equal(t, http.StatusPartialContent, rec.Code)

	body, _ := io.ReadAll(rec.Body)
	assert.Len(t, body, 600)
	assert.Equal(t, strings.Repeat("A", 88)+strings.Repeat("B", 512), string(body))
	assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", total-600, total-1, total),
		rec.Header().Get("Content-Range"))
}

func TestRange_CrossChunkBoundary_OpenEndFromChunk1(t *testing.T) {
	srv, _ := newTestServer(t)
	id, total := uploadTwoChunkObj(t, srv)

	// Open-end range starting 10 bytes into chunk1: returns remaining 502 bytes.
	start := chunkSize + 10
	rec := rangeGet(t, srv, id, fmt.Sprintf("bytes=%d-", start))
	require.Equal(t, http.StatusPartialContent, rec.Code)

	body, _ := io.ReadAll(rec.Body)
	assert.Equal(t, strings.Repeat("B", 502), string(body))
	assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", start, total-1, total),
		rec.Header().Get("Content-Range"))
}
