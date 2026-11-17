package api

import (
	"encoding/json"
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
