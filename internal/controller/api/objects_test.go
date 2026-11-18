package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func uploadSimple(t *testing.T, srv *Server, content string) uploadResponse {
	t.Helper()
	body, ct := multipartBody(t, "f.txt", content)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/objects", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, "upload failed: %s", rec.Body.String())
	var resp uploadResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

func doList(t *testing.T, srv *Server) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func doDelete(t *testing.T, srv *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/objects/"+id, nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func doGetMeta(t *testing.T, srv *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+id+"/metadata", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func doPatch(t *testing.T, srv *Server, id, ifMatch, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/objects/"+id+"/metadata",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

// errorBody decodes the JSON error response body from writeError.
func errorBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	return body
}

// ── handleList ────────────────────────────────────────────────────────────────

func TestList_Empty(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doList(t, srv)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	// Body must be a JSON array (possibly null/empty).
	var items []any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&items))
}

func TestList_ReturnsMultipleCommittedObjects(t *testing.T) {
	srv, _ := newTestServer(t)
	uploadSimple(t, srv, "alpha content")
	uploadSimple(t, srv, "beta content")

	rec := doList(t, srv)
	require.Equal(t, http.StatusOK, rec.Code)

	var items []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&items))
	assert.Len(t, items, 2, "must list both committed objects")
}

func TestList_DeletedObjectNotReturned(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "to be deleted")
	uploadSimple(t, srv, "stays committed")

	doDelete(t, srv, resp.ObjectID)

	rec := doList(t, srv)
	require.Equal(t, http.StatusOK, rec.Code)
	var items []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&items))
	assert.Len(t, items, 1, "deleted object must not appear in list")
}

func TestList_ResponseHasRequestID(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doList(t, srv)
	assert.NotEmpty(t, rec.Header().Get("X-Request-ID"))
}

// ── handleGetMetadata ─────────────────────────────────────────────────────────

func TestGetMetadata_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doGetMeta(t, srv, "doesnotexist")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := errorBody(t, rec)
	assert.Equal(t, "not_found", body["code"])
	assert.NotEmpty(t, body["request_id"])
}

func TestGetMetadata_CommittedObject_200(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "metadata payload")

	rec := doGetMeta(t, srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)

	var body metadataResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, resp.ObjectID, body.ID)
	assert.Equal(t, "COMMITTED", string(body.Status))
	assert.NotEmpty(t, rec.Header().Get("ETag"))
}

func TestGetMetadata_ETagMatchesVersion(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "etag check")

	rec := doGetMeta(t, srv, resp.ObjectID)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	version := int64(body["version"].(float64))
	assert.Equal(t, fmt.Sprintf(`"%d"`, version), rec.Header().Get("ETag"))
}

func TestGetMetadata_HasRequestID(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doGetMeta(t, srv, "missing")
	assert.NotEmpty(t, rec.Header().Get("X-Request-ID"))
}

// ── handleDelete ──────────────────────────────────────────────────────────────

func TestDelete_CommittedObject_Returns204(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "delete me")

	rec := doDelete(t, srv, resp.ObjectID)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestDelete_NotFound_Returns404(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doDelete(t, srv, "doesnotexist")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := errorBody(t, rec)
	assert.Equal(t, "not_found", body["code"])
}

func TestDelete_AlreadyDeleted_Returns404(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "delete twice")
	doDelete(t, srv, resp.ObjectID)

	rec := doDelete(t, srv, resp.ObjectID)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDelete_RemovesFromList(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "gone soon")
	uploadSimple(t, srv, "stays")

	doDelete(t, srv, resp.ObjectID)

	rec := doList(t, srv)
	var items []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&items))
	for _, item := range items {
		assert.NotEqual(t, resp.ObjectID, item["id"], "deleted object must not appear in list")
	}
	assert.Len(t, items, 1)
}

func TestDelete_DownloadReturnsForbiddenAfter(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "gone content")
	doDelete(t, srv, resp.ObjectID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/"+resp.ObjectID, nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "deleted object must not be downloadable")
}

func TestDelete_ErrorResponseHasCode(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doDelete(t, srv, "missing")
	body := errorBody(t, rec)
	assert.Equal(t, "not_found", body["code"])
	assert.NotEmpty(t, body["request_id"])
}

// ── handlePatchMetadata ───────────────────────────────────────────────────────

func TestPatchMetadata_AddsKeys(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "patch target")

	rec := doPatch(t, srv, resp.ObjectID, "", `{"env":"prod"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	cm := body["custom_metadata"].(map[string]any)
	assert.Equal(t, "prod", cm["env"])
}

func TestPatchMetadata_VersionConflict_Returns409(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "conflict test")

	// Bump the version once.
	doPatch(t, srv, resp.ObjectID, "", `{"first":"patch"}`)

	// Now try with version 0 — must conflict because version is now 1.
	rec := doPatch(t, srv, resp.ObjectID, `"0"`, `{"stale":"attempt"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
	body := errorBody(t, rec)
	assert.Equal(t, "conflict", body["code"])
}

func TestPatchMetadata_NotFound_Returns404(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doPatch(t, srv, "doesnotexist", "", `{"k":"v"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPatchMetadata_InvalidJSON_Returns400(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "bad json target")
	rec := doPatch(t, srv, resp.ObjectID, "", `not json`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := errorBody(t, rec)
	assert.Equal(t, "bad_request", body["code"])
}

func TestPatchMetadata_InvalidIfMatch_Returns400(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "bad if-match")
	rec := doPatch(t, srv, resp.ObjectID, "not-a-number", `{"k":"v"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPatchMetadata_ETagUpdated(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := uploadSimple(t, srv, "etag evolves")

	rec1 := doGetMeta(t, srv, resp.ObjectID)
	etag1 := rec1.Header().Get("ETag")

	doPatch(t, srv, resp.ObjectID, "", `{"k":"v"}`)

	rec2 := doGetMeta(t, srv, resp.ObjectID)
	etag2 := rec2.Header().Get("ETag")

	assert.NotEqual(t, etag1, etag2, "ETag must change after a successful patch")
}

// ── error response format ─────────────────────────────────────────────────────

func TestErrorResponse_HasCodeAndRequestID(t *testing.T) {
	srv, _ := newTestServer(t)
	// Trigger a known 404.
	rec := doGetMeta(t, srv, "missing-id")
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.NotEmpty(t, body["error"])
	assert.Equal(t, "not_found", body["code"])
	assert.NotEmpty(t, body["request_id"])
}

func TestErrorResponse_DifferentRequestIDEachRequest(t *testing.T) {
	srv, _ := newTestServer(t)
	rec1 := doGetMeta(t, srv, "missing")
	rec2 := doGetMeta(t, srv, "missing")
	assert.NotEqual(t,
		rec1.Header().Get("X-Request-ID"),
		rec2.Header().Get("X-Request-ID"),
		"each request must get a unique request ID")
}

// ── handleList error response ─────────────────────────────────────────────────

func TestList_ObjectFields(t *testing.T) {
	srv, _ := newTestServer(t)
	uploadSimple(t, srv, "field check content")

	rec := doList(t, srv)
	require.Equal(t, http.StatusOK, rec.Code)

	var items []map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&items))
	require.Len(t, items, 1)

	item := items[0]
	for _, field := range []string{"object_id", "name", "size_bytes", "status"} {
		assert.Contains(t, item, field, "list item must contain field %q", field)
	}
	assert.Equal(t, "COMMITTED", item["status"])
}

func TestList_StatusCodeAndContentType(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doList(t, srv)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
}

// ── handleDelete with non-committed states ────────────────────────────────────

func TestDelete_UnknownID_Returns404WithCode(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doDelete(t, srv, "completely-unknown-id")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := errorBody(t, rec)
	assert.Equal(t, "not_found", body["code"])
	_ = models.StatusStaged // ensure models import stays used
}
