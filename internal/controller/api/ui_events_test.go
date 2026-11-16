package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func tracedRequest(t *testing.T, srv *Server, method, path, trace string, body io.Reader, ctype string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if trace != "" {
		req.Header.Set("X-Trace-Id", trace)
	}
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	return rec
}

func tracedUpload(t *testing.T, srv *Server, trace, name, content string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", name)
	require.NoError(t, err)
	io.WriteString(fw, content)
	mw.Close()
	return tracedRequest(t, srv, http.MethodPost, "/api/v1/objects", trace, &buf, mw.FormDataContentType())
}

func eventsFor(trace string) []events.Event {
	var out []events.Event
	for _, ev := range events.Default.Since(0, 0) {
		if ev.Fields["trace"] == trace {
			out = append(out, ev)
		}
	}
	return out
}

func kinds(evs []events.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}

func ofKind(evs []events.Event, kind string) []events.Event {
	var out []events.Event
	for _, e := range evs {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func TestUploadEmitsTheWholePipelineInOrder(t *testing.T) {
	h := newHarness(t)
	trace := "t-pipeline-" + fmt.Sprint(time.Now().UnixNano())
	rec := tracedUpload(t, h.srv, trace, "hello.txt", "hello pipeline")
	require.Equal(t, http.StatusCreated, rec.Code)

	evs := eventsFor(trace)
	assert.Equal(t, []string{"upload.start", "upload.chunk", "upload.place", "upload.bloom", "upload.push", "upload.push", "upload.durable", "upload.commit"}, kinds(evs))

	start := evs[0].Fields
	assert.Equal(t, "hello.txt", start["name"])
	assert.EqualValues(t, len("hello pipeline"), start["size"])
	oid := start["object_id"]
	require.NotEmpty(t, oid)
	for _, e := range evs {
		assert.Equal(t, oid, e.Fields["object_id"], e.Kind)
	}

	cid := chunkIDOf("hello pipeline")
	chunk := evs[1].Fields
	assert.Equal(t, cid, chunk["chunk_id"], "the event carries the SHA-256 chunk id")
	assert.EqualValues(t, 0, chunk["index"])
	assert.EqualValues(t, len("hello pipeline"), chunk["size"])

	place := evs[2].Fields
	assert.EqualValues(t, placement.KeyHash(cid), place["ring_pos"], "the ring position is the real key hash")
	assert.NotEmpty(t, place["primary"])
	assert.NotEqual(t, place["primary"], place["secondary"])

	bloom := evs[3].Fields
	assert.Equal(t, false, bloom["maybe"])
	assert.Equal(t, "definitely_new", bloom["verdict"])

	pushes := ofKind(evs, "upload.push")
	assert.Equal(t, "primary", pushes[0].Fields["role"])
	assert.Equal(t, place["primary"], pushes[0].Fields["worker"])
	assert.Equal(t, "secondary", pushes[1].Fields["role"])
	assert.Equal(t, place["secondary"], pushes[1].Fields["worker"])
	for _, p := range pushes {
		assert.Equal(t, true, p.Fields["ok"])
	}
	assert.EqualValues(t, 1, evs[len(evs)-1].Fields["chunks"])
}

func TestUploadEventsHaveNoTraceWhenTheClientSendsNone(t *testing.T) {
	h := newHarness(t)
	before := events.Default.LastSeq()
	require.Equal(t, http.StatusCreated, tracedUpload(t, h.srv, "", "a", "no trace here").Code)
	got := events.Default.Since(before, 0)
	require.NotEmpty(t, got)
	for _, e := range got {
		assert.NotContains(t, e.Fields, "trace", e.Kind)
	}
}

func TestOverlongTraceIdIsTruncated(t *testing.T) {
	assert.Equal(t, strings.Repeat("x", 64), traceOf(withTrace(context.Background(), strings.Repeat("x", 500))))
	assert.Equal(t, "", traceOf(withTrace(context.Background(), "")))
}

func TestMultiChunkUploadEmitsPerChunkEvents(t *testing.T) {
	h := newHarness(t)
	trace := "t-multi-" + fmt.Sprint(time.Now().UnixNano())
	content := strings.Repeat("A", chunkSize) + strings.Repeat("B", chunkSize) + "tail"
	require.Equal(t, http.StatusCreated, tracedUpload(t, h.srv, trace, "big", content).Code)
	evs := eventsFor(trace)
	assert.Len(t, ofKind(evs, "upload.chunk"), 3)
	assert.Len(t, ofKind(evs, "upload.push"), 6)
	assert.Len(t, ofKind(evs, "upload.durable"), 3)
	for i, e := range ofKind(evs, "upload.chunk") {
		assert.EqualValues(t, i, e.Fields["index"])
		assert.EqualValues(t, i*chunkSize, e.Fields["offset"])
	}
}

func TestDuplicateUploadEmitsBloomHitAndDedupWithoutPushes(t *testing.T) {
	h := newHarness(t)
	tracedUpload(t, h.srv, "", "first", "same bytes twice")
	trace := "t-dup-" + fmt.Sprint(time.Now().UnixNano())
	require.Equal(t, http.StatusCreated, tracedUpload(t, h.srv, trace, "second", "same bytes twice").Code)
	evs := eventsFor(trace)
	bloom := ofKind(evs, "upload.bloom")
	require.Len(t, bloom, 1)
	assert.Equal(t, true, bloom[0].Fields["maybe"])
	assert.Equal(t, "true_positive", bloom[0].Fields["verdict"])
	assert.Len(t, ofKind(evs, "upload.dedup"), 1)
	assert.Empty(t, ofKind(evs, "upload.push"), "an identical chunk is never sent to workers again")
	assert.Equal(t, models.ChunkDurable, func() models.ChunkState {
		c, _ := h.store.GetChunk(context.Background(), chunkIDOf("same bytes twice"))
		return c.State
	}())
}

func TestBloomFalsePositiveIsClassifiedAsSuch(t *testing.T) {
	h := newHarness(t)
	content := "bloom says maybe but the store has never seen this"
	h.srv.bloom.Add(chunkIDOf(content))
	before := h.srv.bloom.Counters()
	trace := "t-fp-" + fmt.Sprint(time.Now().UnixNano())
	require.Equal(t, http.StatusCreated, tracedUpload(t, h.srv, trace, "x", content).Code)
	bloom := ofKind(eventsFor(trace), "upload.bloom")
	require.Len(t, bloom, 1)
	assert.Equal(t, "false_positive", bloom[0].Fields["verdict"])
	assert.Len(t, ofKind(eventsFor(trace), "upload.push"), 2, "the authoritative store decides, so the chunk is still stored")
	after := h.srv.bloom.Counters()
	assert.Equal(t, before.FalsePositive+1, after.FalsePositive)
	assert.Equal(t, before.Queries+1, after.Queries)
}

func TestFailedUploadEmitsErrorAndFail(t *testing.T) {
	h := newHarness(t)
	failing := &failingPool{chunkStore: h.workers}
	failing.n.Store(2)
	srv := New(":0", h.store, h.ring, failing, &fakeHealth{})
	trace := "t-fail-" + fmt.Sprint(time.Now().UnixNano())
	require.Equal(t, http.StatusBadGateway, tracedUpload(t, srv, trace, "x", "this push will fail").Code)
	evs := eventsFor(trace)
	assert.Contains(t, kinds(evs), "upload.error")
	assert.Contains(t, kinds(evs), "upload.fail")
	assert.NotContains(t, kinds(evs), "upload.commit")
	bad := false
	for _, p := range ofKind(evs, "upload.push") {
		if p.Fields["ok"] == false {
			bad = true
			assert.NotEmpty(t, p.Fields["error"])
		}
	}
	assert.True(t, bad, "the failed push is reported with its error")
}

func TestDownloadEmitsFetchAndDone(t *testing.T) {
	h := newHarness(t)
	oid := h.upload(t, "d.txt", "download me").ObjectID
	trace := "t-dl-" + fmt.Sprint(time.Now().UnixNano())
	rec := tracedRequest(t, h.srv, http.MethodGet, "/api/v1/objects/"+oid, trace, nil, "")
	require.Equal(t, http.StatusOK, rec.Code)
	evs := eventsFor(trace)
	assert.Equal(t, "download.start", evs[0].Kind)
	assert.Equal(t, "download.done", evs[len(evs)-1].Kind)
	fetch := ofKind(evs, "download.fetch")
	require.NotEmpty(t, fetch)
	assert.Equal(t, true, fetch[0].Fields["ok"])
	assert.Equal(t, "primary", fetch[0].Fields["role"])
	assert.Equal(t, chunkIDOf("download me"), fetch[0].Fields["chunk_id"])
}

func TestCorruptPrimaryEmitsCorruptAndRepair(t *testing.T) {
	h := newHarness(t)
	content := "this copy will be damaged"
	oid := h.upload(t, "c.txt", content).ObjectID
	cid := chunkIDOf(content)
	rec, err := h.store.GetChunk(context.Background(), cid)
	require.NoError(t, err)
	primaryAddr := h.srv.workerAddr(rec.Replicas[0].WorkerID)
	h.workers.mu.Lock()
	flipped := []byte(content)
	flipped[3] ^= 0xFF
	h.workers.chunks[h.workers.key(primaryAddr, cid)] = flipped
	h.workers.mu.Unlock()

	trace := "t-corrupt-" + fmt.Sprint(time.Now().UnixNano())
	resp := tracedRequest(t, h.srv, http.MethodGet, "/api/v1/objects/"+oid, trace, nil, "")
	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, content, resp.Body.String(), "the client still gets the right bytes")
	require.Eventually(t, func() bool { return len(ofKind(eventsFor(trace), "download.repair")) == 1 }, 3*time.Second, 10*time.Millisecond)

	evs := eventsFor(trace)
	corrupt := ofKind(evs, "download.corrupt")
	require.Len(t, corrupt, 1)
	assert.Equal(t, rec.Replicas[0].WorkerID, corrupt[0].Fields["worker"])
	assert.NotEqual(t, cid, corrupt[0].Fields["got"], "the event shows the hash that was actually received")
	repair := ofKind(evs, "download.repair")[0]
	assert.Equal(t, true, repair.Fields["ok"])
	assert.Equal(t, rec.Replicas[1].WorkerID, repair.Fields["from"])
	assert.Equal(t, rec.Replicas[0].WorkerID, repair.Fields["to"])
	var roles []any
	for _, f := range ofKind(evs, "download.fetch") {
		roles = append(roles, f.Fields["role"])
	}
	assert.Contains(t, roles, "secondary", "after the corrupt primary the secondary served the chunk")
}

func TestDeleteAndRegistrationEmitEvents(t *testing.T) {
	h := newHarness(t)
	oid := h.upload(t, "gone.txt", "delete me please").ObjectID
	trace := "t-del-" + fmt.Sprint(time.Now().UnixNano())
	require.Equal(t, http.StatusNoContent, tracedRequest(t, h.srv, http.MethodDelete, "/api/v1/objects/"+oid, trace, nil, "").Code)
	del := ofKind(eventsFor(trace), "object.delete")
	require.Len(t, del, 1)
	assert.Equal(t, oid, del[0].Fields["object_id"])
	assert.EqualValues(t, 1, del[0].Fields["chunks_deleted"])

	before := events.Default.LastSeq()
	rec := tracedRequest(t, h.srv, http.MethodPost, "/internal/workers/register", "", strings.NewReader(`{"worker_id":"w9","address":"w9","port":8001}`), "application/json")
	require.Equal(t, http.StatusCreated, rec.Code)
	var found bool
	for _, e := range events.Default.Since(before, 0) {
		if e.Kind == "worker.register" && e.Fields["worker"] == "w9" {
			found = true
		}
	}
	assert.True(t, found)
}
