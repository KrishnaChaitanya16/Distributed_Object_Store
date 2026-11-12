package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

type worker struct {
	id    string
	srv   *httptest.Server
	store *storage.Store
	dir   string
}

func (w *worker) addr() string        { return w.srv.Listener.Addr().String() }
func (w *worker) url(p string) string { return w.srv.URL + p }

func newWorker(t *testing.T, id string) *worker {
	t.Helper()
	root := t.TempDir()
	st, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(nil)
	srv.Config.Handler = New(st, id, srv.Listener.Addr().String(), srv.Client()).Handler()
	t.Cleanup(srv.Close)
	return &worker{id: id, srv: srv, store: st, dir: filepath.Join(root, "chunks")}
}

func newFakeWorker(t *testing.T, fs *fakeStore) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(New(fs, "w1", "self:1", http.DefaultClient).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func chunk(t *testing.T, n int) ([]byte, string) {
	t.Helper()
	b := make([]byte, n)
	rand.Read(b)
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

func do(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, url, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func expect(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d want %d (body %s)", resp.StatusCode, status, b)
	}
	if code == "" {
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("error Content-Type %q", ct)
	}
	var e models.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatalf("error body not JSON: %v", err)
	}
	if e.Code != code || e.Error == "" {
		t.Fatalf("got code=%q error=%q, want code %q with a message", e.Code, e.Error, code)
	}
}

func putChunk(t *testing.T, w *worker, data []byte, id string) {
	t.Helper()
	expect(t, do(t, http.MethodPut, w.url("/internal/chunks/"+id), data), http.StatusCreated, "")
}

func getBody(t *testing.T, w *worker, id string) []byte {
	t.Helper()
	resp := do(t, http.MethodGet, w.url("/internal/chunks/"+id), nil)
	expect(t, resp, http.StatusOK, "")
	b, _ := io.ReadAll(resp.Body)
	return b
}

// ── PUT ──────────────────────────────────────────────────────────────────────

func TestPutThenGet(t *testing.T) {
	for _, n := range []int{0, 1, 4096, storage.MaxChunkSize} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			w := newWorker(t, "w1")
			data, id := chunk(t, n)
			putChunk(t, w, data, id)
			resp := do(t, http.MethodGet, w.url("/internal/chunks/"+id), nil)
			expect(t, resp, http.StatusOK, "")
			if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
				t.Fatalf("Content-Type %q", ct)
			}
			if resp.ContentLength != int64(n) {
				t.Fatalf("Content-Length %d want %d", resp.ContentLength, n)
			}
			got, _ := io.ReadAll(resp.Body)
			if !bytes.Equal(got, data) {
				t.Fatal("body differs")
			}
		})
	}
}

func TestPutIsIdempotent(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 100)
	putChunk(t, w, data, id)
	putChunk(t, w, data, id)
	if !bytes.Equal(getBody(t, w, id), data) {
		t.Fatal("second PUT changed data")
	}
}

func TestPutRejections(t *testing.T) {
	data, id := chunk(t, 100)
	_, otherID := chunk(t, 100)
	cases := []struct {
		name   string
		id     string
		body   []byte
		status int
		code   string
	}{
		{"wrong hash", otherID, data, http.StatusBadRequest, "CHECKSUM_MISMATCH"},
		{"short id", "abc", data, http.StatusBadRequest, "INVALID_CHUNK_ID"},
		{"uppercase id", strings.ToUpper(id), data, http.StatusBadRequest, "INVALID_CHUNK_ID"},
		{"encoded traversal", "..%2F..%2F" + id[:58], data, http.StatusBadRequest, "INVALID_CHUNK_ID"},
		{"too large", id, make([]byte, storage.MaxChunkSize+1), http.StatusRequestEntityTooLarge, "CHUNK_TOO_LARGE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorker(t, "w1")
			expect(t, do(t, http.MethodPut, w.url("/internal/chunks/"+c.id), c.body), c.status, c.code)
			if ids, _ := w.store.List(context.Background()); len(ids) != 0 {
				t.Fatalf("rejected PUT stored %v", ids)
			}
		})
	}
}

func TestPutWithoutContentLength(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 100)
	req, _ := http.NewRequest(http.MethodPut, w.url("/internal/chunks/"+id), io.NopCloser(bytes.NewReader(data)))
	req.ContentLength = -1
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expect(t, resp, http.StatusLengthRequired, "LENGTH_REQUIRED")
}

// A client that declares more bytes than it sends must get 400, not a storage 500.
func TestPutTruncatedBody(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 100)
	conn, err := net.Dial("tcp", w.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "PUT /internal/chunks/%s HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n", id)
	conn.Write(data[:40])
	conn.(*net.TCPConn).CloseWrite()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	expect(t, resp, http.StatusBadRequest, "BODY_READ_FAILED")
	if ok, _ := w.store.Exists(context.Background(), id); ok {
		t.Fatal("truncated upload was stored")
	}
}

func TestPutStorageFailure(t *testing.T) {
	data, id := chunk(t, 10)
	srv := newFakeWorker(t, &fakeStore{writeErr: errors.New("disk on fire")})
	expect(t, do(t, http.MethodPut, srv.URL+"/internal/chunks/"+id, data), http.StatusInternalServerError, "STORAGE_ERROR")
}

// ── GET / HEAD ───────────────────────────────────────────────────────────────

func TestGetErrors(t *testing.T) {
	w := newWorker(t, "w1")
	_, id := chunk(t, 10)
	expect(t, do(t, http.MethodGet, w.url("/internal/chunks/"+id), nil), http.StatusNotFound, "CHUNK_NOT_FOUND")
	expect(t, do(t, http.MethodGet, w.url("/internal/chunks/nothex"), nil), http.StatusBadRequest, "INVALID_CHUNK_ID")

	os.WriteFile(filepath.Join(w.dir, id+".bin"), []byte{1, 2}, 0o644)
	expect(t, do(t, http.MethodGet, w.url("/internal/chunks/"+id), nil), http.StatusInternalServerError, "CHUNK_CORRUPT")
}

func TestGetStorageFailure(t *testing.T) {
	_, id := chunk(t, 10)
	srv := newFakeWorker(t, &fakeStore{readErr: errors.New("io error")})
	expect(t, do(t, http.MethodGet, srv.URL+"/internal/chunks/"+id, nil), http.StatusInternalServerError, "STORAGE_ERROR")
}

func TestHead(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 5000)
	putChunk(t, w, data, id)
	resp := do(t, http.MethodHead, w.url("/internal/chunks/"+id), nil)
	expect(t, resp, http.StatusOK, "")
	if resp.ContentLength != 5000 {
		t.Fatalf("HEAD Content-Length %d", resp.ContentLength)
	}
	if b, _ := io.ReadAll(resp.Body); len(b) != 0 {
		t.Fatal("HEAD returned a body")
	}
	expect(t, do(t, http.MethodHead, w.url("/internal/chunks/"+strings.Repeat("0", 64)), nil), http.StatusNotFound, "")
}

// The worker serves raw bytes; detecting a flipped bit is the controller's sha256 check and the scrubber's job.
func TestGetServesCorruptedBytesForControllerToCatch(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 100)
	putChunk(t, w, data, id)
	flip(t, filepath.Join(w.dir, id+".bin"), 10)
	got := getBody(t, w, id)
	if sum := sha256.Sum256(got); hex.EncodeToString(sum[:]) == id {
		t.Fatal("expected corrupted bytes to fail the sha256 check")
	}
}

func flip(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := []byte{0}
	f.ReadAt(b, off)
	b[0] ^= 0xFF
	f.WriteAt(b, off)
}

// ── DELETE ───────────────────────────────────────────────────────────────────

func TestDelete(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 10)
	putChunk(t, w, data, id)
	expect(t, do(t, http.MethodDelete, w.url("/internal/chunks/"+id), nil), http.StatusNoContent, "")
	expect(t, do(t, http.MethodGet, w.url("/internal/chunks/"+id), nil), http.StatusNotFound, "CHUNK_NOT_FOUND")
	expect(t, do(t, http.MethodDelete, w.url("/internal/chunks/"+id), nil), http.StatusNotFound, "CHUNK_NOT_FOUND")
	expect(t, do(t, http.MethodDelete, w.url("/internal/chunks/xyz"), nil), http.StatusBadRequest, "INVALID_CHUNK_ID")
}

func TestDeleteStorageFailure(t *testing.T) {
	_, id := chunk(t, 10)
	srv := newFakeWorker(t, &fakeStore{deleteErr: errors.New("io error")})
	expect(t, do(t, http.MethodDelete, srv.URL+"/internal/chunks/"+id, nil), http.StatusInternalServerError, "STORAGE_ERROR")
}

// ── Inventory ────────────────────────────────────────────────────────────────

func inventory(t *testing.T, w *worker) models.InventoryResponse {
	t.Helper()
	resp := do(t, http.MethodGet, w.url("/internal/workers/"+w.id+"/chunks"), nil)
	expect(t, resp, http.StatusOK, "")
	var inv models.InventoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&inv); err != nil {
		t.Fatal(err)
	}
	return inv
}

func TestInventoryEmptyEncodesArray(t *testing.T) {
	w := newWorker(t, "w1")
	resp := do(t, http.MethodGet, w.url("/internal/workers/w1/chunks"), nil)
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), `"chunk_ids":[]`) {
		t.Fatalf("empty inventory must encode [] not null: %s", raw)
	}
}

func TestInventoryListsChunks(t *testing.T) {
	w := newWorker(t, "w1")
	want := map[string]bool{}
	for i := 0; i < 5; i++ {
		data, id := chunk(t, 50)
		putChunk(t, w, data, id)
		want[id] = true
	}
	inv := inventory(t, w)
	if inv.WorkerID != "w1" || inv.Count != 5 || len(inv.ChunkIDs) != 5 {
		t.Fatalf("got %+v", inv)
	}
	for _, id := range inv.ChunkIDs {
		if !want[id] {
			t.Fatalf("unexpected id %s", id)
		}
	}
}

func TestInventoryWrongWorker(t *testing.T) {
	w := newWorker(t, "w1")
	expect(t, do(t, http.MethodGet, w.url("/internal/workers/w2/chunks"), nil), http.StatusNotFound, "WORKER_MISMATCH")
}

func TestInventoryStorageFailure(t *testing.T) {
	srv := newFakeWorker(t, &fakeStore{listErr: errors.New("io error")})
	expect(t, do(t, http.MethodGet, srv.URL+"/internal/workers/w1/chunks", nil), http.StatusInternalServerError, "STORAGE_ERROR")
}

// ── Replicate peer ───────────────────────────────────────────────────────────

func replicate(t *testing.T, src *worker, body string) *http.Response {
	t.Helper()
	return do(t, http.MethodPost, src.url("/internal/workers/replicate-peer"), []byte(body))
}

func replJSON(id, target string) string {
	b, _ := json.Marshal(models.ReplicatePeerRequest{ChunkID: id, TargetAddr: target})
	return string(b)
}

func TestReplicatePeerCopiesChunk(t *testing.T) {
	for _, n := range []int{0, 1, storage.MaxChunkSize} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			a, b := newWorker(t, "w1"), newWorker(t, "w2")
			data, id := chunk(t, n)
			putChunk(t, a, data, id)
			expect(t, replicate(t, a, replJSON(id, b.addr())), http.StatusNoContent, "")
			if !bytes.Equal(getBody(t, b, id), data) {
				t.Fatal("target copy differs")
			}
			if ok, err := b.store.VerifyCRC(context.Background(), id); !ok || err != nil {
				t.Fatalf("target crc ok=%v err=%v", ok, err)
			}
			if !bytes.Equal(getBody(t, a, id), data) {
				t.Fatal("source copy changed")
			}
		})
	}
}

func TestReplicatePeerOverwritesExistingTargetCopy(t *testing.T) {
	a, b := newWorker(t, "w1"), newWorker(t, "w2")
	data, id := chunk(t, 100)
	putChunk(t, a, data, id)
	putChunk(t, b, data, id)
	flip(t, filepath.Join(b.dir, id+".bin"), 5)
	expect(t, replicate(t, a, replJSON(id, b.addr())), http.StatusNoContent, "")
	if ok, _ := b.store.VerifyCRC(context.Background(), id); !ok {
		t.Fatal("replicate-peer did not repair the target copy")
	}
}

// A bit-flipped source must never spread: the target's sha256 check refuses it.
func TestReplicatePeerRefusesToSpreadCorruption(t *testing.T) {
	a, b := newWorker(t, "w1"), newWorker(t, "w2")
	data, id := chunk(t, 1000)
	putChunk(t, a, data, id)
	flip(t, filepath.Join(a.dir, id+".bin"), 500)
	expect(t, replicate(t, a, replJSON(id, b.addr())), http.StatusBadGateway, "PEER_REJECTED")
	if ok, _ := b.store.Exists(context.Background(), id); ok {
		t.Fatal("corrupted chunk was replicated")
	}
}

func TestReplicatePeerRequestValidation(t *testing.T) {
	a, b := newWorker(t, "w1"), newWorker(t, "w2")
	data, id := chunk(t, 10)
	putChunk(t, a, data, id)
	_, missing := chunk(t, 10)
	cases := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"empty body", "", http.StatusBadRequest, "INVALID_REQUEST"},
		{"not json", "chunk please", http.StatusBadRequest, "INVALID_REQUEST"},
		{"wrong json type", `{"chunk_id": 5}`, http.StatusBadRequest, "INVALID_REQUEST"},
		{"oversized body", `{"chunk_id":"` + strings.Repeat("a", 5000) + `"}`, http.StatusBadRequest, "INVALID_REQUEST"},
		{"missing target", replJSON(id, ""), http.StatusBadRequest, "INVALID_TARGET"},
		{"target without port", replJSON(id, "worker2"), http.StatusBadRequest, "INVALID_TARGET"},
		{"target with scheme", replJSON(id, "http://worker2:8001"), http.StatusBadRequest, "INVALID_TARGET"},
		{"target without host", replJSON(id, ":8001"), http.StatusBadRequest, "INVALID_TARGET"},
		{"target port zero", replJSON(id, "worker2:0"), http.StatusBadRequest, "INVALID_TARGET"},
		{"target with path", replJSON(id, "worker2:8001/evil"), http.StatusBadRequest, "INVALID_TARGET"},
		{"target is self", replJSON(id, a.addr()), http.StatusBadRequest, "TARGET_IS_SELF"},
		{"missing chunk id", replJSON("", b.addr()), http.StatusBadRequest, "INVALID_CHUNK_ID"},
		{"invalid chunk id", replJSON("../x", b.addr()), http.StatusBadRequest, "INVALID_CHUNK_ID"},
		{"chunk not on source", replJSON(missing, b.addr()), http.StatusNotFound, "CHUNK_NOT_FOUND"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expect(t, replicate(t, a, c.body), c.status, c.code)
		})
	}
	if ids, _ := b.store.List(context.Background()); len(ids) != 0 {
		t.Fatalf("invalid requests reached target: %v", ids)
	}
}

func TestReplicatePeerTargetUnreachable(t *testing.T) {
	a := newWorker(t, "w1")
	data, id := chunk(t, 10)
	putChunk(t, a, data, id)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	expect(t, replicate(t, a, replJSON(id, dead)), http.StatusBadGateway, "PEER_UNREACHABLE")
}

func TestReplicatePeerTargetErrors(t *testing.T) {
	a := newWorker(t, "w1")
	data, id := chunk(t, 10)
	putChunk(t, a, data, id)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInsufficientStorage)
	}))
	defer bad.Close()
	expect(t, replicate(t, a, replJSON(id, bad.Listener.Addr().String())), http.StatusBadGateway, "PEER_REJECTED")
}

// ── Routing ──────────────────────────────────────────────────────────────────

func TestRouting(t *testing.T) {
	w := newWorker(t, "w1")
	_, id := chunk(t, 10)
	cases := []struct {
		method, path string
		status       int
	}{
		{http.MethodPost, "/internal/chunks/" + id, http.StatusMethodNotAllowed},
		{http.MethodPatch, "/internal/chunks/" + id, http.StatusMethodNotAllowed},
		{http.MethodGet, "/internal/workers/replicate-peer", http.StatusMethodNotAllowed},
		{http.MethodPut, "/internal/workers/w1/chunks", http.StatusMethodNotAllowed},
		{http.MethodGet, "/internal/chunks/", http.StatusNotFound},
		{http.MethodGet, "/", http.StatusNotFound},
		{http.MethodGet, "/internal/chunks/" + id + "/extra", http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			expect(t, do(t, c.method, w.url(c.path), nil), c.status, "")
		})
	}
}

// ── Concurrency ──────────────────────────────────────────────────────────────

func TestConcurrentRequests(t *testing.T) {
	a, b := newWorker(t, "w1"), newWorker(t, "w2")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, id := chunk(t, 20000)
			req, _ := http.NewRequest(http.MethodPut, a.url("/internal/chunks/"+id), bytes.NewReader(data))
			if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 201 {
				t.Errorf("put failed: %v", err)
				return
			} else {
				resp.Body.Close()
			}
			resp, err := http.Post(a.url("/internal/workers/replicate-peer"), "application/json", strings.NewReader(replJSON(id, b.addr())))
			if err != nil || resp.StatusCode != http.StatusNoContent {
				t.Errorf("replicate failed: %v", err)
				return
			}
			resp.Body.Close()
			resp, err = http.Get(b.url("/internal/chunks/" + id))
			if err != nil {
				t.Error(err)
				return
			}
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if !bytes.Equal(got, data) {
				t.Errorf("chunk %s differs on target", id[:8])
			}
		}()
	}
	wg.Wait()
	if inv := inventory(t, b); inv.Count != 20 {
		t.Fatalf("target has %d chunks, want 20", inv.Count)
	}
}

// ── fake store for forcing storage failures ──────────────────────────────────

type fakeStore struct {
	writeErr, readErr, deleteErr, listErr error
}

func (f *fakeStore) Write(_ context.Context, _ string, r io.Reader, _ int64) error {
	io.Copy(io.Discard, r)
	return f.writeErr
}
func (f *fakeStore) Read(context.Context, string) (io.ReadCloser, error) { return nil, f.readErr }
func (f *fakeStore) Delete(context.Context, string) error                { return f.deleteErr }
func (f *fakeStore) Exists(context.Context, string) (bool, error)        { return false, nil }
func (f *fakeStore) List(context.Context) ([]string, error)              { return nil, f.listErr }
func (f *fakeStore) VerifyCRC(context.Context, string) (bool, error)     { return false, nil }
