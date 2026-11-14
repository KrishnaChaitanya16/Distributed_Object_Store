package workerclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- circuit breaker ---

func TestCircuitBreaker_InitiallyClosed(t *testing.T) {
	cb := newCircuitBreaker()
	assert.NoError(t, cb.Allow())
	assert.Equal(t, "CLOSED", cb.State())
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := newCircuitBreaker()
	for i := 0; i < failureThreshold; i++ {
		cb.RecordFailure()
	}
	assert.Equal(t, "OPEN", cb.State())
	assert.ErrorIs(t, cb.Allow(), ErrCircuitOpen)
}

func TestCircuitBreaker_SuccessResetsClosed(t *testing.T) {
	cb := newCircuitBreaker()
	for i := 0; i < failureThreshold; i++ {
		cb.RecordFailure()
	}
	// Force into HALF_OPEN by backdating
	cb.mu.Lock()
	cb.lastFailureAt = cb.lastFailureAt.Add(-halfOpenTimeout - 1)
	cb.mu.Unlock()

	assert.NoError(t, cb.Allow()) // transitions to HALF_OPEN
	cb.RecordSuccess()
	assert.Equal(t, "CLOSED", cb.State())
	assert.NoError(t, cb.Allow())
}

func TestCircuitBreaker_StaysOpenBeforeTimeout(t *testing.T) {
	cb := newCircuitBreaker()
	for i := 0; i < failureThreshold; i++ {
		cb.RecordFailure()
	}
	assert.ErrorIs(t, cb.Allow(), ErrCircuitOpen)
}

// --- HTTP client via test server ---

func newTestServer(handler http.HandlerFunc) (*httptest.Server, *Pool) {
	srv := httptest.NewServer(handler)
	pool := New()
	// Override HTTP client to talk to test server without scheme prefix confusion
	pool.http = srv.Client()
	// Store the breaker for the test server addr
	return srv, pool
}

func workerAddr(srv *httptest.Server) string {
	// Pool prepends "http://", test server URL already has it — strip scheme
	return strings.TrimPrefix(srv.URL, "http://")
}

func TestPool_PutChunk_Success(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/internal/chunks/abc123", r.URL.Path)
		body, _ := io.ReadAll(r.Body)
		assert.Equal(t, "hello", string(body))
		w.WriteHeader(http.StatusCreated)
	})
	defer srv.Close()

	err := pool.PutChunk(context.Background(), workerAddr(srv), "abc123",
		strings.NewReader("hello"), 5)
	require.NoError(t, err)
}

func TestPool_PutChunk_WorkerError(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer srv.Close()

	err := pool.PutChunk(context.Background(), workerAddr(srv), "abc123",
		strings.NewReader("hello"), 5)
	assert.Error(t, err)
}

func TestPool_GetChunk_Success(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/internal/chunks/abc123", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("chunkdata")) //nolint:errcheck
	})
	defer srv.Close()

	rc, err := pool.GetChunk(context.Background(), workerAddr(srv), "abc123")
	require.NoError(t, err)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	assert.Equal(t, "chunkdata", string(data))
}

func TestPool_GetChunk_NotFound(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	defer srv.Close()

	_, err := pool.GetChunk(context.Background(), workerAddr(srv), "missing")
	assert.Error(t, err)
}

func TestPool_DeleteChunk_Success(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		w.WriteHeader(http.StatusNoContent)
	})
	defer srv.Close()

	err := pool.DeleteChunk(context.Background(), workerAddr(srv), "abc123")
	require.NoError(t, err)
}

func TestPool_ReplicatePeer_Success(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/internal/workers/replicate-peer", r.URL.Path)
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body) //nolint:errcheck
		assert.Equal(t, "abc123", body["chunk_id"])
		assert.Equal(t, "10.0.0.5:8080", body["target_addr"])
		w.WriteHeader(http.StatusOK)
	})
	defer srv.Close()

	err := pool.ReplicatePeer(context.Background(), workerAddr(srv), "abc123", "10.0.0.5:8080")
	require.NoError(t, err)
}

func TestPool_ListChunks_Success(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/internal/workers/w1/chunks", r.URL.Path)
		json.NewEncoder(w).Encode(map[string]any{"worker_id": "w1", "chunk_ids": []string{"id1", "id2", "id3"}, "count": 3}) //nolint:errcheck
	})
	defer srv.Close()

	ids, err := pool.ListChunks(context.Background(), workerAddr(srv), "w1")
	require.NoError(t, err)
	assert.Equal(t, []string{"id1", "id2", "id3"}, ids)
}

func TestPool_CircuitBreaker_OpensOnRepeatedFailures(t *testing.T) {
	srv, pool := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer srv.Close()

	addr := workerAddr(srv)
	for i := 0; i < failureThreshold; i++ {
		pool.PutChunk(context.Background(), addr, "x", strings.NewReader(""), 0) //nolint:errcheck
	}

	// Next call should be rejected by the open breaker without hitting server
	err := pool.PutChunk(context.Background(), addr, "x", strings.NewReader(""), 0)
	assert.ErrorIs(t, err, ErrCircuitOpen)
}
