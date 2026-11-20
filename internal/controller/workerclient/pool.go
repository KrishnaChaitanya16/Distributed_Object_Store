package workerclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

var errNotFound = errors.New("not found")

// bytesReader wraps a byte slice as an io.Reader (avoids importing bytes in client.go).
func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// Pool implements models.WorkerClientPool.
// Each worker address gets its own circuit breaker; all share one http.Client.
type Pool struct {
	mu       sync.Mutex
	breakers map[string]*circuitBreaker
	http     *http.Client
}

// New creates a Pool with sensible HTTP timeouts.
func New() *Pool {
	return &Pool{
		breakers: make(map[string]*circuitBreaker),
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (p *Pool) breaker(workerAddr string) *circuitBreaker {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cb, ok := p.breakers[workerAddr]; ok {
		return cb
	}
	cb := newCircuitBreaker()
	p.breakers[workerAddr] = cb
	return cb
}

func (p *Pool) call(workerAddr string, fn func(*workerClient) error) error {
	cb := p.breaker(workerAddr)
	if err := cb.Allow(); err != nil {
		slog.Warn("circuit breaker open — skipping worker", "worker_addr", workerAddr)
		return err
	}
	c := newWorkerClient("http://"+workerAddr, p.http)
	err := fn(c)
	if errors.Is(err, context.Canceled) {
		return err
	}
	if err != nil {
		cb.RecordFailure()
		state := cb.State()
		slog.Debug("worker call failed", "worker_addr", workerAddr, "state", state, "err", err)
		if state == "OPEN" {
			slog.Warn("circuit breaker opened", "worker_addr", workerAddr)
		}
		return err
	}
	cb.RecordSuccess()
	return nil
}

func (p *Pool) PutChunk(ctx context.Context, workerAddr string, chunkID string, data io.Reader, size int64) error {
	return p.call(workerAddr, func(c *workerClient) error {
		return c.PutChunk(ctx, chunkID, data, size)
	})
}

func (p *Pool) GetChunk(ctx context.Context, workerAddr string, chunkID string) (io.ReadCloser, error) {
	var rc io.ReadCloser
	err := p.call(workerAddr, func(c *workerClient) error {
		var e error
		rc, e = c.GetChunk(ctx, chunkID)
		return e
	})
	return rc, err
}

func (p *Pool) DeleteChunk(ctx context.Context, workerAddr string, chunkID string) error {
	return p.call(workerAddr, func(c *workerClient) error {
		return c.DeleteChunk(ctx, chunkID)
	})
}

func (p *Pool) ReplicatePeer(ctx context.Context, sourceAddr string, chunkID string, targetAddr string) error {
	return p.call(sourceAddr, func(c *workerClient) error {
		return c.ReplicatePeer(ctx, chunkID, targetAddr)
	})
}

func (p *Pool) ListChunks(ctx context.Context, workerAddr string, workerID string) ([]string, error) {
	var ids []string
	err := p.call(workerAddr, func(c *workerClient) error {
		var e error
		ids, e = c.ListChunks(ctx, workerID)
		return e
	})
	return ids, err
}
