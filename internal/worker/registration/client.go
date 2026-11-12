package registration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// Backoff computes min(Base*2^attempt, Max) + rand[0, Jitter).
type Backoff struct {
	Base, Max, Jitter time.Duration
}

var DefaultBackoff = Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second, Jitter: 100 * time.Millisecond}

func (b Backoff) Delay(attempt int) time.Duration {
	d := b.Max
	if attempt >= 0 && attempt < 62 {
		if e := b.Base << attempt; e > 0 && e < b.Max {
			d = e
		}
	}
	if b.Jitter > 0 {
		d += time.Duration(rand.Int64N(int64(b.Jitter)))
	}
	return d
}

// Client registers this worker with the controller and keeps its lease alive with heartbeats.
type Client struct {
	baseURL string
	req     models.RegisterRequest
	http    *http.Client
	Backoff Backoff
	Logf    func(format string, args ...any)
}

func New(controllerAddr string, req models.RegisterRequest, hc *http.Client) *Client {
	return &Client{baseURL: "http://" + controllerAddr, req: req, http: hc, Backoff: DefaultBackoff, Logf: log.Printf}
}

// Register retries until the controller accepts this worker or ctx is cancelled.
func (c *Client) Register(ctx context.Context) (models.RegisterResponse, error) {
	for attempt := 0; ; attempt++ {
		resp, err := c.registerOnce(ctx)
		if err == nil {
			c.Logf("registered with controller as %s (vnodes=%d)", c.req.WorkerID, resp.VnodeCount)
			return resp, nil
		}
		if ctx.Err() != nil {
			return models.RegisterResponse{}, ctx.Err()
		}
		delay := c.Backoff.Delay(attempt)
		c.Logf("register attempt %d failed: %v; retrying in %s", attempt+1, err, delay.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return models.RegisterResponse{}, ctx.Err()
		case <-time.After(delay):
		}
	}
}

// Run sends a heartbeat every interval until ctx is cancelled, re-registering if the controller has forgotten us.
func (c *Client) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		status, err := c.post(ctx, "/internal/workers/heartbeat", models.HeartbeatRequest{WorkerID: c.req.WorkerID}, nil)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				c.Logf("heartbeat failed: %v", err)
			}
		case status == http.StatusNotFound:
			c.Logf("controller does not know %s, re-registering", c.req.WorkerID)
			c.Register(ctx)
		case status/100 != 2:
			c.Logf("heartbeat rejected with status %d", status)
		}
	}
}

func (c *Client) registerOnce(ctx context.Context) (models.RegisterResponse, error) {
	var out models.RegisterResponse
	status, err := c.post(ctx, "/internal/workers/register", c.req, &out)
	if err != nil {
		return out, err
	}
	if status/100 != 2 {
		return out, fmt.Errorf("controller answered %d", status)
	}
	return out, nil
}

// post sends v as JSON and, on 2xx, decodes the reply into out when out is non-nil; a malformed reply is tolerated.
func (c *Client) post(ctx context.Context, path string, v, out any) (int, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if out != nil && resp.StatusCode/100 == 2 && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			c.Logf("ignoring malformed controller reply to %s: %v", path, err)
		}
	}
	return resp.StatusCode, nil
}
