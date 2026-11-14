package registration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

var fast = Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond}

// fakeController answers register and heartbeat with programmable status codes and records what it received.
type fakeController struct {
	mu          sync.Mutex
	regStatus   []int
	regBody     string
	hbStatus    int
	registers   atomic.Int32
	heartbeats  atomic.Int32
	lastReg     models.RegisterRequest
	lastHB      models.HeartbeatRequest
	contentType string
}

func (f *fakeController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contentType = r.Header.Get("Content-Type")
	switch r.URL.Path {
	case "/internal/workers/register":
		json.NewDecoder(r.Body).Decode(&f.lastReg)
		n := int(f.registers.Add(1))
		status := http.StatusOK
		if n <= len(f.regStatus) {
			status = f.regStatus[n-1]
		}
		w.WriteHeader(status)
		body := f.regBody
		if body == "" {
			body = `{"status":"registered","vnode_count":50}`
		}
		w.Write([]byte(body))
	case "/internal/workers/heartbeat":
		json.NewDecoder(r.Body).Decode(&f.lastHB)
		f.heartbeats.Add(1)
		status := f.hbStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

var req = models.RegisterRequest{WorkerID: "worker1", Address: "worker1", Port: 8001, CapacityBytes: 1 << 30}

func newClient(t *testing.T, fc *fakeController) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(fc)
	t.Cleanup(srv.Close)
	c := New(srv.Listener.Addr().String(), req, srv.Client())
	c.Backoff = fast
	c.Logf = func(string, ...any) {}
	return c, srv
}

// ── Backoff ──────────────────────────────────────────────────────────────────

func TestBackoffDoublesThenCaps(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	want := []time.Duration{100, 200, 400, 800, 1600, 3200, 5000, 5000}
	for i, w := range want {
		if got := b.Delay(i); got != w*time.Millisecond {
			t.Fatalf("attempt %d: %s want %s", i, got, w*time.Millisecond)
		}
	}
}

func TestBackoffExtremeAttemptsDoNotOverflow(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	for _, a := range []int{-1, 40, 62, 63, 64, 1000, 1 << 30} {
		if got := b.Delay(a); got != 5*time.Second {
			t.Fatalf("attempt %d: %s want cap", a, got)
		}
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second, Jitter: 100 * time.Millisecond}
	seen := map[time.Duration]bool{}
	for i := 0; i < 1000; i++ {
		d := b.Delay(0)
		if d < 100*time.Millisecond || d >= 200*time.Millisecond {
			t.Fatalf("delay %s outside [100ms, 200ms)", d)
		}
		seen[d] = true
	}
	if len(seen) < 100 {
		t.Fatalf("jitter looks constant: %d distinct values", len(seen))
	}
}

func TestBackoffZeroBaseUsesMax(t *testing.T) {
	if d := (Backoff{Max: time.Second}).Delay(3); d != time.Second {
		t.Fatalf("got %s", d)
	}
}

func TestDefaultBackoffMatchesDesign(t *testing.T) {
	if DefaultBackoff != (Backoff{100 * time.Millisecond, 5 * time.Second, 100 * time.Millisecond}) {
		t.Fatalf("DefaultBackoff %+v differs from DESIGN.md 2.15", DefaultBackoff)
	}
}

// ── Register ─────────────────────────────────────────────────────────────────

func TestRegisterFirstTry(t *testing.T) {
	fc := &fakeController{}
	c, _ := newClient(t, fc)
	resp, err := c.Register(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if resp.VnodeCount != 50 || resp.Status != "registered" {
		t.Fatalf("resp %+v", resp)
	}
	if fc.registers.Load() != 1 {
		t.Fatalf("register called %d times", fc.registers.Load())
	}
	if fc.lastReg != req {
		t.Fatalf("controller got %+v want %+v", fc.lastReg, req)
	}
	if fc.contentType != "application/json" {
		t.Fatalf("Content-Type %q", fc.contentType)
	}
}

func TestRegisterAccepts201(t *testing.T) {
	fc := &fakeController{regStatus: []int{http.StatusCreated}}
	c, _ := newClient(t, fc)
	if _, err := c.Register(context.Background()); err != nil || fc.registers.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, fc.registers.Load())
	}
}

func TestRegisterRetriesUntilAccepted(t *testing.T) {
	fc := &fakeController{regStatus: []int{500, 503, 400, 404, 502}}
	c, _ := newClient(t, fc)
	if _, err := c.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fc.registers.Load() != 6 {
		t.Fatalf("register called %d times, want 6", fc.registers.Load())
	}
}

func TestRegisterToleratesMalformedReply(t *testing.T) {
	for _, body := range []string{"not json", "{", `{"vnode_count":"fifty"}`, "null"} {
		fc := &fakeController{regBody: body}
		c, _ := newClient(t, fc)
		if _, err := c.Register(context.Background()); err != nil || fc.registers.Load() != 1 {
			t.Fatalf("body %q: err=%v calls=%d", body, err, fc.registers.Load())
		}
	}
}

func TestRegisterWhileControllerDownStopsOnCancel(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	var attempts atomic.Int32
	c := New(addr, req, &http.Client{Timeout: time.Second})
	c.Backoff = fast
	c.Logf = func(f string, _ ...any) {
		if strings.HasPrefix(f, "register attempt") {
			attempts.Add(1)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Register(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Register did not stop promptly after ctx expired")
	}
	if attempts.Load() < 3 {
		t.Fatalf("only %d attempts while controller was down", attempts.Load())
	}
}

func TestRegisterSucceedsOnceControllerComesUp(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	fc := &fakeController{}
	c := New(addr, req, &http.Client{Timeout: time.Second})
	c.Backoff = fast
	c.Logf = func(string, ...any) {}
	go func() {
		time.Sleep(50 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return
		}
		srv := &http.Server{Handler: fc}
		t.Cleanup(func() { srv.Close() })
		srv.Serve(l)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Register(ctx); err != nil {
		t.Fatalf("never registered: %v", err)
	}
}

func TestRegisterAlreadyCancelled(t *testing.T) {
	fc := &fakeController{}
	c, _ := newClient(t, fc)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Register(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if fc.registers.Load() != 0 {
		t.Fatal("request sent despite cancelled context")
	}
}

func TestRegisterSlowControllerTimesOutAndRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			time.Sleep(300 * time.Millisecond)
		}
	}))
	defer srv.Close()
	c := New(srv.Listener.Addr().String(), req, &http.Client{Timeout: 50 * time.Millisecond})
	c.Backoff = fast
	c.Logf = func(string, ...any) {}
	if _, err := c.Register(context.Background()); err != nil || calls.Load() < 2 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

// ── Heartbeats ───────────────────────────────────────────────────────────────

func runFor(c *Client, d, interval time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	c.Run(ctx, interval)
}

func TestHeartbeatsAtInterval(t *testing.T) {
	fc := &fakeController{}
	c, _ := newClient(t, fc)
	runFor(c, 220*time.Millisecond, 20*time.Millisecond)
	n := fc.heartbeats.Load()
	if n < 5 || n > 12 {
		t.Fatalf("%d heartbeats in 220ms at 20ms interval", n)
	}
	if fc.lastHB.WorkerID != "worker1" {
		t.Fatalf("heartbeat body %+v", fc.lastHB)
	}
	if fc.registers.Load() != 0 {
		t.Fatal("healthy heartbeats must not re-register")
	}
}

func TestHeartbeatStopsOnCancel(t *testing.T) {
	fc := &fakeController{}
	c, _ := newClient(t, fc)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { c.Run(ctx, 5*time.Millisecond); close(done) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	before := fc.heartbeats.Load()
	time.Sleep(30 * time.Millisecond)
	if fc.heartbeats.Load() != before {
		t.Fatal("heartbeats continued after Run returned")
	}
}

func TestHeartbeat404TriggersReRegister(t *testing.T) {
	fc := &fakeController{hbStatus: http.StatusNotFound}
	c, _ := newClient(t, fc)
	runFor(c, 100*time.Millisecond, 20*time.Millisecond)
	if fc.registers.Load() < 1 {
		t.Fatal("404 heartbeat did not trigger re-registration")
	}
	if fc.lastReg != req {
		t.Fatalf("re-register payload %+v", fc.lastReg)
	}
}

func TestHeartbeatKeepsGoingAfterErrors(t *testing.T) {
	fc := &fakeController{hbStatus: http.StatusInternalServerError}
	c, _ := newClient(t, fc)
	runFor(c, 150*time.Millisecond, 15*time.Millisecond)
	if fc.heartbeats.Load() < 4 {
		t.Fatalf("only %d heartbeats; loop gave up on 500s", fc.heartbeats.Load())
	}
	if fc.registers.Load() != 0 {
		t.Fatal("500 must not trigger re-registration")
	}
}

func TestHeartbeatSurvivesControllerOutage(t *testing.T) {
	fc := &fakeController{}
	c, srv := newClient(t, fc)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	go func() { time.Sleep(40 * time.Millisecond); srv.CloseClientConnections(); srv.Close() }()
	c.Run(ctx, 10*time.Millisecond)
}
