package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	def := func(id string) workerConfig {
		return workerConfig{workerID: id, address: id, port: 8001, dataDir: "/data", controllerAddr: "controller:8080", heartbeatInterval: 2 * time.Second, scrubInterval: 30 * time.Second, scrubPause: 10 * time.Millisecond}
	}
	cases := []struct {
		name    string
		env     map[string]string
		want    workerConfig
		wantErr bool
	}{
		{name: "only required id set", env: map[string]string{"WORKER_ID": "worker1"}, want: def("worker1")},
		{
			name: "all overridden",
			env: map[string]string{
				"WORKER_ID": "worker3", "WORKER_ADDRESS": "10.0.0.3", "WORKER_PORT": "8003",
				"DATA_DIR": "/mnt/chunks", "CONTROLLER_ADDRESS": "10.0.0.5:9090", "HEARTBEAT_INTERVAL_S": "5",
				"SCRUB_INTERVAL_S": "120", "SCRUB_PAUSE_MS": "25",
			},
			want: workerConfig{workerID: "worker3", address: "10.0.0.3", port: 8003, dataDir: "/mnt/chunks", controllerAddr: "10.0.0.5:9090", heartbeatInterval: 5 * time.Second, scrubInterval: 120 * time.Second, scrubPause: 25 * time.Millisecond},
		},
		{name: "scrub pause zero means unthrottled", env: map[string]string{"WORKER_ID": "w", "SCRUB_PAUSE_MS": "0"}, want: func() workerConfig { c := def("w"); c.scrubPause = 0; return c }()},
		{name: "scrub interval one second", env: map[string]string{"WORKER_ID": "w", "SCRUB_INTERVAL_S": "1"}, want: func() workerConfig { c := def("w"); c.scrubInterval = time.Second; return c }()},
		{name: "zero scrub interval", env: map[string]string{"WORKER_ID": "w", "SCRUB_INTERVAL_S": "0"}, wantErr: true},
		{name: "negative scrub interval", env: map[string]string{"WORKER_ID": "w", "SCRUB_INTERVAL_S": "-30"}, wantErr: true},
		{name: "scrub interval with unit", env: map[string]string{"WORKER_ID": "w", "SCRUB_INTERVAL_S": "30s"}, wantErr: true},
		{name: "negative scrub pause", env: map[string]string{"WORKER_ID": "w", "SCRUB_PAUSE_MS": "-1"}, wantErr: true},
		{name: "non-numeric scrub pause", env: map[string]string{"WORKER_ID": "w", "SCRUB_PAUSE_MS": "fast"}, wantErr: true},
		{name: "id is trimmed and used as address", env: map[string]string{"WORKER_ID": "  worker2 "}, want: def("worker2")},
		{name: "missing id", env: nil, wantErr: true},
		{name: "blank id", env: map[string]string{"WORKER_ID": "   "}, wantErr: true},
		{name: "invalid port", env: map[string]string{"WORKER_ID": "w", "WORKER_PORT": "80a"}, wantErr: true},
		{name: "port out of range", env: map[string]string{"WORKER_ID": "w", "WORKER_PORT": "100000"}, wantErr: true},
		{name: "controller without port", env: map[string]string{"WORKER_ID": "w", "CONTROLLER_ADDRESS": "controller"}, wantErr: true},
		{name: "controller with scheme", env: map[string]string{"WORKER_ID": "w", "CONTROLLER_ADDRESS": "http://controller:8080"}, wantErr: true},
		{name: "controller bad port", env: map[string]string{"WORKER_ID": "w", "CONTROLLER_ADDRESS": "controller:0"}, wantErr: true},
		{name: "zero heartbeat", env: map[string]string{"WORKER_ID": "w", "HEARTBEAT_INTERVAL_S": "0"}, wantErr: true},
		{name: "negative heartbeat", env: map[string]string{"WORKER_ID": "w", "HEARTBEAT_INTERVAL_S": "-2"}, wantErr: true},
		{name: "non-numeric heartbeat", env: map[string]string{"WORKER_ID": "w", "HEARTBEAT_INTERVAL_S": "2s"}, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := loadConfig(func(k string) string { return c.env[k] })
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Fatalf("got %+v want %+v", got, c.want)
			}
		})
	}
}

func TestDiskCapacity(t *testing.T) {
	if c := diskCapacity(t.TempDir()); c <= 0 {
		t.Fatalf("capacity of a real dir should be positive, got %d", c)
	}
	if c := diskCapacity(filepath.Join(t.TempDir(), "missing")); c != 0 {
		t.Fatalf("capacity of a missing dir should be 0, got %d", c)
	}
}

func TestHTTPServerTimeouts(t *testing.T) {
	srv := newHTTPServer(8001, http.NotFoundHandler())
	if srv.Addr != ":8001" {
		t.Fatalf("Addr %q", srv.Addr)
	}
	if srv.ReadHeaderTimeout != 5*time.Second || srv.ReadTimeout != 60*time.Second || srv.IdleTimeout != 120*time.Second {
		t.Fatalf("timeouts header=%s read=%s idle=%s", srv.ReadHeaderTimeout, srv.ReadTimeout, srv.IdleTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Fatal("WriteTimeout would cut off slow chunk downloads")
	}
}

// startShortTimeouts serves h with the production server shape but millisecond timeouts.
func startShortTimeouts(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := newHTTPServer(0, h)
	srv.ReadHeaderTimeout, srv.ReadTimeout, srv.IdleTimeout = 100*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

func closedWithin(t *testing.T, c net.Conn, d time.Duration) bool {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(d))
	_, err := io.ReadAll(c)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return false
	}
	return true
}

func TestStalledHeadersAreDisconnected(t *testing.T) {
	addr := startShortTimeouts(t, http.NotFoundHandler())
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	fmt.Fprint(c, "PUT /x HTTP/1.1\r\nHost: x\r\n")
	if !closedWithin(t, c, 2*time.Second) {
		t.Fatal("client stalling mid-headers kept the connection open")
	}
}

func TestStalledBodyIsDisconnected(t *testing.T) {
	var bodyErr error
	done := make(chan struct{})
	addr := startShortTimeouts(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, bodyErr = io.ReadAll(r.Body)
		close(done)
	}))
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	fmt.Fprint(c, "PUT /x HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\nonly-a-few-bytes")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler still blocked reading a stalled body")
	}
	if bodyErr == nil {
		t.Fatal("stalled body read should fail with a timeout")
	}
}

func TestIdleKeepAliveIsClosed(t *testing.T) {
	addr := startShortTimeouts(t, http.NotFoundHandler())
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	fmt.Fprint(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !closedWithin(t, c, 2*time.Second) {
		t.Fatal("idle keep-alive connection was never closed")
	}
}

func TestSlowButSteadyUploadCompletes(t *testing.T) {
	got := make(chan int, 1)
	addr := startShortTimeouts(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- len(b)
	}))
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	fmt.Fprint(c, "PUT /x HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n")
	for i := 0; i < 4; i++ {
		time.Sleep(30 * time.Millisecond)
		c.Write([]byte("a"))
	}
	if n := <-got; n != 4 {
		t.Fatalf("upload within ReadTimeout got %d bytes", n)
	}
}
