package web

import (
	"bytes"
	"context"
	"encoding/json"
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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
)

// fakeDocker is a Docker Engine API on a unix socket that records what it was asked.
type fakeDocker struct {
	mu           sync.Mutex
	sock         string
	reqs         []string
	execs        map[string][]string
	pings        int
	pingCode     int
	stopCode     int
	startCode    int
	createCode   int
	exitFor      func(cmd []string) int
	runningPolls int
}

func newFakeDocker(t *testing.T) *fakeDocker {
	t.Helper()
	dir, err := os.MkdirTemp("", "fd")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	d := &fakeDocker{sock: filepath.Join(dir, "d.sock"), execs: map[string][]string{}, pingCode: 200, stopCode: 204, startCode: 204, createCode: 201,
		exitFor: func([]string) int { return 0 }}
	l, err := net.Listen("unix", d.sock)
	require.NoError(t, err)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_ping", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		d.pings++
		code := d.pingCode
		d.mu.Unlock()
		w.WriteHeader(code)
		io.WriteString(w, "OK")
	})
	mux.HandleFunc("POST /containers/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
		d.record(r.Method + " " + r.URL.RequestURI())
		d.mu.Lock()
		code := d.stopCode
		d.mu.Unlock()
		w.WriteHeader(code)
		if code >= 400 {
			io.WriteString(w, `{"message":"boom"}`)
		}
	})
	mux.HandleFunc("POST /containers/{id}/start", func(w http.ResponseWriter, r *http.Request) {
		d.record(r.Method + " " + r.URL.RequestURI())
		d.mu.Lock()
		code := d.startCode
		d.mu.Unlock()
		w.WriteHeader(code)
	})
	mux.HandleFunc("POST /containers/{id}/exec", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Cmd []string }
		json.NewDecoder(r.Body).Decode(&body) //nolint:errcheck
		d.mu.Lock()
		id := fmt.Sprintf("exec%d", len(d.execs)+1)
		d.execs[id] = body.Cmd
		d.reqs = append(d.reqs, "EXEC "+r.PathValue("id")+" "+strings.Join(body.Cmd, " "))
		code := d.createCode
		d.mu.Unlock()
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]string{"Id": id}) //nolint:errcheck
	})
	mux.HandleFunc("POST /exec/{id}/start", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /exec/{id}/json", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		cmd := d.execs[r.PathValue("id")]
		running := d.runningPolls > 0
		if running {
			d.runningPolls--
		}
		exit := d.exitFor(cmd)
		d.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"Running": running, "ExitCode": exit}) //nolint:errcheck
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(l) //nolint:errcheck
	t.Cleanup(func() { srv.Close() })
	return d
}

func (d *fakeDocker) record(s string) { d.mu.Lock(); d.reqs = append(d.reqs, s); d.mu.Unlock() }
func (d *fakeDocker) log() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.reqs...)
}

// --- the Docker client on its own ---

func TestDockerPing(t *testing.T) {
	fd := newFakeDocker(t)
	cli := NewDocker(fd.sock)
	assert.NoError(t, cli.Ping(context.Background()))
	fd.pingCode = 500
	assert.Error(t, cli.Ping(context.Background()))
	assert.Error(t, NewDocker(filepath.Join(t.TempDir(), "nothing.sock")).Ping(context.Background()), "no daemon, no ping")
}

func TestDockerStopAndStartAcceptAlreadyInThatState(t *testing.T) {
	fd := newFakeDocker(t)
	cli := NewDocker(fd.sock)
	ctx := context.Background()
	for _, code := range []int{204, 304} {
		fd.stopCode, fd.startCode = code, code
		assert.NoError(t, cli.Stop(ctx, "worker1"), code)
		assert.NoError(t, cli.Start(ctx, "worker1"), code)
	}
	assert.Contains(t, fd.log(), "POST /containers/worker1/stop?t=1")
	assert.Contains(t, fd.log(), "POST /containers/worker1/start")
	fd.stopCode = 500
	err := cli.Stop(ctx, "worker1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
	assert.Contains(t, err.Error(), "boom", "Docker's own message reaches the user")
	fd.startCode = 404
	assert.Error(t, cli.Start(ctx, "worker1"))
}

func TestDockerNamesAreEscapedInThePath(t *testing.T) {
	fd := newFakeDocker(t)
	require.NoError(t, NewDocker(fd.sock).Stop(context.Background(), "a b"))
	assert.Contains(t, fd.log()[0], "/containers/a%20b/stop")
}

func TestDockerExecReturnsTheExitCodeAndRunsWithoutAShell(t *testing.T) {
	fd := newFakeDocker(t)
	fd.exitFor = func(cmd []string) int {
		if len(cmd) > 0 && cmd[0] == "false" {
			return 3
		}
		return 0
	}
	cli := NewDocker(fd.sock)
	code, err := cli.Exec(context.Background(), "worker1", []string{"true"})
	require.NoError(t, err)
	assert.Equal(t, 0, code)
	code, err = cli.Exec(context.Background(), "worker1", []string{"false"})
	require.NoError(t, err)
	assert.Equal(t, 3, code)
	assert.Equal(t, []string{"EXEC worker1 true", "EXEC worker1 false"}, fd.log())
}

func TestDockerExecWaitsWhileTheCommandRuns(t *testing.T) {
	fd := newFakeDocker(t)
	fd.runningPolls = 3
	start := time.Now()
	code, err := NewDocker(fd.sock).Exec(context.Background(), "w", []string{"sleep"})
	require.NoError(t, err)
	assert.Equal(t, 0, code)
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "it polled until Running was false")
}

func TestDockerExecErrors(t *testing.T) {
	fd := newFakeDocker(t)
	fd.createCode = 404
	_, err := NewDocker(fd.sock).Exec(context.Background(), "ghost", []string{"x"})
	assert.Error(t, err)

	fd.createCode = 201
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	fd.runningPolls = 1 << 20
	_, err = NewDocker(fd.sock).Exec(ctx, "w", []string{"never-ends"})
	assert.Error(t, err, "a cancelled context ends the wait")

	_, err = NewDocker(filepath.Join(t.TempDir(), "nope.sock")).Exec(context.Background(), "w", []string{"x"})
	assert.Error(t, err)
}

// --- demo endpoints ---

func (f *fixture) demoPost(path string, body any, header bool) (int, map[string]any) {
	f.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, f.ts.URL+path, rd)
	require.NoError(f.t, err)
	if header {
		req.Header.Set("X-Requested-With", "minidfs-ui")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(f.t, err)
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out) //nolint:errcheck
	return resp.StatusCode, out
}

func demoFixture(t *testing.T) (*fixture, *fakeDocker) {
	t.Helper()
	fd := newFakeDocker(t)
	f := newFixture(t)
	f.build(NewDocker(fd.sock))
	return f, fd
}

func lastDemoEvent(f *fixture) *events.Event {
	evs := f.bus.Since(0, 0)
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == "demo.action" {
			return &evs[i]
		}
	}
	return nil
}

func TestDemoEndpointsAreOffUnlessDemoModeIsOn(t *testing.T) {
	f := newFixture(t)
	for _, p := range []string{"/ui/api/demo/worker/worker1/stop", "/ui/api/demo/corrupt", "/ui/api/demo/scrub/worker1", "/ui/api/demo/reconcile"} {
		code, out := f.demoPost(p, map[string]string{}, true)
		assert.Equal(t, http.StatusForbidden, code, p)
		assert.Contains(t, fmt.Sprint(out["error"]), "DEMO_MODE", p)
	}
	assert.Empty(t, f.bus.Since(0, 0))
}

func TestDemoEndpointsRequireTheAntiCSRFHeader(t *testing.T) {
	f, fd := demoFixture(t)
	for _, p := range []string{"/ui/api/demo/worker/worker1/stop", "/ui/api/demo/corrupt", "/ui/api/demo/scrub/worker1", "/ui/api/demo/reconcile"} {
		code, _ := f.demoPost(p, map[string]string{"worker": "worker1", "chunk_id": sha("x")}, false)
		assert.Equal(t, http.StatusForbidden, code, p+" without the header")
	}
	req, _ := http.NewRequest(http.MethodPost, f.ts.URL+"/ui/api/demo/worker/worker1/stop", nil)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, "the wrong value is no better than none")
	assert.Empty(t, fd.log(), "a rejected request never reaches Docker")
}

func TestDemoEndpointsAreNotReachableWithGET(t *testing.T) {
	f, _ := demoFixture(t)
	for _, p := range []string{"/ui/api/demo/worker/worker1/stop", "/ui/api/demo/corrupt", "/ui/api/demo/scrub/worker1", "/ui/api/demo/reconcile"} {
		code, _, _ := f.get(p)
		assert.NotEqual(t, http.StatusOK, code, p)
	}
}

func TestStopAndStartWorker(t *testing.T) {
	f, fd := demoFixture(t)
	code, out := f.demoPost("/ui/api/demo/worker/worker2/stop", nil, true)
	require.Equal(t, http.StatusOK, code, out)
	assert.Equal(t, true, out["ok"])
	code, _ = f.demoPost("/ui/api/demo/worker/worker2/start", nil, true)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, []string{"POST /containers/worker2/stop?t=1", "POST /containers/worker2/start"}, fd.log())
	ev := lastDemoEvent(f)
	require.NotNil(t, ev)
	assert.Equal(t, "start", ev.Fields["action"])
	assert.Equal(t, "worker2", ev.Fields["worker"])
}

func TestWorkerActionValidation(t *testing.T) {
	f, fd := demoFixture(t)
	code, _ := f.demoPost("/ui/api/demo/worker/worker1/explode", nil, true)
	assert.Equal(t, http.StatusBadRequest, code, "only stop and start exist")
	code, _ = f.demoPost("/ui/api/demo/worker/not-registered/stop", nil, true)
	assert.Equal(t, http.StatusNotFound, code, "only workers the controller knows can be touched")
	for _, bad := range []string{"-rm", ".hidden/..", "a;b", "a$(id)", "%20", strings.Repeat("a", 70)} {
		code, _ = f.demoPost("/ui/api/demo/worker/"+bad+"/stop", nil, true)
		assert.Contains(t, []int{http.StatusBadRequest, http.StatusNotFound}, code, bad)
	}
	assert.Empty(t, fd.log(), "nothing invalid reaches Docker")
	assert.Nil(t, lastDemoEvent(f))
}

func TestDockerFailureBecomesA502(t *testing.T) {
	f, fd := demoFixture(t)
	fd.stopCode = 500
	code, out := f.demoPost("/ui/api/demo/worker/worker1/stop", nil, true)
	assert.Equal(t, http.StatusBadGateway, code)
	assert.Contains(t, fmt.Sprint(out["error"]), "boom")
	assert.Nil(t, lastDemoEvent(f), "a failed action is not announced as done")

	f.build(NewDocker("/nonexistent/docker.sock"))
	code, _ = f.demoPost("/ui/api/demo/worker/worker1/stop", nil, true)
	assert.Equal(t, http.StatusBadGateway, code)
}

func TestCorruptRunsTheExactCommandsWithoutAShell(t *testing.T) {
	f, fd := demoFixture(t)
	id := sha("victim")
	code, out := f.demoPost("/ui/api/demo/corrupt", map[string]string{"worker": "worker3", "chunk_id": id}, true)
	require.Equal(t, http.StatusOK, code, out)
	path := "/data/chunks/" + id + ".bin"
	assert.Equal(t, []string{
		"EXEC worker3 test -f " + path,
		"EXEC worker3 dd if=/dev/urandom of=" + path + " bs=16 count=1 seek=2 conv=notrunc",
	}, fd.log())
	for _, cmd := range fd.execs {
		assert.NotContains(t, []string{"sh", "bash", "/bin/sh"}, cmd[0], "no shell, so nothing in the request can be interpreted")
	}
	ev := lastDemoEvent(f)
	require.NotNil(t, ev)
	assert.Equal(t, "corrupt", ev.Fields["action"])
	assert.Equal(t, id, ev.Fields["chunk_id"])
}

func TestCorruptValidatesItsInput(t *testing.T) {
	f, fd := demoFixture(t)
	good := sha("x")
	cases := []struct {
		name string
		body any
		want int
	}{
		{"short id", map[string]string{"worker": "worker1", "chunk_id": "abc"}, 400},
		{"upper case id", map[string]string{"worker": "worker1", "chunk_id": strings.ToUpper(good)}, 400},
		{"path traversal", map[string]string{"worker": "worker1", "chunk_id": "../../etc/passwd"}, 400},
		{"id with suffix", map[string]string{"worker": "worker1", "chunk_id": good + "/../x"}, 400},
		{"empty id", map[string]string{"worker": "worker1", "chunk_id": ""}, 400},
		{"no worker", map[string]string{"chunk_id": good}, 400},
		{"unknown worker", map[string]string{"worker": "stranger", "chunk_id": good}, 404},
		{"worker with shell chars", map[string]string{"worker": "w1;rm -rf /", "chunk_id": good}, 400},
	}
	for _, c := range cases {
		code, _ := f.demoPost("/ui/api/demo/corrupt", c.body, true)
		assert.Equal(t, c.want, code, c.name)
	}
	req, _ := http.NewRequest(http.MethodPost, f.ts.URL+"/ui/api/demo/corrupt", strings.NewReader("{not json"))
	req.Header.Set("X-Requested-With", "minidfs-ui")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, fd.log(), "no invalid request reaches Docker")
}

func TestCorruptRefusesToCreateAFileThatDoesNotExist(t *testing.T) {
	f, fd := demoFixture(t)
	fd.exitFor = func(cmd []string) int {
		if cmd[0] == "test" {
			return 1
		}
		return 0
	}
	code, out := f.demoPost("/ui/api/demo/corrupt", map[string]string{"worker": "worker1", "chunk_id": sha("absent")}, true)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Contains(t, fmt.Sprint(out["error"]), "does not hold")
	assert.Len(t, fd.log(), 1, "dd must not run: with conv=notrunc it would conjure a bogus chunk file")
	assert.Nil(t, lastDemoEvent(f))
}

func TestCorruptReportsAFailingDd(t *testing.T) {
	f, fd := demoFixture(t)
	fd.exitFor = func(cmd []string) int {
		if cmd[0] == "dd" {
			return 1
		}
		return 0
	}
	code, out := f.demoPost("/ui/api/demo/corrupt", map[string]string{"worker": "worker1", "chunk_id": sha("x")}, true)
	assert.Equal(t, http.StatusBadGateway, code)
	assert.Contains(t, fmt.Sprint(out["error"]), "dd exited with 1")
	assert.Nil(t, lastDemoEvent(f))
}

func TestScrubIsProxiedToTheWorker(t *testing.T) {
	f, _ := demoFixture(t)
	code, out := f.demoPost("/ui/api/demo/scrub/worker2", nil, true)
	require.Equal(t, http.StatusOK, code, out)
	assert.EqualValues(t, 1, f.workers["worker2"].runs.Load())
	assert.EqualValues(t, 0, f.workers["worker1"].runs.Load(), "only the chosen worker is scrubbed")
	ev := lastDemoEvent(f)
	require.NotNil(t, ev)
	assert.Equal(t, "scrub", ev.Fields["action"])
}

func TestScrubFailures(t *testing.T) {
	f, _ := demoFixture(t)
	f.workers["worker1"].runCode.Store(http.StatusInternalServerError)
	code, _ := f.demoPost("/ui/api/demo/scrub/worker1", nil, true)
	assert.Equal(t, http.StatusBadGateway, code)
	f.workers["worker2"].ts.Close()
	code, _ = f.demoPost("/ui/api/demo/scrub/worker2", nil, true)
	assert.Equal(t, http.StatusBadGateway, code)
	code, _ = f.demoPost("/ui/api/demo/scrub/stranger", nil, true)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Nil(t, lastDemoEvent(f))
}

func TestReconcileTriggersAPass(t *testing.T) {
	f, _ := demoFixture(t)
	code, out := f.demoPost("/ui/api/demo/reconcile", nil, true)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, true, out["queued"])
	assert.EqualValues(t, 1, f.recon.triggers.Load())
	f.recon.queued = false
	_, out = f.demoPost("/ui/api/demo/reconcile", nil, true)
	assert.Equal(t, false, out["queued"], "a pass already waiting is reported, not hidden")
}

func TestReconcileWithoutAReconciler(t *testing.T) {
	fd := newFakeDocker(t)
	f := newFixture(t)
	f.srv = New(Deps{Meta: f.store, Workers: f.pool, Ring: f.ring, Health: f.health, Bloom: f.bloom, Bus: f.bus, Demo: NewDocker(fd.sock)})
	ts := httptest.NewServer(f.srv.Handler())
	defer ts.Close()
	f.ts = ts
	code, _ := f.demoPost("/ui/api/demo/reconcile", nil, true)
	assert.Equal(t, http.StatusServiceUnavailable, code)
}

// --- the demo flag in the state ---

func TestStateReportsDemoAvailability(t *testing.T) {
	f, fd := demoFixture(t)
	st := f.state()
	assert.True(t, st.Demo.Enabled)
	assert.True(t, st.Demo.Available)

	fd.mu.Lock()
	fd.pingCode = 500
	fd.mu.Unlock()
	f.srv.demoAt = time.Time{}
	st = f.state()
	assert.True(t, st.Demo.Enabled)
	assert.False(t, st.Demo.Available, "enabled but the Docker daemon is not answering")
}

func TestDockerAvailabilityIsNotPolledOnEveryStateRead(t *testing.T) {
	f, fd := demoFixture(t)
	for i := 0; i < 10; i++ {
		f.state()
	}
	fd.mu.Lock()
	defer fd.mu.Unlock()
	assert.Equal(t, 1, fd.pings, "ten state reads inside the cache window ping Docker once")
}
