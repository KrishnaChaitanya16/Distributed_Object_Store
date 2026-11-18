package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// Docker is a minimal client for the Docker Engine API over its unix socket; it exists so the demo can stop workers and damage chunks on cue.
type Docker struct {
	hc *http.Client
}

// NewDocker connects to the Docker socket at path; nothing is contacted until a method is called.
func NewDocker(socket string) *Docker {
	return &Docker{hc: &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}}
}

func (d *Docker) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, data, nil
}

// Ping reports whether the Docker daemon answers.
func (d *Docker) Ping(ctx context.Context) error {
	st, _, err := d.do(ctx, http.MethodGet, "/_ping", nil)
	if err != nil {
		return err
	}
	if st != http.StatusOK {
		return fmt.Errorf("docker ping: status %d", st)
	}
	return nil
}

// Stop stops a container after a one second grace period; stopping one that is already stopped succeeds.
func (d *Docker) Stop(ctx context.Context, name string) error {
	return d.lifecycle(ctx, name, "stop?t=1")
}

// Start starts a container; starting one that is already running succeeds.
func (d *Docker) Start(ctx context.Context, name string) error {
	return d.lifecycle(ctx, name, "start")
}

func (d *Docker) lifecycle(ctx context.Context, name, action string) error {
	st, body, err := d.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(name)+"/"+action, nil)
	if err != nil {
		return err
	}
	if st == http.StatusNoContent || st == http.StatusNotModified {
		return nil
	}
	return fmt.Errorf("docker %s %s: status %d: %s", action, name, st, bytes.TrimSpace(body))
}

// Exec runs cmd inside a running container without a shell and returns its exit code.
func (d *Docker) Exec(ctx context.Context, name string, cmd []string) (int, error) {
	st, body, err := d.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(name)+"/exec", map[string]any{"Cmd": cmd, "AttachStdout": false, "AttachStderr": false})
	if err != nil {
		return -1, err
	}
	if st != http.StatusCreated {
		return -1, fmt.Errorf("docker exec create: status %d: %s", st, bytes.TrimSpace(body))
	}
	var created struct{ Id string }
	if err := json.Unmarshal(body, &created); err != nil || created.Id == "" {
		return -1, errors.New("docker exec create: no exec id")
	}
	if st, body, err = d.do(ctx, http.MethodPost, "/exec/"+created.Id+"/start", map[string]any{"Detach": true, "Tty": false}); err != nil {
		return -1, err
	} else if st != http.StatusOK {
		return -1, fmt.Errorf("docker exec start: status %d: %s", st, bytes.TrimSpace(body))
	}
	deadline := time.Now().Add(8 * time.Second)
	for {
		st, body, err = d.do(ctx, http.MethodGet, "/exec/"+created.Id+"/json", nil)
		if err != nil {
			return -1, err
		}
		var info struct {
			Running  bool
			ExitCode int
		}
		if st == http.StatusOK && json.Unmarshal(body, &info) == nil && !info.Running {
			return info.ExitCode, nil
		}
		if time.Now().After(deadline) {
			return -1, errors.New("docker exec did not finish in time")
		}
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

var (
	chunkIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	workerPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
)

// demoHeader must be sent with every demo request; browsers will not add a custom header to a cross-site request, which keeps other web pages from driving the demo.
const demoHeader = "X-Requested-With"
const demoHeaderValue = "minidfs-ui"

func (s *Server) demoStatus(ctx context.Context) demoView {
	if s.d.Demo == nil {
		return demoView{}
	}
	s.demoMu.Lock()
	defer s.demoMu.Unlock()
	if time.Since(s.demoAt) < 5*time.Second {
		return s.demoVal
	}
	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	s.demoVal = demoView{Enabled: true, Available: s.d.Demo.Ping(pctx) == nil}
	s.demoAt = time.Now()
	return s.demoVal
}

// demoGuard rejects the request unless demo controls are on and the request carries the CSRF header.
func (s *Server) demoGuard(w http.ResponseWriter, r *http.Request) bool {
	if s.d.Demo == nil {
		writeError(w, http.StatusForbidden, "demo controls are off: start the controller with DEMO_MODE=true and the Docker socket mounted")
		return false
	}
	if r.Header.Get(demoHeader) != demoHeaderValue {
		writeError(w, http.StatusForbidden, "missing "+demoHeader+" header")
		return false
	}
	return true
}

// registeredWorker returns the worker record for id, or writes a 404 and returns nil; only workers the controller knows can be touched.
func (s *Server) registeredWorker(w http.ResponseWriter, r *http.Request, id string) *models.WorkerRecord {
	if !workerPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid worker id")
		return nil
	}
	rec, err := s.d.Meta.GetWorker(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "unknown worker "+id)
		return nil
	}
	return rec
}

func (s *Server) handleDemoWorker(w http.ResponseWriter, r *http.Request) {
	if !s.demoGuard(w, r) {
		return
	}
	id, action := r.PathValue("id"), r.PathValue("action")
	if s.registeredWorker(w, r, id) == nil {
		return
	}
	var err error
	switch action {
	case "stop":
		err = s.d.Demo.Stop(r.Context(), id)
	case "start":
		err = s.d.Demo.Start(r.Context(), id)
	default:
		writeError(w, http.StatusBadRequest, "action must be stop or start")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.d.Bus.Emit("demo.action", "demo control used: "+action+" "+id, "action", action, "worker", id)
	s.inv.forget()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "worker": id, "action": action})
}

func (s *Server) handleDemoCorrupt(w http.ResponseWriter, r *http.Request) {
	if !s.demoGuard(w, r) {
		return
	}
	var req struct {
		Worker  string `json:"worker"`
		ChunkID string `json:"chunk_id"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "body must be JSON {worker, chunk_id}")
		return
	}
	if !chunkIDPattern.MatchString(req.ChunkID) {
		writeError(w, http.StatusBadRequest, "chunk_id must be 64 lowercase hex characters")
		return
	}
	if s.registeredWorker(w, r, req.Worker) == nil {
		return
	}
	path := "/data/chunks/" + req.ChunkID + ".bin"
	code, err := s.d.Demo.Exec(r.Context(), req.Worker, []string{"test", "-f", path})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if code != 0 {
		writeError(w, http.StatusNotFound, "that worker does not hold this chunk")
		return
	}
	code, err = s.d.Demo.Exec(r.Context(), req.Worker, []string{"dd", "if=/dev/urandom", "of=" + path, "bs=16", "count=1", "seek=2", "conv=notrunc"})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if code != 0 {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("dd exited with %d", code))
		return
	}
	s.d.Bus.Emit("demo.action", "demo control used: corrupted a copy on disk", "action", "corrupt", "worker", req.Worker, "chunk_id", req.ChunkID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDemoScrub(w http.ResponseWriter, r *http.Request) {
	if !s.demoGuard(w, r) {
		return
	}
	id := r.PathValue("id")
	rec := s.registeredWorker(w, r, id)
	if rec == nil {
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, fmt.Sprintf("http://%s:%d/internal/scrubber/run", rec.Address, rec.Port), nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp, err := s.d.HTTP.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16)) //nolint:errcheck
	if resp.StatusCode != http.StatusAccepted {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("worker answered %d", resp.StatusCode))
		return
	}
	s.d.Bus.Emit("demo.action", "demo control used: scrub started on "+id, "action", "scrub", "worker", id)
	s.inv.forget()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDemoReconcile(w http.ResponseWriter, r *http.Request) {
	if !s.demoGuard(w, r) {
		return
	}
	if s.d.Recon == nil {
		writeError(w, http.StatusServiceUnavailable, "reconciler is not running")
		return
	}
	queued := s.d.Recon.Trigger()
	s.d.Bus.Emit("demo.action", "demo control used: reconciler pass requested", "action", "reconcile")
	s.inv.forget()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "queued": queued})
}
