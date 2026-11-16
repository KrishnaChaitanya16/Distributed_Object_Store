package scrubber

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// fakeStore scripts every answer the scrubber can get and records how it was called.
type fakeStore struct {
	mu          sync.Mutex
	ids         []string
	listErr     error
	verify      map[string]error
	corrupt     map[string]bool
	quarErr     map[string]error
	quarNotMove map[string]bool
	verifyHook  func(id string)
	verified    []string
	quarCalls   []string
	active      atomic.Int32
	maxActive   atomic.Int32
	listCalls   atomic.Int32
}

func newFake(ids ...string) *fakeStore {
	return &fakeStore{ids: ids, verify: map[string]error{}, corrupt: map[string]bool{}, quarErr: map[string]error{}, quarNotMove: map[string]bool{}}
}

func (f *fakeStore) List(ctx context.Context) ([]string, error) {
	f.listCalls.Add(1)
	n := f.active.Add(1)
	defer f.active.Add(-1)
	for {
		m := f.maxActive.Load()
		if n <= m || f.maxActive.CompareAndSwap(m, n) {
			break
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]string(nil), f.ids...), nil
}

func (f *fakeStore) VerifyCRC(ctx context.Context, id string) (bool, error) {
	if f.verifyHook != nil {
		f.verifyHook(id)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verified = append(f.verified, id)
	if err := f.verify[id]; err != nil {
		return false, err
	}
	return !f.corrupt[id], nil
}

func (f *fakeStore) Quarantine(ctx context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quarCalls = append(f.quarCalls, id)
	if err := f.quarErr[id]; err != nil {
		return false, err
	}
	if f.quarNotMove[id] {
		return false, nil
	}
	return true, nil
}

func (f *fakeStore) snapshot() (verified, quar []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.verified...), append([]string(nil), f.quarCalls...)
}

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *logSink) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if bytes.Contains([]byte(s), []byte(sub)) {
			n++
		}
	}
	return n
}

func mustNew(t *testing.T, st Store, cfg Config) (*Scrubber, *logSink) {
	t.Helper()
	logs := &logSink{}
	if cfg.Interval == 0 {
		cfg.Interval = time.Hour
	}
	cfg.Logf = logs.logf
	s, err := New(st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s, logs
}

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		sum := sha256.Sum256([]byte{byte(i), byte(i >> 8)})
		out[i] = hex.EncodeToString(sum[:])
	}
	return out
}

// startRun runs the scrubber in the background and returns a stop function that waits for Run to return.
func startRun(t *testing.T, s *Scrubber) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestNewValidation(t *testing.T) {
	st := newFake()
	cases := []struct {
		name  string
		store Store
		cfg   Config
	}{
		{"nil store", nil, Config{Interval: time.Second}},
		{"zero interval", st, Config{}},
		{"negative interval", st, Config{Interval: -time.Second}},
		{"negative pause", st, Config{Interval: time.Second, Pause: -time.Millisecond}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.store, c.cfg); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	s, err := New(st, Config{Interval: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if s.cfg.Logf == nil {
		t.Fatal("Logf must default to log.Printf")
	}
	got := s.Status()
	if got.IntervalSeconds != 30 || got.Passes != 0 || got.Running || got.LastPassStarted != nil || got.Recent == nil {
		t.Fatalf("initial status %+v", got)
	}
}

func TestStatusJSONShape(t *testing.T) {
	s, _ := mustNew(t, newFake(), Config{Interval: 2 * time.Second})
	b, _ := json.Marshal(s.Status())
	var m map[string]any
	json.Unmarshal(b, &m)
	for _, k := range []string{"running", "interval_seconds", "passes", "chunks_scanned", "corrupt_quarantined", "errors", "last_pass_duration_ms", "last_pass_scanned", "last_pass_corrupt", "recent_corruption"} {
		if _, ok := m[k]; !ok {
			t.Errorf("status JSON missing %q: %s", k, b)
		}
	}
	if _, ok := m["last_pass_started"]; ok {
		t.Error("last_pass_started must be omitted before the first pass")
	}
	if string(mustJSON(m["recent_corruption"])) != "[]" {
		t.Errorf("recent_corruption must be [] not null: %s", b)
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func TestPassOverHealthyChunks(t *testing.T) {
	all := ids(10)
	st := newFake(all...)
	called := 0
	s, _ := mustNew(t, st, Config{OnCorrupt: func(string) { called++ }})
	s.pass(context.Background())
	verified, quar := st.snapshot()
	if len(verified) != 10 || len(quar) != 0 || called != 0 {
		t.Fatalf("verified=%d quarantined=%d callbacks=%d", len(verified), len(quar), called)
	}
	got := s.Status()
	if got.Passes != 1 || got.ChunksScanned != 10 || got.LastPassScanned != 10 || got.CorruptQuarantined != 0 || got.Errors != 0 || got.Running || got.LastPassStarted == nil {
		t.Fatalf("status %+v", got)
	}
}

func TestPassOverEmptyStore(t *testing.T) {
	s, _ := mustNew(t, newFake(), Config{})
	s.pass(context.Background())
	if got := s.Status(); got.Passes != 1 || got.ChunksScanned != 0 || got.Errors != 0 {
		t.Fatalf("status %+v", got)
	}
}

func TestPassQuarantinesOnlyCorruptChunks(t *testing.T) {
	all := ids(6)
	st := newFake(all...)
	st.corrupt[all[1]] = true
	st.corrupt[all[4]] = true
	var got []string
	s, logs := mustNew(t, st, Config{OnCorrupt: func(id string) { got = append(got, id) }})
	s.pass(context.Background())
	_, quar := st.snapshot()
	if len(quar) != 2 || quar[0] != all[1] || quar[1] != all[4] {
		t.Fatalf("quarantine calls %v", quar)
	}
	if len(got) != 2 || got[0] != all[1] || got[1] != all[4] {
		t.Fatalf("callbacks %v", got)
	}
	st2 := s.Status()
	if st2.CorruptQuarantined != 2 || st2.LastPassCorrupt != 2 || st2.ChunksScanned != 6 || len(st2.Recent) != 2 || st2.Recent[0].ChunkID != all[4] {
		t.Fatalf("status %+v", st2)
	}
	if logs.count("quarantined") != 2 {
		t.Fatalf("expected a log line per quarantined chunk: %v", logs.lines)
	}
}

func TestLastPassCountersResetEachPass(t *testing.T) {
	all := ids(3)
	st := newFake(all...)
	st.corrupt[all[0]] = true
	s, _ := mustNew(t, st, Config{})
	s.pass(context.Background())
	st.mu.Lock()
	st.corrupt = map[string]bool{}
	st.mu.Unlock()
	s.pass(context.Background())
	got := s.Status()
	if got.LastPassCorrupt != 0 || got.LastPassScanned != 3 || got.CorruptQuarantined != 1 || got.ChunksScanned != 6 || got.Passes != 2 {
		t.Fatalf("status %+v", got)
	}
}

func TestListErrorIsRecordedAndNextPassRecovers(t *testing.T) {
	st := newFake(ids(2)...)
	st.listErr = errors.New("disk on fire")
	s, logs := mustNew(t, st, Config{})
	s.pass(context.Background())
	got := s.Status()
	if got.Errors != 1 || got.Passes != 1 || got.LastError == "" || logs.count("disk on fire") != 1 {
		t.Fatalf("status %+v logs %v", got, logs.lines)
	}
	st.mu.Lock()
	st.listErr = nil
	st.mu.Unlock()
	s.pass(context.Background())
	if got := s.Status(); got.ChunksScanned != 2 || got.Passes != 2 {
		t.Fatalf("status after recovery %+v", got)
	}
}

func TestVerifyErrorSkipsChunkAndContinues(t *testing.T) {
	all := ids(4)
	st := newFake(all...)
	st.verify[all[1]] = errors.New("EIO")
	st.corrupt[all[2]] = true
	s, _ := mustNew(t, st, Config{})
	s.pass(context.Background())
	_, quar := st.snapshot()
	if len(quar) != 1 || quar[0] != all[2] {
		t.Fatalf("an I/O error must not quarantine; corrupt chunk after it must still be found: %v", quar)
	}
	got := s.Status()
	if got.Errors != 1 || got.ChunksScanned != 3 || got.CorruptQuarantined != 1 {
		t.Fatalf("status %+v", got)
	}
}

func TestChunkDeletedDuringPassIsSkippedQuietly(t *testing.T) {
	all := ids(3)
	st := newFake(all...)
	st.verify[all[0]] = fmt.Errorf("open: %w", models.ErrNotFound)
	s, _ := mustNew(t, st, Config{})
	s.pass(context.Background())
	got := s.Status()
	if got.Errors != 0 || got.ChunksScanned != 2 {
		t.Fatalf("a chunk deleted mid-pass is normal, not an error: %+v", got)
	}
}

func TestQuarantineOutcomes(t *testing.T) {
	cases := []struct {
		name        string
		quarErr     error
		notMoved    bool
		wantCorrupt int64
		wantErrors  int64
		wantCB      int
		wantLog     string
	}{
		{name: "moved", wantCorrupt: 1, wantCB: 1, wantLog: "quarantined"},
		{name: "repaired before quarantine", notMoved: true, wantLog: "replaced by a good copy"},
		{name: "deleted before quarantine", quarErr: models.ErrNotFound, wantLog: "deleted before quarantine"},
		{name: "rename failed", quarErr: errors.New("read-only fs"), wantErrors: 1, wantLog: "read-only fs"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			all := ids(1)
			st := newFake(all...)
			st.corrupt[all[0]] = true
			if c.quarErr != nil {
				st.quarErr[all[0]] = c.quarErr
			}
			st.quarNotMove[all[0]] = c.notMoved
			cb := 0
			s, logs := mustNew(t, st, Config{OnCorrupt: func(string) { cb++ }})
			s.pass(context.Background())
			got := s.Status()
			if got.CorruptQuarantined != c.wantCorrupt || got.Errors != c.wantErrors || cb != c.wantCB {
				t.Fatalf("status %+v callbacks %d", got, cb)
			}
			if logs.count(c.wantLog) != 1 {
				t.Fatalf("missing log %q in %v", c.wantLog, logs.lines)
			}
		})
	}
}

func TestMovedButFsyncFailedCountsBoth(t *testing.T) {
	st := &movedWithErr{fakeStore: newFake(ids(1)...)}
	st.corrupt[st.ids[0]] = true
	cb := 0
	s, _ := mustNew(t, st, Config{OnCorrupt: func(string) { cb++ }})
	s.pass(context.Background())
	got := s.Status()
	if got.CorruptQuarantined != 1 || got.Errors != 1 || cb != 1 {
		t.Fatalf("status %+v callbacks %d", got, cb)
	}
}

type movedWithErr struct{ *fakeStore }

func (m *movedWithErr) Quarantine(context.Context, string) (bool, error) {
	return true, errors.New("fsync quarantine dir: EIO")
}

func TestNilCallbackIsFine(t *testing.T) {
	all := ids(1)
	st := newFake(all...)
	st.corrupt[all[0]] = true
	s, _ := mustNew(t, st, Config{})
	s.pass(context.Background())
	if s.Status().CorruptQuarantined != 1 {
		t.Fatal("corrupt chunk not counted")
	}
}

func TestRecentHistoryIsCappedNewestFirst(t *testing.T) {
	all := ids(maxRecent + 7)
	st := newFake(all...)
	for _, id := range all {
		st.corrupt[id] = true
	}
	s, _ := mustNew(t, st, Config{})
	s.pass(context.Background())
	got := s.Status()
	if len(got.Recent) != maxRecent || got.CorruptQuarantined != int64(len(all)) {
		t.Fatalf("recent=%d corrupt=%d", len(got.Recent), got.CorruptQuarantined)
	}
	if got.Recent[0].ChunkID != all[len(all)-1] || got.Recent[maxRecent-1].ChunkID != all[len(all)-maxRecent] {
		t.Fatal("recent history must keep the newest events, newest first")
	}
}

func TestStatusIsACopy(t *testing.T) {
	all := ids(1)
	st := newFake(all...)
	st.corrupt[all[0]] = true
	s, _ := mustNew(t, st, Config{})
	s.pass(context.Background())
	got := s.Status()
	got.Recent[0].ChunkID = "mutated"
	*got.LastPassStarted = time.Time{}
	again := s.Status()
	if again.Recent[0].ChunkID != all[0] || again.LastPassStarted.IsZero() {
		t.Fatal("callers must not be able to mutate the scrubber's state through Status")
	}
}

func TestPauseThrottlesBetweenChunks(t *testing.T) {
	st := newFake(ids(5)...)
	s, _ := mustNew(t, st, Config{Pause: 30 * time.Millisecond})
	start := time.Now()
	s.pass(context.Background())
	if took := time.Since(start); took < 4*30*time.Millisecond {
		t.Fatalf("5 chunks with a 30ms pause took only %s", took)
	}
}

func TestZeroPauseIsUnthrottled(t *testing.T) {
	st := newFake(ids(2000)...)
	s, _ := mustNew(t, st, Config{})
	start := time.Now()
	s.pass(context.Background())
	if took := time.Since(start); took > time.Second {
		t.Fatalf("unthrottled pass over 2000 fake chunks took %s", took)
	}
	if s.Status().ChunksScanned != 2000 {
		t.Fatal("not every chunk scanned")
	}
}

func TestCancelDuringPauseStopsPromptly(t *testing.T) {
	st := newFake(ids(100)...)
	s, _ := mustNew(t, st, Config{Pause: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.pass(ctx); close(done) }()
	waitFor(t, "first chunk verified", func() bool { v, _ := st.snapshot(); return len(v) == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pass ignored cancellation during its pause")
	}
	got := s.Status()
	if got.Passes != 0 || got.Running || got.Errors != 0 {
		t.Fatalf("a cancelled pass is not completed and not an error: %+v", got)
	}
}

func TestCancelDuringVerifyIsNotAnError(t *testing.T) {
	all := ids(3)
	st := newFake(all...)
	ctx, cancel := context.WithCancel(context.Background())
	st.verifyHook = func(id string) {
		if id == all[1] {
			cancel()
		}
	}
	s, _ := mustNew(t, st, Config{})
	s.pass(ctx)
	got := s.Status()
	if got.Errors != 0 || got.Passes != 0 || got.ChunksScanned != 1 {
		t.Fatalf("status %+v", got)
	}
}

func TestCancelledBeforeListIsNotAnError(t *testing.T) {
	st := newFake(ids(3)...)
	st.listErr = context.Canceled
	s, _ := mustNew(t, st, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.pass(ctx)
	if got := s.Status(); got.Errors != 0 || got.Passes != 0 {
		t.Fatalf("status %+v", got)
	}
}

func TestRunTicksOnInterval(t *testing.T) {
	st := newFake(ids(2)...)
	s, _ := mustNew(t, st, Config{Interval: 20 * time.Millisecond})
	startRun(t, s)
	waitFor(t, "three passes", func() bool { return s.Status().Passes >= 3 })
}

func TestRunWaitsOneIntervalBeforeFirstPass(t *testing.T) {
	st := newFake(ids(2)...)
	s, _ := mustNew(t, st, Config{Interval: time.Hour})
	startRun(t, s)
	time.Sleep(50 * time.Millisecond)
	if st.listCalls.Load() != 0 {
		t.Fatal("first pass must wait for the interval or a trigger")
	}
}

func TestTriggerRunsPassImmediately(t *testing.T) {
	st := newFake(ids(2)...)
	s, _ := mustNew(t, st, Config{Interval: time.Hour})
	startRun(t, s)
	if !s.Trigger() {
		t.Fatal("first trigger must be queued")
	}
	waitFor(t, "triggered pass", func() bool { return s.Status().Passes == 1 })
}

func TestTriggerBeforeRunIsKept(t *testing.T) {
	st := newFake(ids(1)...)
	s, _ := mustNew(t, st, Config{Interval: time.Hour})
	s.Trigger()
	startRun(t, s)
	waitFor(t, "pass queued before Run", func() bool { return s.Status().Passes == 1 })
}

func TestTriggersCoalesceWhileAPassRuns(t *testing.T) {
	all := ids(3)
	st := newFake(all...)
	release := make(chan struct{})
	var gate sync.Once
	st.verifyHook = func(string) { gate.Do(func() { <-release }) }
	s, _ := mustNew(t, st, Config{Interval: time.Hour})
	startRun(t, s)
	s.Trigger()
	waitFor(t, "pass running", func() bool { return s.Status().Running })
	queued := 0
	for i := 0; i < 10; i++ {
		if s.Trigger() {
			queued++
		}
	}
	if queued != 1 {
		t.Fatalf("while a pass runs exactly one more may be queued, got %d", queued)
	}
	close(release)
	waitFor(t, "two passes", func() bool { return s.Status().Passes == 2 })
	time.Sleep(50 * time.Millisecond)
	if p := s.Status().Passes; p != 2 {
		t.Fatalf("ten triggers during one pass must cause one extra pass, got %d passes", p)
	}
}

func TestPassesNeverOverlap(t *testing.T) {
	st := newFake(ids(20)...)
	s, _ := mustNew(t, st, Config{Interval: time.Millisecond, Pause: time.Millisecond})
	startRun(t, s)
	for i := 0; i < 200; i++ {
		s.Trigger()
		time.Sleep(100 * time.Microsecond)
	}
	waitFor(t, "several passes", func() bool { return s.Status().Passes >= 3 })
	if m := st.maxActive.Load(); m != 1 {
		t.Fatalf("%d passes ran at once", m)
	}
}

func TestRunTwiceIsRejected(t *testing.T) {
	s, _ := mustNew(t, newFake(), Config{})
	startRun(t, s)
	waitFor(t, "Run started", func() bool { return s.started.Load() })
	if err := s.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run returned %v", err)
	}
}

func TestRunStopsMidPassOnCancel(t *testing.T) {
	st := newFake(ids(1000)...)
	s, _ := mustNew(t, st, Config{Interval: time.Hour, Pause: 5 * time.Millisecond})
	stop := startRun(t, s)
	s.Trigger()
	waitFor(t, "pass running", func() bool { return s.Status().Running })
	start := time.Now()
	stop()
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("shutdown waited %s for the pass", took)
	}
	if got := s.Status(); got.Running || got.Passes != 0 {
		t.Fatalf("status after cancel %+v", got)
	}
}

func TestStatusIsSafeDuringRun(t *testing.T) {
	all := ids(50)
	st := newFake(all...)
	for i := 0; i < 50; i += 5 {
		st.corrupt[all[i]] = true
	}
	s, _ := mustNew(t, st, Config{Interval: time.Millisecond})
	startRun(t, s)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				_ = s.Status()
				s.Trigger()
			}
		}()
	}
	wg.Wait()
	waitFor(t, "some passes", func() bool { return s.Status().Passes >= 2 })
}

// The tests below drive the scrubber against the real storage package on a real directory.

func realStore(t *testing.T) (*storage.Store, string) {
	t.Helper()
	root := t.TempDir()
	st, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	return st, root
}

func putReal(t *testing.T, st *storage.Store, n int) ([]byte, string) {
	t.Helper()
	b := make([]byte, n)
	rand.Read(b)
	sum := sha256.Sum256(b)
	id := hex.EncodeToString(sum[:])
	if err := st.Write(context.Background(), id, bytes.NewReader(b), int64(n)); err != nil {
		t.Fatal(err)
	}
	return b, id
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
	b[0] ^= 0x01
	f.WriteAt(b, off)
}

func TestRealStoreFindsAndQuarantinesCorruption(t *testing.T) {
	ctx := context.Background()
	st, root := realStore(t)
	var good, bad []string
	for i := 0; i < 8; i++ {
		_, id := putReal(t, st, 1000+i*50000)
		if i%3 == 0 {
			flip(t, filepath.Join(root, "chunks", id+".bin"), int64(i))
			bad = append(bad, id)
		} else {
			good = append(good, id)
		}
	}
	var reported []string
	s, _ := mustNew(t, st, Config{OnCorrupt: func(id string) { reported = append(reported, id) }})
	s.pass(ctx)

	sort.Strings(bad)
	sort.Strings(reported)
	if fmt.Sprint(reported) != fmt.Sprint(bad) {
		t.Fatalf("reported %v want %v", reported, bad)
	}
	left, _ := st.List(ctx)
	sort.Strings(good)
	if fmt.Sprint(left) != fmt.Sprint(good) {
		t.Fatalf("serving set %v want %v", left, good)
	}
	q, _ := os.ReadDir(filepath.Join(root, "quarantine"))
	if len(q) != len(bad) {
		t.Fatalf("quarantine holds %d files, want %d", len(q), len(bad))
	}
	for _, id := range bad {
		if _, err := st.Read(ctx, id); !errors.Is(err, models.ErrNotFound) {
			t.Fatalf("corrupt chunk still readable: %v", err)
		}
	}
	got := s.Status()
	if got.ChunksScanned != 8 || got.CorruptQuarantined != int64(len(bad)) || got.Errors != 0 {
		t.Fatalf("status %+v", got)
	}

	s.pass(ctx)
	if got := s.Status(); got.CorruptQuarantined != int64(len(bad)) || got.LastPassScanned != len(good) {
		t.Fatalf("second pass must find nothing new: %+v", got)
	}
}

func TestRealStoreRepairedChunkPassesNextScrub(t *testing.T) {
	ctx := context.Background()
	st, root := realStore(t)
	data, id := putReal(t, st, storage.MaxChunkSize)
	flip(t, filepath.Join(root, "chunks", id+".bin"), 12345)
	s, _ := mustNew(t, st, Config{})
	s.pass(ctx)
	if err := st.Write(ctx, id, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	s.pass(ctx)
	got := s.Status()
	if got.CorruptQuarantined != 1 || got.LastPassCorrupt != 0 || got.LastPassScanned != 1 {
		t.Fatalf("status %+v", got)
	}
}

func TestRealStoreJunkFilesAreIgnored(t *testing.T) {
	ctx := context.Background()
	st, root := realStore(t)
	putReal(t, st, 10)
	dir := filepath.Join(root, "chunks")
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(dir, "SHORT.bin"), []byte("x"), 0o644)
	os.Mkdir(filepath.Join(dir, strings.Repeat("e", 64)+".bin"), 0o755)
	s, _ := mustNew(t, st, Config{})
	s.pass(ctx)
	got := s.Status()
	if got.ChunksScanned != 1 || got.Errors != 0 || got.CorruptQuarantined != 0 {
		t.Fatalf("status %+v", got)
	}
	for _, n := range []string{"notes.txt", "SHORT.bin"} {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Fatalf("scrubber must not touch %s", n)
		}
	}
}

func TestRealStoreConcurrentWritesDuringScrub(t *testing.T) {
	ctx := context.Background()
	st, _ := realStore(t)
	for i := 0; i < 20; i++ {
		putReal(t, st, 20000)
	}
	s, _ := mustNew(t, st, Config{Interval: time.Millisecond})
	startRun(t, s)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				_, id := putReal(t, st, 30000)
				if i%2 == 0 {
					st.Delete(ctx, id)
				}
			}
		}()
	}
	wg.Wait()
	waitFor(t, "passes during churn", func() bool { return s.Status().Passes >= 3 })
	if got := s.Status(); got.CorruptQuarantined != 0 || got.Errors != 0 {
		t.Fatalf("healthy churn must never look corrupt: %+v", got)
	}
}
