package web

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

type eventsResp struct {
	Events  []events.Event `json:"events"`
	LastSeq uint64         `json:"last_seq"`
}

func TestEventsEndpointReturnsHistoryOldestFirst(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 5; i++ {
		f.bus.Emit("k", fmt.Sprint(i), "n", i)
	}
	var out eventsResp
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/events", &out))
	require.Len(t, out.Events, 5)
	assert.Equal(t, "1", out.Events[0].Msg)
	assert.Equal(t, "5", out.Events[4].Msg)
	assert.EqualValues(t, 5, out.LastSeq)

	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/events?since=3", &out))
	require.Len(t, out.Events, 2)
	assert.EqualValues(t, 4, out.Events[0].Seq)

	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/events?limit=2", &out))
	require.Len(t, out.Events, 2)
	assert.EqualValues(t, 4, out.Events[0].Seq, "the newest events win when a limit applies")
}

func TestEventsEndpointWhenThereAreNone(t *testing.T) {
	f := newFixture(t)
	_, _, body := f.get("/ui/api/events")
	assert.JSONEq(t, `{"events":[],"last_seq":0}`, string(body), "an empty log is [] so the UI can loop over it")
	_, _, body = f.get("/ui/api/events?since=99")
	assert.JSONEq(t, `{"events":[],"last_seq":0}`, string(body))
}

func TestEventsEndpointValidatesParameters(t *testing.T) {
	f := newFixture(t)
	for _, q := range []string{"since=-1", "since=abc", "since=1.5", "limit=0", "limit=-3", "limit=x"} {
		assert.Equal(t, http.StatusBadRequest, f.getJSON("/ui/api/events?"+q, nil), q)
	}
	for i := 0; i < maxEventLimit+10; i++ {
		f.bus.Emit("k", "x")
	}
	var out eventsResp
	require.Equal(t, http.StatusOK, f.getJSON("/ui/api/events?limit=999999", &out))
	assert.LessOrEqual(t, len(out.Events), maxEventLimit, "a huge limit is capped")
}

// sse reads server-sent events from a stream until the context ends.
type sse struct {
	resp *http.Response
	ch   chan events.Event
	raw  chan string
}

func openStream(t *testing.T, f *fixture, ctx context.Context, path string, hdr map[string]string) *sse {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.ts.URL+path, nil)
	require.NoError(t, err)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	s := &sse{resp: resp, ch: make(chan events.Event, 100), raw: make(chan string, 100)}
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			s.raw <- line
			if strings.HasPrefix(line, "data: ") {
				var ev events.Event
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) == nil {
					s.ch <- ev
				}
			}
		}
	}()
	t.Cleanup(func() { resp.Body.Close() })
	return s
}

func (s *sse) next(t *testing.T) events.Event {
	t.Helper()
	select {
	case ev := <-s.ch:
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("no event arrived on the stream")
		return events.Event{}
	}
}

func (s *sse) expectNone(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case ev := <-s.ch:
		t.Fatalf("unexpected event %+v", ev)
	case <-time.After(d):
	}
}

func TestStreamDeliversNewEventsLive(t *testing.T) {
	f := newFixture(t)
	f.bus.Emit("old", "before the stream opened")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := openStream(t, f, ctx, "/ui/api/events/stream", nil)
	assert.Equal(t, "text/event-stream", s.resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-cache", s.resp.Header.Get("Cache-Control"))
	s.expectNone(t, 150*time.Millisecond)
	f.bus.Emit("upload.start", "live", "name", "x.bin")
	ev := s.next(t)
	assert.Equal(t, "upload.start", ev.Kind)
	assert.Equal(t, "x.bin", ev.Fields["name"])
	f.bus.Emit("a", "1")
	f.bus.Emit("b", "2")
	assert.Equal(t, "a", s.next(t).Kind)
	assert.Equal(t, "b", s.next(t).Kind)
}

func TestStreamSinceReplaysMissedEvents(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 4; i++ {
		f.bus.Emit("k", fmt.Sprint(i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := openStream(t, f, ctx, "/ui/api/events/stream?since=2", nil)
	assert.EqualValues(t, 3, s.next(t).Seq)
	assert.EqualValues(t, 4, s.next(t).Seq)
	s.expectNone(t, 100*time.Millisecond)
}

func TestStreamResumesFromLastEventID(t *testing.T) {
	f := newFixture(t)
	for i := 1; i <= 3; i++ {
		f.bus.Emit("k", fmt.Sprint(i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := openStream(t, f, ctx, "/ui/api/events/stream", map[string]string{"Last-Event-ID": "1"})
	assert.EqualValues(t, 2, s.next(t).Seq, "a reconnecting browser sends Last-Event-ID and misses nothing")
	assert.EqualValues(t, 3, s.next(t).Seq)
}

func TestStreamIgnoresAMalformedLastEventIDButRejectsABadSince(t *testing.T) {
	f := newFixture(t)
	f.bus.Emit("k", "1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := openStream(t, f, ctx, "/ui/api/events/stream", map[string]string{"Last-Event-ID": "garbage"})
	s.expectNone(t, 100*time.Millisecond)
	assert.Equal(t, http.StatusBadRequest, f.getJSON("/ui/api/events/stream?since=nope", nil))
}

func TestStreamEventsCarryTheirSequenceAsTheSSEId(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := openStream(t, f, ctx, "/ui/api/events/stream", nil)
	s.expectNone(t, 50*time.Millisecond)
	f.bus.Emit("k", "x")
	s.next(t)
	var ids []string
drain:
	for {
		select {
		case l := <-s.raw:
			if strings.HasPrefix(l, "id: ") {
				ids = append(ids, l)
			}
		default:
			break drain
		}
	}
	assert.Equal(t, []string{"id: 1"}, ids)
}

func TestStreamSendsKeepAlivePings(t *testing.T) {
	f := newFixture(t)
	old := streamPing
	streamPing = 40 * time.Millisecond
	defer func() { streamPing = old }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := openStream(t, f, ctx, "/ui/api/events/stream", nil)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case l := <-s.raw:
			if l == ": ping" {
				return
			}
		case <-deadline:
			t.Fatal("no keep-alive ping")
		}
	}
}

func TestStreamEndsWhenTheClientGoesAway(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := openStream(t, f, ctx, "/ui/api/events/stream", nil)
	f.bus.Emit("k", "x")
	s.next(t)
	cancel()
	for i := 0; i < 50; i++ {
		f.bus.Emit("k", "after disconnect")
		time.Sleep(5 * time.Millisecond)
	}
	f.ts.Config.SetKeepAlivesEnabled(false)
	done := make(chan struct{})
	go func() { f.ts.CloseClientConnections(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("server kept a connection open for a client that left")
	}
}

func TestManyStreamsEachGetEveryEvent(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var streams []*sse
	for i := 0; i < 8; i++ {
		streams = append(streams, openStream(t, f, ctx, "/ui/api/events/stream", nil))
	}
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 30; i++ {
		f.bus.Emit("k", fmt.Sprint(i))
	}
	for si, s := range streams {
		for want := 1; want <= 30; want++ {
			assert.EqualValues(t, want, s.next(t).Seq, "stream %d", si)
		}
	}
}

// --- observers ---

func TestHealthObserverEmitsOnlyOnChanges(t *testing.T) {
	f := newFixture(t)
	obs := newObserver()
	f.srv.checkHealth(obs)
	assert.Empty(t, f.bus.Since(0, 0), "the first sighting of each worker is silent")
	f.srv.checkHealth(obs)
	assert.Empty(t, f.bus.Since(0, 0), "no change, no event")

	f.health.set("worker2", models.WorkerOffline)
	f.srv.checkHealth(obs)
	f.srv.checkHealth(obs)
	evs := f.bus.Since(0, 0)
	require.Len(t, evs, 1, "one flip, one event, however often it is observed")
	assert.Equal(t, "worker.offline", evs[0].Kind)
	assert.Equal(t, "worker2", evs[0].Fields["worker"])

	f.health.set("worker2", models.WorkerOnline)
	f.srv.checkHealth(obs)
	evs = f.bus.Since(0, 0)
	require.Len(t, evs, 2)
	assert.Equal(t, "worker.online", evs[1].Kind)
}

func TestHealthObserverIgnoresANewWorkerAndUnknownTransitions(t *testing.T) {
	f := newFixture(t)
	obs := newObserver()
	f.srv.checkHealth(obs)
	f.health.set("brand-new", models.WorkerOnline)
	f.srv.checkHealth(obs)
	assert.Empty(t, f.bus.Since(0, 0), "a worker appearing is registration, announced elsewhere")
	f.health.set("worker1", models.WorkerUnknown)
	f.srv.checkHealth(obs)
	assert.Empty(t, f.bus.Since(0, 0), "UNKNOWN is neither up nor down")
}

func TestScrubberObserverReportsEachQuarantineOnce(t *testing.T) {
	f := newFixture(t)
	obs := newObserver()
	ctx := context.Background()
	chunk := sha("damaged")
	f.srv.checkScrubbers(ctx, obs)
	assert.Empty(t, f.bus.Since(0, 0))

	f.workers["worker2"].scrubber.Store(map[string]any{"passes": 4, "recent_corruption": []any{
		map[string]any{"chunk_id": chunk, "at": "2026-11-16T10:00:00Z"},
	}})
	f.srv.checkScrubbers(ctx, obs)
	f.srv.checkScrubbers(ctx, obs)
	evs := f.bus.Since(0, 0)
	require.Len(t, evs, 1, "the same finding is never announced twice")
	assert.Equal(t, "scrub.quarantine", evs[0].Kind)
	assert.Equal(t, "worker2", evs[0].Fields["worker"])
	assert.Equal(t, chunk, evs[0].Fields["chunk_id"])

	f.workers["worker2"].scrubber.Store(map[string]any{"recent_corruption": []any{
		map[string]any{"chunk_id": sha("second"), "at": "2026-11-16T10:00:30Z"},
		map[string]any{"chunk_id": chunk, "at": "2026-11-16T10:00:00Z"},
	}})
	f.srv.checkScrubbers(ctx, obs)
	evs = f.bus.Since(0, 0)
	require.Len(t, evs, 2)
	assert.Equal(t, sha("second"), evs[1].Fields["chunk_id"])
}

func TestScrubberObserverStaysSilentAboutFindingsBeforeItStarted(t *testing.T) {
	f := newFixture(t)
	f.workers["worker3"].scrubber.Store(map[string]any{"recent_corruption": []any{
		map[string]any{"chunk_id": sha("ancient"), "at": "2026-01-01T00:00:00Z"},
	}})
	obs := newObserver()
	f.srv.checkScrubbers(context.Background(), obs)
	assert.Empty(t, f.bus.Since(0, 0), "history from before the controller started is not news")
}

func TestScrubberObserverToleratesMalformedReports(t *testing.T) {
	f := newFixture(t)
	obs := newObserver()
	f.srv.checkScrubbers(context.Background(), obs)
	f.workers["worker1"].scrubber.Store(map[string]any{"recent_corruption": "not a list"})
	f.workers["worker2"].scrubber.Store(map[string]any{"recent_corruption": []any{"junk", 5, map[string]any{"chunk_id": ""}, map[string]any{}}})
	f.workers["worker3"].scrubber.Store(map[string]any{})
	assert.NotPanics(t, func() { f.srv.checkScrubbers(context.Background(), obs) })
	assert.Empty(t, f.bus.Since(0, 0))
}

func TestRunStopsOnCancelAndWatchesHealth(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.srv.Run(ctx); close(done) }()
	time.Sleep(700 * time.Millisecond)
	f.health.set("worker4", models.WorkerOffline)
	require.Eventually(t, func() bool {
		for _, e := range f.bus.Since(0, 0) {
			if e.Kind == "worker.offline" && e.Fields["worker"] == "worker4" {
				return true
			}
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestDiskChangingEventsDropTheCachedInventory(t *testing.T) {
	f := newFixture(t)
	f.srv.inv.ttl = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	f.state()
	n := f.pool.calls.Load()
	f.state()
	require.Equal(t, n, f.pool.calls.Load(), "cached while nothing changes")

	for _, kind := range []string{"gc.pass", "upload.chunk", "upload.start", "download.fetch"} {
		f.bus.Emit(kind, "does not change any disk")
	}
	time.Sleep(150 * time.Millisecond)
	f.state()
	assert.Equal(t, n, f.pool.calls.Load(), "routine events keep the cache")

	f.pool.set("worker1", sha("fresh"))
	f.bus.Emit("upload.commit", "a new chunk reached the disks")
	require.Eventually(t, func() bool {
		w := f.workerView(f.state(), "worker1")
		return len(w.Chunks) == 1
	}, 3*time.Second, 20*time.Millisecond, "right after a commit the UI must see the new chunk, not a stale snapshot")
}

func TestEveryDiskChangingEventKindIsListed(t *testing.T) {
	for _, k := range []string{"upload.commit", "upload.durable", "object.delete", "rebalance.copy", "gc.restore", "gc.excess", "gc.chunk_deleted", "download.repair", "scrub.quarantine", "worker.online", "worker.offline"} {
		assert.True(t, changesDisks[k], k)
	}
	for _, k := range []string{"gc.pass", "upload.chunk", "upload.bloom", "download.fetch", "demo.action"} {
		assert.False(t, changesDisks[k], k)
	}
}
