package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/scrubber"
)

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp := do(t, http.MethodGet, url+"/metrics", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func wantLine(t *testing.T, body, line string) {
	t.Helper()
	for _, l := range strings.Split(body, "\n") {
		if l == line {
			return
		}
	}
	t.Fatalf("metrics output has no line %q", line)
}

func TestMetrics_EndpointExistsOnFreshWorker(t *testing.T) {
	w := newWorker(t, "w1")
	body := scrape(t, w.srv.URL)
	wantLine(t, body, "minidfs_worker_chunks_stored 0")
	wantLine(t, body, "minidfs_worker_chunk_bytes_written_total 0")
}

func TestMetrics_CountsWritesReadsAndBytes(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 4096)
	do(t, http.MethodPut, w.url("/internal/chunks/"+id), data).Body.Close()
	resp := do(t, http.MethodGet, w.url("/internal/chunks/"+id), nil)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(got) != len(data) {
		t.Fatalf("read %d bytes, want %d", len(got), len(data))
	}
	body := scrape(t, w.srv.URL)
	wantLine(t, body, `minidfs_worker_requests_total{code="201",op="put"} 1`)
	wantLine(t, body, `minidfs_worker_requests_total{code="200",op="get"} 1`)
	wantLine(t, body, "minidfs_worker_chunk_bytes_written_total 4096")
	wantLine(t, body, "minidfs_worker_chunks_stored 1")
	if !strings.Contains(body, "minidfs_worker_chunk_bytes_read_total 4") {
		t.Fatalf("read bytes not counted:\n%s", body)
	}
}

func TestMetrics_FailedRequestsKeepTheirStatusAndAddNoBytes(t *testing.T) {
	w := newWorker(t, "w1")
	_, id := chunk(t, 10)
	do(t, http.MethodGet, w.url("/internal/chunks/"+id), nil).Body.Close()
	do(t, http.MethodPut, w.url("/internal/chunks/not-a-valid-id"), []byte("x")).Body.Close()
	body := scrape(t, w.srv.URL)
	wantLine(t, body, `minidfs_worker_requests_total{code="404",op="get"} 1`)
	wantLine(t, body, `minidfs_worker_requests_total{code="400",op="put"} 1`)
	wantLine(t, body, "minidfs_worker_chunk_bytes_written_total 0")
}

func TestMetrics_DeleteLowersStoredGauge(t *testing.T) {
	w := newWorker(t, "w1")
	data, id := chunk(t, 100)
	do(t, http.MethodPut, w.url("/internal/chunks/"+id), data).Body.Close()
	do(t, http.MethodDelete, w.url("/internal/chunks/"+id), nil).Body.Close()
	body := scrape(t, w.srv.URL)
	wantLine(t, body, "minidfs_worker_chunks_stored 0")
	wantLine(t, body, `minidfs_worker_requests_total{code="204",op="delete"} 1`)
}

func TestMetrics_TwoWorkersDoNotShareCounters(t *testing.T) {
	a, b := newWorker(t, "a"), newWorker(t, "b")
	data, id := chunk(t, 64)
	do(t, http.MethodPut, a.url("/internal/chunks/"+id), data).Body.Close()
	wantLine(t, scrape(t, b.srv.URL), "minidfs_worker_chunks_stored 0")
}

type fakeScrubber struct{ st scrubber.Status }

func (f fakeScrubber) Trigger() bool           { return true }
func (f fakeScrubber) Status() scrubber.Status { return f.st }

func TestMetrics_ScrubberAndDiskGauges(t *testing.T) {
	srv := newFakeStoreServer(t)
	body := scrape(t, srv.URL)
	wantLine(t, body, "minidfs_worker_scrubber_passes_total 3")
	wantLine(t, body, "minidfs_worker_scrubber_corrupt_quarantined_total 2")
	wantLine(t, body, "minidfs_worker_disk_free_bytes 500")
	wantLine(t, body, "minidfs_worker_disk_total_bytes 1000")
}

func newFakeStoreServer(t *testing.T) *httptest.Server {
	t.Helper()
	w := newWorker(t, "w1")
	h := New(w.store, "w1", "self:1", http.DefaultClient).
		WithScrubber(fakeScrubber{scrubber.Status{Passes: 3, ChunksScanned: 9, CorruptQuarantined: 2}}).
		WithDiskStats(func() (int64, int64) { return 500, 1000 }).Handler()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestMetrics_NoScrubberMeansNoScrubberMetrics(t *testing.T) {
	w := newWorker(t, "w1")
	if strings.Contains(scrape(t, w.srv.URL), "scrubber") {
		t.Fatal("scrubber metrics present without a scrubber")
	}
}
