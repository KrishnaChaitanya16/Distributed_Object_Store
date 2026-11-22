package api

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// workerMetrics holds one worker's Prometheus metrics in a registry of its own, so test servers never collide on the default one.
type workerMetrics struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	written  prometheus.Counter
	read     prometheus.Counter
}

func newWorkerMetrics(s *Server) *workerMetrics {
	m := &workerMetrics{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "minidfs_worker_requests_total",
			Help: "Chunk requests handled by this worker, by operation and status code.",
		}, []string{"op", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "minidfs_worker_request_duration_seconds",
			Help:    "Latency of chunk requests handled by this worker, by operation.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
		written: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "minidfs_worker_chunk_bytes_written_total",
			Help: "Chunk bytes stored by successful writes, including peer replication.",
		}),
		read: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "minidfs_worker_chunk_bytes_read_total",
			Help: "Chunk bytes sent in response to chunk reads.",
		}),
	}
	m.reg.MustRegister(m.requests, m.duration, m.written, m.read, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "minidfs_worker_chunks_stored",
		Help: "Chunk files currently on this worker's disk.",
	}, func() float64 {
		ids, err := s.store.List(context.Background())
		if err != nil {
			return 0
		}
		return float64(len(ids))
	}))
	return m
}

// registerScrubber exposes the scrubber's lifetime counters; it is a no-op without a scrubber.
func (m *workerMetrics) registerScrubber(sc ScrubberControl) {
	counter := func(name, help string, f func() float64) {
		m.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, f))
	}
	counter("minidfs_worker_scrubber_passes_total", "Completed scrubber passes.", func() float64 { return float64(sc.Status().Passes) })
	counter("minidfs_worker_scrubber_chunks_scanned_total", "Chunks the scrubber has verified.", func() float64 { return float64(sc.Status().ChunksScanned) })
	counter("minidfs_worker_scrubber_corrupt_quarantined_total", "Corrupt chunk copies the scrubber moved to quarantine.", func() float64 { return float64(sc.Status().CorruptQuarantined) })
	counter("minidfs_worker_scrubber_errors_total", "Scrubber errors.", func() float64 { return float64(sc.Status().Errors) })
}

// registerDisk exposes the free and total bytes of the filesystem holding the chunks.
func (m *workerMetrics) registerDisk(stats func() (free, total int64)) {
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "minidfs_worker_disk_free_bytes", Help: "Free bytes on the filesystem holding the chunks."}, func() float64 { f, _ := stats(); return float64(f) }))
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "minidfs_worker_disk_total_bytes", Help: "Total bytes of the filesystem holding the chunks."}, func() float64 { _, t := stats(); return float64(t) }))
}

func (m *workerMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// instrument records one request's count and latency; get and replicate-peer bytes are counted from what is really written.
func (m *workerMetrics) instrument(op string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		h(rec, r)
		m.requests.WithLabelValues(op, strconv.Itoa(rec.code)).Inc()
		m.duration.WithLabelValues(op).Observe(time.Since(start).Seconds())
		switch {
		case op == "put" && rec.code == http.StatusCreated && r.ContentLength > 0:
			m.written.Add(float64(r.ContentLength))
		case op == "get":
			m.read.Add(float64(rec.bytes))
		}
	}
}

// statusWriter remembers the status code and body size while keeping the zero-copy path of the real writer.
type statusWriter struct {
	http.ResponseWriter
	code  int
	bytes int64
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

// ReadFrom lets io.Copy keep using sendfile on the underlying connection.
func (w *statusWriter) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.Copy(w.ResponseWriter, r)
	w.bytes += n
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
