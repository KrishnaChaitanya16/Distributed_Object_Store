// Package metrics defines all Prometheus metrics for the controller.
// All vars are registered with the default registry via promauto.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTP layer — recorded by the instrumented middleware in server.go
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "minidfs_http_requests_total",
		Help: "Total HTTP requests by handler, method and status code.",
	}, []string{"handler", "method", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "minidfs_http_request_duration_seconds",
		Help:    "HTTP request latency by handler and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"handler", "method"})

	// Upload path
	UploadBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minidfs_upload_bytes_total",
		Help: "Total bytes received across all successful uploads.",
	})

	ChunksNewTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minidfs_chunks_new_total",
		Help: "New (non-deduplicated) chunks written to workers.",
	})

	ChunksDedupTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minidfs_chunks_dedup_total",
		Help: "Chunks skipped because an identical chunk already existed (CAS dedup).",
	})

	// Download path
	DownloadBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minidfs_download_bytes_total",
		Help: "Total bytes sent to clients across all successful downloads.",
	})

	ReadRepairsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minidfs_read_repairs_total",
		Help: "Background read-repairs triggered by primary hash mismatches.",
	})

	HedgedReadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minidfs_hedged_reads_total",
		Help: "Chunk fetches where the hedge timer fired and secondary was contacted.",
	})

	// Rebalancer
	RebalancerRepairsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minidfs_rebalancer_repairs_total",
		Help: "Chunks re-replicated by the background rebalancer to restore RF=2.",
	})

	// Cluster
	WorkersOnline = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "minidfs_workers_online",
		Help: "Number of workers currently considered online by the health monitor.",
	})
)
