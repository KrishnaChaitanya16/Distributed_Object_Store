package benchmarks

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	controllerapi "github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/dedup"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// layerImages is the upload order of benchmark 1: the two Ubuntu bases first, then three images that share Debian layers.
var layerImages = []string{"ubuntu_20.04", "ubuntu_22.04", "nginx_latest", "node_18-slim", "python_3.11-slim"}

// sharedBaseImages are three application images built FROM the same base; build_app_images.sh creates them.
var sharedBaseImages = []string{"minidfs-bench-app-requests_1", "minidfs-bench-app-flask_1", "minidfs-bench-app-numpy_1"}

// springImages are three Spring Boot style service images built FROM eclipse-temurin:17-jre-jammy; build_spring_images.sh creates them.
var springImages = []string{"minidfs-bench-spring-user-service_1", "minidfs-bench-spring-order-service_1", "minidfs-bench-spring-payment-service_1"}

// pythonImages is the variant a reviewer proposed: three Python versions that might share a base layer.
var pythonImages = []string{"python_3.11-slim", "python_3.10-slim", "python_3.9-slim"}

const workers = 4

type fileStat struct {
	Image string  `json:"image"`
	File  string  `json:"file"`
	Bytes int64   `json:"bytes"`
	Ms    float64 `json:"ms"`
}

type imageStat struct {
	Image         string  `json:"image"`
	Bytes         int64   `json:"bytes"`
	Ms            float64 `json:"ms"`
	Chunks        int     `json:"chunks"`
	ChunksNew     int     `json:"chunks_written"`
	ChunksDeduped int     `json:"chunks_deduplicated"`
	PushedBytes   int64   `json:"bytes_sent_to_workers"`
}

type dedupResult struct {
	Workload        string      `json:"workload"`
	DedupOn         bool        `json:"dedup_on"`
	Workers         int         `json:"workers"`
	Files           int         `json:"files"`
	UploadedBytes   int64       `json:"uploaded_bytes"`
	BaselineBytes   int64       `json:"no_dedup_bytes_on_disk"`
	DiskBytes       int64       `json:"bytes_on_disk"`
	DiskFiles       int         `json:"chunk_files_on_disk"`
	Chunks          int         `json:"chunks"`
	ChunksWritten   int         `json:"chunks_written"`
	ChunksDeduped   int         `json:"chunks_deduplicated"`
	PushedBytes     int64       `json:"bytes_sent_to_workers"`
	StorageRatio    float64     `json:"storage_ratio"`
	P50Ms           float64     `json:"upload_p50_ms"`
	P99Ms           float64     `json:"upload_p99_ms"`
	LaterP50Ms      float64     `json:"upload_p50_ms_images_2_plus"`
	LaterP99Ms      float64     `json:"upload_p99_ms_images_2_plus"`
	MsPerChunk      float64     `json:"upload_ms_per_chunk"`
	BloomQueries    uint64      `json:"bloom_queries"`
	BloomFalsePos   uint64      `json:"bloom_false_positives"`
	BloomTrueNeg    uint64      `json:"bloom_definitely_new"`
	BloomFPRate     float64     `json:"bloom_false_positive_rate"`
	BloomEstimateFP float64     `json:"bloom_estimated_fp_at_end"`
	ElapsedS        float64     `json:"elapsed_s"`
	UploadSeconds   float64     `json:"upload_seconds"`
	PerImage        []imageStat `json:"per_image,omitempty"`
	PerFile         []fileStat  `json:"per_file"`
}

// chunksOf is how many 1 MiB chunks a file of n bytes is cut into.
func chunksOf(n int64) int {
	cs := int64(controllerapi.ChunkSize)
	return int((n + cs - 1) / cs)
}

// layerDir is where fetch_layers.sh puts the extracted image layers; BENCH_LAYERS_DIR overrides it.
func layerDir() string {
	if d := os.Getenv("BENCH_LAYERS_DIR"); d != "" {
		return d
	}
	return filepath.Join("..", "..", "testdata", "layers")
}

type layerFile struct{ image, path string }

// findLayers lists the extracted layers in upload order and reports images that are missing.
func findLayers(root string, images []string) (files []layerFile, missing []string) {
	for _, img := range images {
		matches, _ := filepath.Glob(filepath.Join(root, img, "*.tar"))
		sort.Strings(matches)
		if len(matches) == 0 {
			missing = append(missing, img)
		}
		for _, m := range matches {
			files = append(files, layerFile{img, m})
		}
	}
	return files, missing
}

// summarize fills in the derived numbers of a run from its raw measurements.
func summarize(r *dedupResult) {
	r.BaselineBytes = r.UploadedBytes * replicationFactor
	if r.DiskBytes > 0 {
		r.StorageRatio = float64(r.BaselineBytes) / float64(r.DiskBytes)
	}
	lat := make([]float64, 0, len(r.PerFile))
	later := make([]float64, 0, len(r.PerFile))
	first := ""
	if len(r.PerFile) > 0 {
		first = r.PerFile[0].Image
	}
	for _, f := range r.PerFile {
		lat = append(lat, f.Ms)
		if f.Image != first {
			later = append(later, f.Ms)
		}
	}
	r.P50Ms, r.P99Ms = percentile(lat, 50), percentile(lat, 99)
	r.LaterP50Ms, r.LaterP99Ms = percentile(later, 50), percentile(later, 99)
	if tn := r.BloomFalsePos + r.BloomTrueNeg; tn > 0 {
		r.BloomFPRate = float64(r.BloomFalsePos) / float64(tn)
	}
}

type workItem struct{ image, path string }

// runWorkload uploads the files in order to a fresh 4-worker cluster and measures the result.
func runWorkload(tb testing.TB, name string, items []workItem, dedupOn bool) *dedupResult {
	tb.Helper()
	c := newCluster(tb, workers, dedupOn)
	res := &dedupResult{Workload: name, DedupOn: dedupOn, Workers: workers}
	began := time.Now()

	var cur *imageStat
	var putsAtStart, bytesAtStart int64
	closeImage := func() {
		if cur == nil {
			return
		}
		cur.PushedBytes = c.pool.putBytes.Load() - bytesAtStart
		cur.ChunksNew = int(c.pool.puts.Load()-putsAtStart) / replicationFactor
		cur.ChunksDeduped = cur.Chunks - cur.ChunksNew
		res.PerImage = append(res.PerImage, *cur)
	}
	var uploadTime time.Duration
	for _, it := range items {
		if cur == nil || cur.Image != it.image {
			closeImage()
			cur = &imageStat{Image: it.image}
			putsAtStart, bytesAtStart = c.pool.puts.Load(), c.pool.putBytes.Load()
		}
		d, n, err := c.uploadFile(it.path)
		if err != nil {
			tb.Fatal(err)
		}
		chunks := chunksOf(n)
		res.Files++
		res.UploadedBytes += n
		res.Chunks += chunks
		cur.Bytes += n
		cur.Ms += ms(d)
		cur.Chunks += chunks
		uploadTime += d
		res.PerFile = append(res.PerFile, fileStat{Image: it.image, File: filepath.Base(it.path), Bytes: n, Ms: ms(d)})
	}
	closeImage()

	res.ElapsedS = time.Since(began).Seconds()
	res.DiskBytes, res.DiskFiles = c.diskUsage()
	res.PushedBytes = c.pool.putBytes.Load()
	res.ChunksWritten = int(c.pool.puts.Load()) / replicationFactor
	res.ChunksDeduped = res.Chunks - res.ChunksWritten
	res.UploadSeconds = uploadTime.Seconds()
	if res.Chunks > 0 {
		res.MsPerChunk = ms(uploadTime) / float64(res.Chunks)
	}
	bc := c.api.Bloom().Counters()
	res.BloomQueries, res.BloomFalsePos, res.BloomTrueNeg = bc.Queries, bc.FalsePositive, bc.DefinitelyNew
	res.BloomEstimateFP = c.api.Bloom().Stats(1).EstimatedFP
	summarize(res)
	return res
}

// makeRandomMedia writes n files of fileBytes random bytes each into dir.
func makeRandomMedia(tb testing.TB, dir string, n int, fileBytes int64) []workItem {
	tb.Helper()
	var items []workItem
	for i := 1; i <= n; i++ {
		p := filepath.Join(dir, fmt.Sprintf("media-%d.bin", i))
		f, err := os.Create(p)
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := io.CopyN(f, rand.Reader, fileBytes); err != nil {
			tb.Fatal(err)
		}
		if err := f.Close(); err != nil {
			tb.Fatal(err)
		}
		items = append(items, workItem{image: "media", path: p})
	}
	return items
}

func layerItems(tb testing.TB, images []string) []workItem {
	tb.Helper()
	files, missing := findLayers(layerDir(), images)
	if len(missing) > 0 {
		tb.Skipf("layers for %s are missing: run test/benchmarks/fetch_layers.sh", strings.Join(missing, ", "))
	}
	items := make([]workItem, len(files))
	for i, f := range files {
		items[i] = workItem{image: f.image, path: f.path}
	}
	return items
}

func totalBytes(tb testing.TB, items []workItem) int64 {
	tb.Helper()
	var n int64
	for _, it := range items {
		st, err := os.Stat(it.path)
		if err != nil {
			tb.Fatal(err)
		}
		n += st.Size()
	}
	return n
}

type dedupSide struct {
	DedupOn        bool    `json:"dedup_on"`
	SentMiB        Spread  `json:"bytes_sent_to_workers_mib"`
	DiskMiB        Spread  `json:"bytes_on_worker_disks_mib"`
	ChunksDeduped  Spread  `json:"chunks_deduplicated"`
	StorageRatio   Spread  `json:"storage_ratio"`
	BloomFPPct     Spread  `json:"bloom_false_positive_pct"`
	ChunkMs        Latency `json:"upload_ms_per_chunk,omitempty"`
	UploadSeconds  Spread  `json:"upload_seconds"`
	ThroughputMiBs Spread  `json:"throughput_mib_per_s"`
}

type dedupReport struct {
	Workload      string    `json:"workload"`
	Files         int       `json:"files"`
	Chunks        int       `json:"chunks"`
	UploadedMiB   float64   `json:"uploaded_mib"`
	On            dedupSide `json:"dedup_on"`
	Off           dedupSide `json:"dedup_off"`
	TimeOffOverOn Ratio     `json:"upload_time_off_over_on"`
}

// fileChunkMs is the upload time per chunk of each file, one sample per file.
func fileChunkMs(r *dedupResult) []float64 {
	out := make([]float64, 0, len(r.PerFile))
	for _, f := range r.PerFile {
		if n := chunksOf(f.Bytes); n > 0 {
			out = append(out, f.Ms/float64(n))
		}
	}
	return out
}

func foldDedup(on bool, runs []*dedupResult) dedupSide {
	mib := float64(1 << 20)
	var perRep [][]float64
	for _, r := range runs {
		perRep = append(perRep, fileChunkMs(r))
	}
	return dedupSide{
		DedupOn:        on,
		SentMiB:        spreadOf(col(runs, func(r *dedupResult) float64 { return float64(r.PushedBytes) / mib })),
		DiskMiB:        spreadOf(col(runs, func(r *dedupResult) float64 { return float64(r.DiskBytes) / mib })),
		ChunksDeduped:  spreadOf(col(runs, func(r *dedupResult) float64 { return float64(r.ChunksDeduped) })),
		StorageRatio:   spreadOf(col(runs, func(r *dedupResult) float64 { return r.StorageRatio })),
		BloomFPPct:     spreadOf(col(runs, func(r *dedupResult) float64 { return 100 * r.BloomFPRate })),
		ChunkMs:        latencyOf(perRep),
		UploadSeconds:  spreadOf(col(runs, func(r *dedupResult) float64 { return r.UploadSeconds })),
		ThroughputMiBs: spreadOf(col(runs, func(r *dedupResult) float64 { return float64(r.UploadedBytes) / mib / r.UploadSeconds })),
	}
}

// runDedupPair uploads the items with dedup on and off, alternating which goes first each repetition, and reports both sides.
func runDedupPair(b *testing.B, name string, items []workItem) {
	var on, off []*dedupResult
	repeat(b, repCount(), func(rep int) {
		order := []bool{true, false}
		if rep%2 != 0 {
			order = []bool{false, true}
		}
		for _, mode := range order {
			r := runWorkload(b, name, items, mode)
			if rep < 0 {
				continue
			}
			if mode {
				on = append(on, r)
			} else {
				off = append(off, r)
			}
		}
	})
	rep := dedupReport{
		Workload: name, Files: on[0].Files, Chunks: on[0].Chunks, UploadedMiB: float64(on[0].UploadedBytes) / float64(1<<20),
		On: foldDedup(true, on), Off: foldDedup(false, off),
		TimeOffOverOn: ratioOf(col(off, func(r *dedupResult) float64 { return r.UploadSeconds }), col(on, func(r *dedupResult) float64 { return r.UploadSeconds })),
	}
	b.ReportMetric(rep.On.StorageRatio.Median, "storage_ratio_x")
	b.ReportMetric(rep.On.ChunkMs.Pooled.P99, "on_p99_ms_per_chunk")
	b.ReportMetric(rep.Off.ChunkMs.Pooled.P99, "off_p99_ms_per_chunk")
	b.ReportMetric(rep.TimeOffOverOn.Value, "time_off_over_on")
	saveReport(b, "dedup-"+name, rep)
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func benchDockerLayers(b *testing.B, name string, images []string) {
	items := layerItems(b, images)
	for i := 0; i < b.N; i++ {
		runDedupPair(b, name, items)
	}
}

// BenchmarkDedup_DockerLayers uploads the layers of five real images with dedup on and off.
func BenchmarkDedup_DockerLayers(b *testing.B) { benchDockerLayers(b, "docker_layers", layerImages) }

// BenchmarkDedup_PythonVersions uploads python 3.11, 3.10 and 3.9 slim, which share no whole layer.
func BenchmarkDedup_PythonVersions(b *testing.B) {
	benchDockerLayers(b, "python_versions", pythonImages)
}

// BenchmarkDedup_SharedBase uploads three application images built FROM python:3.11-slim, so they share its layers exactly.
func BenchmarkDedup_SharedBase(b *testing.B) { benchDockerLayers(b, "shared_base", sharedBaseImages) }

// BenchmarkDedup_SpringBase uploads three services built FROM one JRE image, where the shared base is almost the whole image.
func BenchmarkDedup_SpringBase(b *testing.B) { benchDockerLayers(b, "spring_base", springImages) }

// BenchmarkDedup_RawMedia uploads random bytes of the same total size as the Docker workload, where no chunk repeats.
func BenchmarkDedup_RawMedia(b *testing.B) {
	// the raw-media total matches the Docker workload when the layers exist, otherwise 5 x 20 MiB
	var each int64 = 20 << 20
	if files, missing := findLayers(layerDir(), layerImages); len(missing) == 0 {
		var n int64
		for _, f := range files {
			if st, err := os.Stat(f.path); err == nil {
				n += st.Size()
			}
		}
		each = n / 5
	}
	dir, err := os.MkdirTemp("", "minidfs-media-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	items := makeRandomMedia(b, dir, 5, each)
	for i := 0; i < b.N; i++ {
		runDedupPair(b, "raw_media", items)
	}
}

// --- the cost of the dedup check itself ---

func newMeta(tb testing.TB) *metadata.MetadataStore {
	tb.Helper()
	m, err := metadata.NewMetadataStore(filepath.Join(tb.TempDir(), "meta.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { m.Close() })
	return m
}

func chunkIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		b := make([]byte, 32)
		_, _ = rand.Read(b)
		ids[i] = fmt.Sprintf("%x", b)
	}
	return ids
}

// BenchmarkDedupCheck_BloomMiss is the Bloom filter lookup an upload does for a chunk it has never seen.
func BenchmarkDedupCheck_BloomMiss(b *testing.B) {
	f := dedup.New(1_000_000, 0.01)
	ids := chunkIDs(1 << 14)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Observe(ids[i&(len(ids)-1)])
	}
}

// BenchmarkDedupCheck_BloomAdd is the Bloom insert after a new chunk is stored.
func BenchmarkDedupCheck_BloomAdd(b *testing.B) {
	f := dedup.New(1_000_000, 0.01)
	ids := chunkIDs(1 << 14)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Add(ids[i&(len(ids)-1)])
	}
}

// BenchmarkDedupCheck_ChunkRecordLookup is the bbolt read that confirms or refutes a Bloom "maybe".
func BenchmarkDedupCheck_ChunkRecordLookup(b *testing.B) {
	m := newMeta(b)
	ids := chunkIDs(1 << 12)
	for _, id := range ids {
		if err := m.SaveChunk(b.Context(), &models.ChunkRecord{ChunkID: id, State: models.ChunkDurable}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.GetChunk(b.Context(), ids[i&(len(ids)-1)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDedupCheck_RecordChunkReference is the bbolt transaction that records a chunk reference; both modes pay it, so it is not dedup overhead.
func BenchmarkDedupCheck_RecordChunkReference(b *testing.B) {
	m := newMeta(b)
	obj := &models.ObjectManifest{ID: "bench-object", Status: models.StatusStaged, ModifiedAt: time.Now()}
	if err := m.SaveObject(b.Context(), obj); err != nil {
		b.Fatal(err)
	}
	ids := chunkIDs(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.AddChunkRef(b.Context(), obj.ID, models.ChunkRef{ChunkID: ids[0], Index: i}); err != nil {
			b.Fatal(err)
		}
	}
}
