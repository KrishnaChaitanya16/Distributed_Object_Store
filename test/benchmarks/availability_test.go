package benchmarks

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

type objectRec struct {
	id        string
	sum       [32]byte
	locations []string
}

type availResult struct {
	Replicas        int         `json:"replicas"`
	Objects         int         `json:"objects"`
	Killed          []string    `json:"killed_workers"`
	Served          int         `json:"served_correctly"`
	Corrupt         int         `json:"served_corrupt"`
	Unavailable     int         `json:"unavailable"`
	ExpectedLost    int         `json:"expected_unavailable"`
	StatusCodes     map[int]int `json:"unavailable_status_codes"`
	SuccessRate     float64     `json:"success_rate_pct"`
	BeforeP50Ms     float64     `json:"before_failure_p50_ms"`
	BeforeP99Ms     float64     `json:"before_failure_p99_ms"`
	AfterP50Ms      float64     `json:"after_failure_p50_ms"`
	AfterP99Ms      float64     `json:"after_failure_p99_ms"`
	FallbackServed  int         `json:"served_from_secondary_after_failure"`
	DetectionSecond []float64   `json:"detection_seconds,omitempty"`
}

// uploadSmallObjects stores n single-chunk objects of random bytes and records where each landed.
func uploadSmallObjects(tb testing.TB, c *cluster, n, size int) []objectRec {
	tb.Helper()
	objs := make([]objectRec, n)
	for i := range objs {
		data := make([]byte, size)
		_, _ = rand.Read(data)
		id, err := c.uploadBytes(fmt.Sprintf("obj-%d", i), data)
		if err != nil {
			tb.Fatal(err)
		}
		ch := c.objectChunks(id)
		if len(ch) != 1 {
			tb.Fatalf("object %s has %d chunks, want 1", id, len(ch))
		}
		objs[i] = objectRec{id: id, sum: sha256.Sum256(data), locations: ch[0].Locations}
	}
	return objs
}

// downloadAll fetches every object and classifies the outcome without trusting the status code alone.
func downloadAll(c *cluster, objs []objectRec) (served, corrupt int, codes map[int]int, latencies []float64) {
	codes = map[int]int{}
	for _, o := range objs {
		code, body, d := c.download(o.id)
		switch {
		case code == http.StatusOK && sha256.Sum256(body) == o.sum:
			served++
			latencies = append(latencies, ms(d))
		case code == http.StatusOK:
			corrupt++
		default:
			codes[code]++
		}
	}
	return served, corrupt, codes, latencies
}

// expectedLost counts the objects whose every copy was on a killed worker.
func expectedLost(objs []objectRec, killed []string) int {
	dead := map[string]bool{}
	for _, k := range killed {
		dead[k] = true
	}
	lost := 0
	for _, o := range objs {
		all := true
		for _, l := range o.locations {
			if !dead[l] {
				all = false
			}
		}
		if all {
			lost++
		}
	}
	return lost
}

// runAvailability uploads n objects, kills the named workers and downloads everything again.
func runAvailability(tb testing.TB, n int, singleReplica bool, kill []string) *availResult {
	tb.Helper()
	c := newClusterWith(tb, options{workers: 4, singleReplica: singleReplica})
	objs := uploadSmallObjects(tb, c, n, 64<<10)
	r := &availResult{Replicas: 2, Objects: n, Killed: kill}
	if singleReplica {
		r.Replicas = 1
		// with one copy the controller still reports a secondary location; only the primary holds data
		for i := range objs {
			objs[i].locations = objs[i].locations[:1]
		}
	}
	served, corrupt, _, before := downloadAll(c, objs)
	if served != n || corrupt != 0 {
		tb.Fatalf("before any failure: %d of %d served, %d corrupt", served, n, corrupt)
	}
	r.BeforeP50Ms, r.BeforeP99Ms = percentile(before, 50), percentile(before, 99)

	for _, k := range kill {
		c.kill(k)
	}
	r.ExpectedLost = expectedLost(objs, kill)
	var after []float64
	r.Served, r.Corrupt, r.StatusCodes, after = downloadAll(c, objs)
	r.Unavailable = n - r.Served - r.Corrupt
	r.SuccessRate = 100 * float64(r.Served) / float64(n)
	r.AfterP50Ms, r.AfterP99Ms = percentile(after, 50), percentile(after, 99)
	dead := map[string]bool{}
	for _, k := range kill {
		dead[k] = true
	}
	for _, o := range objs {
		if len(o.locations) > 0 && dead[o.locations[0]] {
			r.FallbackServed++
		}
	}
	r.FallbackServed -= r.Unavailable
	return r
}

func reportAvail(b *testing.B, name string, r *availResult) {
	b.ReportMetric(r.SuccessRate, "success_%")
	b.ReportMetric(float64(r.Unavailable), "unavailable")
	b.ReportMetric(float64(r.Corrupt), "corrupt")
	b.ReportMetric(r.AfterP99Ms, "after_p99_ms")
	b.ReportMetric(r.BeforeP99Ms, "before_p99_ms")
	if out := os.Getenv("BENCH_OUT"); out != "" {
		if err := writeJSON(filepath.Join(out, "availability-"+name+".json"), r); err != nil {
			b.Fatal(err)
		}
	}
	if r.Corrupt != 0 {
		b.Fatalf("%d objects were served with wrong bytes", r.Corrupt)
	}
	if r.Unavailable != r.ExpectedLost {
		b.Fatalf("%d objects unavailable but only %d had every copy on a dead worker", r.Unavailable, r.ExpectedLost)
	}
}

// objectCount is how many objects the availability runs use; BENCH_OBJECTS overrides the spec's 200 for tighter statistics.
func objectCount() int {
	if v := os.Getenv("BENCH_OBJECTS"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return 200
}

func BenchmarkAvailability_RF2_OneWorkerDown(b *testing.B) {
	for i := 0; i < b.N; i++ {
		reportAvail(b, "rf2_one_down", runAvailability(b, objectCount(), false, []string{"worker2"}))
	}
}

func BenchmarkAvailability_RF1_OneWorkerDown(b *testing.B) {
	for i := 0; i < b.N; i++ {
		reportAvail(b, "rf1_one_down", runAvailability(b, objectCount(), true, []string{"worker2"}))
	}
}

func BenchmarkAvailability_RF2_TwoWorkersDown(b *testing.B) {
	for i := 0; i < b.N; i++ {
		reportAvail(b, "rf2_two_down", runAvailability(b, objectCount(), false, []string{"worker2", "worker3"}))
	}
}

func BenchmarkAvailability_RF1_TwoWorkersDown(b *testing.B) {
	for i := 0; i < b.N; i++ {
		reportAvail(b, "rf1_two_down", runAvailability(b, objectCount(), true, []string{"worker2", "worker3"}))
	}
}

// BenchmarkAvailability_DetectionTime measures how long after a worker dies the health monitor declares it OFFLINE.
func BenchmarkAvailability_DetectionTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := newClusterWith(b, options{workers: 4, heartbeats: true})
		time.Sleep(2500 * time.Millisecond)
		var secs []float64
		for _, id := range []string{"worker1", "worker2", "worker3"} {
			start := time.Now()
			c.kill(id)
			for c.table.GetWorkerStatus(id) != models.WorkerOffline {
				if time.Since(start) > 20*time.Second {
					b.Fatalf("%s was never declared offline", id)
				}
				time.Sleep(20 * time.Millisecond)
			}
			secs = append(secs, time.Since(start).Seconds())
			time.Sleep(700 * time.Millisecond)
		}
		sort.Float64s(secs)
		b.ReportMetric(secs[0], "detect_min_s")
		b.ReportMetric(secs[len(secs)-1], "detect_max_s")
		if out := os.Getenv("BENCH_OUT"); out != "" {
			if err := writeJSON(filepath.Join(out, "availability-detection.json"), map[string]any{"detection_seconds": secs}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// --- checks on the benchmark's own bookkeeping ---

func TestObjectCountDefaultsToTheSpecsTwoHundred(t *testing.T) {
	t.Setenv("BENCH_OBJECTS", "")
	assert.Equal(t, 200, objectCount())
	t.Setenv("BENCH_OBJECTS", "1500")
	assert.Equal(t, 1500, objectCount())
	for _, bad := range []string{"0", "-4", "abc"} {
		t.Setenv("BENCH_OBJECTS", bad)
		assert.Equal(t, 200, objectCount(), bad)
	}
}

func TestExpectedLostCountsOnlyObjectsWithNoSurvivingCopy(t *testing.T) {
	objs := []objectRec{
		{locations: []string{"a", "b"}}, {locations: []string{"a", "c"}}, {locations: []string{"b", "c"}}, {locations: []string{"a"}},
	}
	assert.Equal(t, 0, expectedLost(objs, nil))
	assert.Equal(t, 1, expectedLost(objs, []string{"a"}), "only the single-copy object")
	assert.Equal(t, 2, expectedLost(objs, []string{"a", "b"}), "a+b lost, and the single copy on a")
	assert.Equal(t, 4, expectedLost(objs, []string{"a", "b", "c"}))
}

func TestAvailabilityRunClassifiesRealFailures(t *testing.T) {
	rf2 := runAvailability(t, 40, false, []string{"worker2"})
	assert.Equal(t, 40, rf2.Served, "one worker down loses nothing with 2 copies")
	assert.Zero(t, rf2.Unavailable)
	assert.Zero(t, rf2.Corrupt)
	assert.Equal(t, 100.0, rf2.SuccessRate)

	rf1 := runAvailability(t, 40, true, []string{"worker2"})
	assert.Equal(t, rf1.ExpectedLost, rf1.Unavailable, "exactly the objects stored on the dead worker are lost")
	assert.Greater(t, rf1.Unavailable, 0)
	assert.Zero(t, rf1.Corrupt)

	two := runAvailability(t, 60, false, []string{"worker2", "worker3"})
	assert.Equal(t, two.ExpectedLost, two.Unavailable)
	assert.Zero(t, two.Corrupt, "losing both copies must never produce wrong bytes")
	for code := range two.StatusCodes {
		assert.GreaterOrEqual(t, code, 500, "a lost object is an error, not a success")
	}
}

func TestChunkInfoComesFromTheControllersMetadata(t *testing.T) {
	c := newCluster(t, 4, true)
	objs := uploadSmallObjects(t, c, 3, 1000)
	for _, o := range objs {
		assert.Len(t, o.locations, 2)
		assert.NotEqual(t, o.locations[0], o.locations[1])
	}
	_ = bytes.Equal
}
