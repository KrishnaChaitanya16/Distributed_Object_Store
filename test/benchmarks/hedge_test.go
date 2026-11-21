package benchmarks

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
)

type hedgeRun struct {
	Label        string  `json:"label"`
	ThresholdMs  int     `json:"hedge_threshold_ms"`
	DelayMs      float64 `json:"slow_worker_delay_ms"`
	SlowShare    float64 `json:"share_of_reads_slowed"`
	Reads        int     `json:"reads"`
	P50Ms        float64 `json:"p50_ms"`
	P95Ms        float64 `json:"p95_ms"`
	P99Ms        float64 `json:"p99_ms"`
	MaxMs        float64 `json:"max_ms"`
	Hedges       uint64  `json:"hedge_requests_fired"`
	HedgeRatePct float64 `json:"hedge_rate_pct"`
	Failed       int     `json:"failed_reads"`
	samples      []float64
}

// loadPrimaryOnSlow stores n single-chunk objects whose primary is worker1, which sits behind the slow proxy.
func loadPrimaryOnSlow(tb testing.TB, c *cluster, n int) []objectRec {
	tb.Helper()
	var objs []objectRec
	for tries := 0; len(objs) < n; tries++ {
		if tries > 200*n {
			tb.Fatal("could not find enough chunks that hash to worker1")
		}
		data := make([]byte, 32<<10)
		_, _ = rand.Read(data)
		sum := sha256.Sum256(data)
		if p, err := c.ring.Primary(hex.EncodeToString(sum[:])); err != nil || p != "worker1" {
			continue
		}
		id, err := c.uploadBytes(fmt.Sprintf("h%d", len(objs)), data)
		if err != nil {
			tb.Fatal(err)
		}
		ch := c.objectChunks(id)
		if ch[0].Locations[0] != "worker1" {
			tb.Fatalf("object %s landed on %v, not worker1 first", id, ch[0].Locations)
		}
		objs = append(objs, objectRec{id: id, sum: sum, locations: ch[0].Locations})
	}
	return objs
}

// measureReads downloads every object `passes` times and summarizes latency and hedging.
func measureReads(tb testing.TB, c *cluster, objs []objectRec, passes int) hedgeRun {
	tb.Helper()
	before := events.Default.Counts()["download.hedge"]
	var lat []float64
	run := hedgeRun{}
	for p := 0; p < passes; p++ {
		for _, o := range objs {
			code, body, d := c.download(o.id)
			if code != http.StatusOK || sha256.Sum256(body) != o.sum {
				run.Failed++
				continue
			}
			lat = append(lat, ms(d))
		}
	}
	run.Reads = len(lat) + run.Failed
	run.samples = lat
	run.P50Ms, run.P95Ms, run.P99Ms = percentile(lat, 50), percentile(lat, 95), percentile(lat, 99)
	run.MaxMs = percentile(lat, 100)
	run.Hedges = events.Default.Counts()["download.hedge"] - before
	if run.Reads > 0 {
		run.HedgeRatePct = 100 * float64(run.Hedges) / float64(run.Reads)
	}
	return run
}

// runDelays measures reads at each fixed delay of the slow worker with the hedge threshold set to thresholdMs (0 disables hedging).
func runDelays(tb testing.TB, thresholdMs int, delays []time.Duration, objects, passes int) []hedgeRun {
	tb.Helper()
	tb.Setenv("HEDGE_THRESHOLD_MS", strconv.Itoa(thresholdMs))
	c := newClusterWith(tb, options{workers: 4, slowWorker: "worker1"})
	if c.api.HedgeMs() != thresholdMs {
		tb.Fatalf("hedge threshold is %d, want %d", c.api.HedgeMs(), thresholdMs)
	}
	objs := loadPrimaryOnSlow(tb, c, objects)
	var out []hedgeRun
	for _, d := range delays {
		c.slow["worker1"].SetDelay(d)
		r := measureReads(tb, c, objs, passes)
		r.ThresholdMs, r.DelayMs, r.SlowShare = thresholdMs, float64(d.Milliseconds()), 1
		out = append(out, r)
	}
	return out
}

type hedgeCell struct {
	ThresholdMs  int     `json:"hedge_threshold_ms"`
	DelayMs      float64 `json:"slow_worker_delay_ms"`
	SlowShare    float64 `json:"share_of_reads_slowed"`
	ReadMs       Latency `json:"read_ms"`
	Reads        int     `json:"reads_all_reps"`
	Hedges       uint64  `json:"hedge_requests_all_reps"`
	HedgeRatePct float64 `json:"hedge_rate_pct"`
	Failed       int     `json:"failed_reads"`
}

// foldHedge combines one cell (a threshold and delay) across repetitions.
func foldHedge(cells []hedgeRun) hedgeCell {
	out := hedgeCell{ThresholdMs: cells[0].ThresholdMs, DelayMs: cells[0].DelayMs, SlowShare: cells[0].SlowShare}
	var per [][]float64
	for _, c := range cells {
		per = append(per, c.samples)
		out.Reads += c.Reads
		out.Hedges += c.Hedges
		out.Failed += c.Failed
	}
	out.ReadMs = latencyOf(per)
	if out.Reads > 0 {
		out.HedgeRatePct = 100 * float64(out.Hedges) / float64(out.Reads)
	}
	return out
}

var hedgeDelays = []time.Duration{0, 10 * time.Millisecond, 30 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}

// BenchmarkHedge_Delays reads from a worker that is slow by a fixed delay, with hedging at the default 25 ms and with it disabled.
func BenchmarkHedge_Delays(b *testing.B) {
	for i := 0; i < b.N; i++ {
		byTh := map[int][][]hedgeRun{}
		repeat(b, repCount(), func(rep int) {
			order := []int{25, 0}
			if rep%2 != 0 {
				order = []int{0, 25}
			}
			for _, th := range order {
				runs := runDelays(b, th, hedgeDelays, 100, 1)
				if rep >= 0 {
					byTh[th] = append(byTh[th], runs)
				}
			}
		})
		out := map[string][]hedgeCell{}
		for th, reps := range byTh {
			name := "hedging_on_25ms"
			if th == 0 {
				name = "hedging_off"
			}
			for d := range hedgeDelays {
				var cells []hedgeRun
				for _, r := range reps {
					cells = append(cells, r[d])
				}
				out[name] = append(out[name], foldHedge(cells))
			}
		}
		for _, c := range out["hedging_on_25ms"] {
			b.ReportMetric(c.ReadMs.Pooled.P99, fmt.Sprintf("on_p99_ms_delay%.0f", c.DelayMs))
		}
		for _, c := range out["hedging_off"] {
			b.ReportMetric(c.ReadMs.Pooled.P99, fmt.Sprintf("off_p99_ms_delay%.0f", c.DelayMs))
		}
		saveReport(b, "hedge-delays", out)
	}
}

// BenchmarkHedge_ThresholdSweep uses a worker that is slow (100 ms) for 10% of reads and measures several hedge thresholds.
func BenchmarkHedge_ThresholdSweep(b *testing.B) {
	thresholds := []int{0, 2, 5, 10, 25, 50, 100}
	for i := 0; i < b.N; i++ {
		byTh := map[int][]hedgeRun{}
		repeat(b, repCount(), func(rep int) {
			for _, th := range thresholds {
				b.Setenv("HEDGE_THRESHOLD_MS", strconv.Itoa(th))
				c := newClusterWith(b, options{workers: 4, slowWorker: "worker1"})
				objs := loadPrimaryOnSlow(b, c, 100)
				c.slow["worker1"].SetIntermittent(100*time.Millisecond, 0.10)
				r := measureReads(b, c, objs, 2)
				r.ThresholdMs, r.DelayMs, r.SlowShare = th, 100, 0.10
				if rep >= 0 {
					byTh[th] = append(byTh[th], r)
				}
				c.Close()
			}
		})
		var out []hedgeCell
		for _, th := range thresholds {
			out = append(out, foldHedge(byTh[th]))
		}
		saveReport(b, "hedge-threshold_sweep", out)
	}
}

// --- checks on the benchmark's own machinery ---

func TestSlowProxyHoldsBackOnlyChunkReadsByTheConfiguredAmount(t *testing.T) {
	t.Setenv("HEDGE_THRESHOLD_MS", "0")
	c := newClusterWith(t, options{workers: 4, slowWorker: "worker1"})
	objs := loadPrimaryOnSlow(t, c, 5)
	c.slow["worker1"].SetDelay(0)
	fast := measureReads(t, c, objs, 1)
	c.slow["worker1"].SetDelay(60 * time.Millisecond)
	slow := measureReads(t, c, objs, 1)
	assert.Zero(t, fast.Failed+slow.Failed)
	assert.Less(t, fast.P50Ms, 30.0)
	assert.GreaterOrEqual(t, slow.P50Ms, 60.0, "with hedging off the read waits for the slow primary")
	assert.Zero(t, slow.Hedges)
}

func TestIntermittentDelaySlowsRoughlyTheConfiguredShare(t *testing.T) {
	sc := newSlowProxy("http://127.0.0.1:1")
	defer sc.srv.Close()
	sc.SetIntermittent(time.Millisecond, 0.25)
	n := 0
	for i := 0; i < 4000; i++ {
		if sc.shouldDelay() {
			n++
		}
	}
	assert.InDelta(t, 1000, n, 120)
	sc.SetDelay(time.Millisecond)
	assert.True(t, sc.shouldDelay(), "SetDelay slows every read")
}

func TestHedgeFiresOnlyAboveTheThreshold(t *testing.T) {
	runs := runDelays(t, 25, []time.Duration{0, 10 * time.Millisecond, 60 * time.Millisecond}, 8, 1)
	require.Len(t, runs, 3)
	assert.EqualValues(t, 0, runs[0].Hedges, "a fast primary needs no hedge")
	assert.EqualValues(t, 0, runs[1].Hedges, "10 ms is under the 25 ms threshold")
	assert.EqualValues(t, 8, runs[2].Hedges, "60 ms is over it: every read hedges")
	assert.Less(t, runs[2].P99Ms, 45.0, "and the secondary answers long before the slow primary would")
	assert.Zero(t, runs[2].Failed)
}

func TestLoadPrimaryOnSlowReallyPlacesThemOnWorker1(t *testing.T) {
	c := newClusterWith(t, options{workers: 4, slowWorker: "worker1"})
	for _, o := range loadPrimaryOnSlow(t, c, 6) {
		assert.Equal(t, "worker1", o.locations[0])
		assert.Len(t, o.locations, 2)
	}
}
