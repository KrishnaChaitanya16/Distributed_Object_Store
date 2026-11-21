package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Every benchmark follows one protocol: a discarded warm-up repetition, then repCount() measured repetitions, each on a fresh cluster.
// Latency samples from all repetitions are pooled into one distribution (P50, P95, P99, max), and the per-repetition P50 and P99 are kept so the spread between repetitions is visible.
// Any other measurement is reported as the median of the repetitions with its min to max range and a bootstrap 95% confidence interval of the median.
const (
	defaultReps = 7
	warmups     = 1
	bootstraps  = 2000
)

// repCount is how many measured repetitions each benchmark runs; BENCH_REPS overrides the default of 7.
func repCount() int {
	if n, err := strconv.Atoi(os.Getenv("BENCH_REPS")); err == nil && n > 0 {
		return n
	}
	return defaultReps
}

// repeat runs fn once as a warm-up (rep -1, to be discarded) and then n measured times, closing every cluster after each run.
func repeat(tb testing.TB, n int, fn func(rep int)) {
	tb.Helper()
	for rep := -warmups; rep < n; rep++ {
		fn(rep)
		closeLive()
	}
}

// Dist summarizes latency samples.
type Dist struct {
	N   int     `json:"n"`
	Min float64 `json:"min"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

func distOf(s []float64) Dist {
	if len(s) == 0 {
		return Dist{}
	}
	return Dist{N: len(s), Min: percentile(s, 0), P50: percentile(s, 50), P95: percentile(s, 95), P99: percentile(s, 99), Max: percentile(s, 100)}
}

// Spread summarizes one number measured once per repetition.
type Spread struct {
	N      int     `json:"n"`
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	CILo   float64 `json:"ci95_lo"`
	CIHi   float64 `json:"ci95_hi"`
}

// medianOf is the usual median: the middle value, or the mean of the two middle values.
func medianOf(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func resample(r *rand.Rand, v []float64) []float64 {
	out := make([]float64, len(v))
	for i := range out {
		out[i] = v[r.Intn(len(v))]
	}
	return out
}

// bootstrapCI returns the 2.5th and 97.5th percentile of stat over resamples, with a fixed seed so a run's report is reproducible.
func bootstrapCI(stat func(r *rand.Rand) float64) (lo, hi float64) {
	r := rand.New(rand.NewSource(1))
	draws := make([]float64, bootstraps)
	for i := range draws {
		draws[i] = stat(r)
	}
	sort.Float64s(draws)
	return draws[int(0.025*float64(bootstraps))], draws[int(0.975*float64(bootstraps))-1]
}

func spreadOf(v []float64) Spread {
	if len(v) == 0 {
		return Spread{}
	}
	sp := Spread{N: len(v), Median: medianOf(v), Min: percentile(v, 0), Max: percentile(v, 100)}
	sp.CILo, sp.CIHi = bootstrapCI(func(r *rand.Rand) float64 { return medianOf(resample(r, v)) })
	return sp
}

// Ratio is median(a) / median(b) with a bootstrap 95% confidence interval; an interval containing 1 means the two are not distinguishable.
type Ratio struct {
	Value float64 `json:"value"`
	Lo    float64 `json:"ci95_lo"`
	Hi    float64 `json:"ci95_hi"`
}

func ratioOf(a, b []float64) Ratio {
	if len(a) == 0 || len(b) == 0 || medianOf(b) == 0 {
		return Ratio{}
	}
	rt := Ratio{Value: medianOf(a) / medianOf(b)}
	rt.Lo, rt.Hi = bootstrapCI(func(r *rand.Rand) float64 {
		d := medianOf(resample(r, b))
		if d == 0 {
			return 0
		}
		return medianOf(resample(r, a)) / d
	})
	return rt
}

// Latency is the distribution of one kind of operation over all repetitions.
type Latency struct {
	Pooled Dist   `json:"pooled"`
	RepP50 Spread `json:"p50_across_reps"`
	RepP99 Spread `json:"p99_across_reps"`
}

func latencyOf(perRep [][]float64) Latency {
	var all, p50, p99 []float64
	for _, r := range perRep {
		if len(r) == 0 {
			continue
		}
		all = append(all, r...)
		p50 = append(p50, percentile(r, 50))
		p99 = append(p99, percentile(r, 99))
	}
	return Latency{Pooled: distOf(all), RepP50: spreadOf(p50), RepP99: spreadOf(p99)}
}

// Env describes the machine and code every number in a report came from.
type Env struct {
	CPU     string `json:"cpu"`
	Threads int    `json:"threads"`
	Kernel  string `json:"kernel"`
	Go      string `json:"go"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
	Reps    int    `json:"measured_reps"`
	Warmups int    `json:"warmup_reps"`
}

func environment() Env {
	e := Env{Threads: runtime.NumCPU(), Go: runtime.Version(), Commit: os.Getenv("BENCH_COMMIT"), Date: time.Now().UTC().Format("2006-01-02"), Reps: repCount(), Warmups: warmups}
	if raw, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(l, "model name") {
				e.CPU = strings.TrimSpace(l[strings.Index(l, ":")+1:])
				break
			}
		}
	}
	if raw, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		e.Kernel = strings.TrimSpace(string(raw))
	}
	return e
}

// saveReport writes a benchmark's results with its environment to BENCH_OUT/<name>.json.
func saveReport(tb testing.TB, name string, results any) {
	tb.Helper()
	out := os.Getenv("BENCH_OUT")
	if out == "" {
		return
	}
	if err := writeJSON(filepath.Join(out, name+".json"), map[string]any{"benchmark": name, "env": environment(), "results": results}); err != nil {
		tb.Fatal(err)
	}
}

// col extracts one number from every repetition's result.
func col[T any](runs []T, f func(T) float64) []float64 {
	out := make([]float64, len(runs))
	for i, r := range runs {
		out[i] = f(r)
	}
	return out
}

func describe(sp Spread) string {
	return fmt.Sprintf("%.1f [%.1f-%.1f]", sp.Median, sp.Min, sp.Max)
}
