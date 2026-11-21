package benchmarks

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noise returns deterministic pseudo-random bytes, so no two chunks of it are alike.
func noise(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func TestPercentileUsesTheNearestRank(t *testing.T) {
	s := []float64{50, 10, 40, 20, 30}
	assert.Equal(t, 30.0, percentile(s, 50))
	assert.Equal(t, 50.0, percentile(s, 99), "with 5 samples the 99th percentile is the maximum")
	assert.Equal(t, 10.0, percentile(s, 1))
	assert.Equal(t, 50.0, percentile(s, 100))
	assert.Equal(t, 10.0, percentile(s, 0), "p0 is clamped to the first sample")
	assert.Equal(t, 0.0, percentile(nil, 50))
	assert.Equal(t, 7.0, percentile([]float64{7}, 99))
	assert.Equal(t, []float64{50, 10, 40, 20, 30}, s, "the input is not reordered")
	hundred := make([]float64, 100)
	for i := range hundred {
		hundred[i] = float64(i + 1)
	}
	assert.Equal(t, 50.0, percentile(hundred, 50))
	assert.Equal(t, 99.0, percentile(hundred, 99))
}

func TestClusterStoresAndCountsRealBytes(t *testing.T) {
	c := newCluster(t, 4, true)
	require.Len(t, c.workers, 4)
	path := filepath.Join(t.TempDir(), "obj.bin")
	data := noise(2<<20+123, 1)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	d, n, err := c.uploadFile(path)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), n)
	assert.Greater(t, d.Nanoseconds(), int64(0))

	disk, files := c.diskUsage()
	assert.Equal(t, 6, files, "3 chunks, 2 copies each")
	assert.Equal(t, int64(2*(len(data)+3*4)), disk, "every byte twice, plus a 4 byte CRC footer per file")
	assert.EqualValues(t, 6, c.pool.puts.Load())
	assert.EqualValues(t, 2*len(data), c.pool.putBytes.Load())
}

func TestUploadOfAMissingFileFails(t *testing.T) {
	c := newCluster(t, 4, true)
	_, _, err := c.uploadFile(filepath.Join(t.TempDir(), "nope"))
	assert.Error(t, err)
}

func TestIdenticalUploadAddsNoDiskOrNetworkWithDedupOn(t *testing.T) {
	c := newCluster(t, 4, true)
	path := filepath.Join(t.TempDir(), "o.bin")
	data := noise(3<<20, 2)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	_, _, err := c.uploadFile(path)
	require.NoError(t, err)
	disk1, files1 := c.diskUsage()
	pushed1 := c.pool.putBytes.Load()
	_, _, err = c.uploadFile(path)
	require.NoError(t, err)
	disk2, files2 := c.diskUsage()
	assert.Equal(t, disk1, disk2)
	assert.Equal(t, files1, files2)
	assert.Equal(t, pushed1, c.pool.putBytes.Load(), "nothing new is sent to the workers")
}

func TestIdenticalUploadSendsEverythingAgainWithDedupOff(t *testing.T) {
	c := newCluster(t, 4, false)
	path := filepath.Join(t.TempDir(), "o.bin")
	data := noise(3<<20, 2)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	_, _, err := c.uploadFile(path)
	require.NoError(t, err)
	pushed1 := c.pool.putBytes.Load()
	_, _, err = c.uploadFile(path)
	require.NoError(t, err)
	assert.Equal(t, 2*pushed1, c.pool.putBytes.Load(), "every chunk is written to the workers again")
	assert.EqualValues(t, 0, c.api.Bloom().Counters().Queries)
}

// --- dedup benchmark bookkeeping ---

func TestFindLayersKeepsUploadOrderAndReportsMissingImages(t *testing.T) {
	root := t.TempDir()
	for img, names := range map[string][]string{"a": {"02.tar", "01.tar"}, "c": {"01.tar"}} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, img), 0o755))
		for _, n := range names {
			require.NoError(t, os.WriteFile(filepath.Join(root, img, n), []byte("x"), 0o644))
		}
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "a", "ignore.txt"), []byte("x"), 0o644))
	files, missing := findLayers(root, []string{"a", "b", "c"})
	var got []string
	for _, f := range files {
		got = append(got, f.image+"/"+filepath.Base(f.path))
	}
	assert.Equal(t, []string{"a/01.tar", "a/02.tar", "c/01.tar"}, got, "images in the given order, layers sorted, other files ignored")
	assert.Equal(t, []string{"b"}, missing)
	_, missing = findLayers(filepath.Join(root, "nowhere"), []string{"a"})
	assert.Equal(t, []string{"a"}, missing)
}

func TestChunksOfRoundsUp(t *testing.T) {
	const mib = 1 << 20
	for n, want := range map[int64]int{0: 0, 1: 1, mib - 1: 1, mib: 1, mib + 1: 2, 5 * mib: 5, 5*mib + 1: 6} {
		assert.Equal(t, want, chunksOf(n), n)
	}
}

func TestSummarizeComputesRatioAndPercentiles(t *testing.T) {
	r := &dedupResult{UploadedBytes: 300, DiskBytes: 200, BloomFalsePos: 1, BloomTrueNeg: 99, PerFile: []fileStat{
		{Image: "a", Ms: 100}, {Image: "a", Ms: 300}, {Image: "b", Ms: 10}, {Image: "b", Ms: 20},
	}}
	summarize(r)
	assert.EqualValues(t, 600, r.BaselineBytes, "what a replicated store without dedup would write")
	assert.InDelta(t, 3.0, r.StorageRatio, 1e-9)
	assert.Equal(t, 20.0, r.P50Ms)
	assert.Equal(t, 300.0, r.P99Ms)
	assert.Equal(t, 10.0, r.LaterP50Ms, "images after the first only")
	assert.Equal(t, 20.0, r.LaterP99Ms)
	assert.InDelta(t, 0.01, r.BloomFPRate, 1e-9)

	empty := &dedupResult{}
	summarize(empty)
	assert.Equal(t, 0.0, empty.StorageRatio, "no division by zero")
	assert.Equal(t, 0.0, empty.BloomFPRate)
}

func TestRunWorkloadAccountsForEveryChunkPerImage(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, b, 0o644))
		return p
	}
	shared := noise(2<<20, 7)
	items := []workItem{
		{"base", write("base.tar", shared)},
		{"app1", write("app1-base.tar", shared)},
		{"app1", write("app1-own.tar", noise(1<<20+5, 8))},
		{"app2", write("app2-base.tar", shared)},
	}
	on := runWorkload(t, "test", items, true)
	assert.Equal(t, 4, on.Files)
	assert.Equal(t, 2+2+2+2, on.Chunks)
	assert.Equal(t, 4, on.ChunksWritten, "the 2 shared chunks and the 2 chunks of the unique file")
	assert.Equal(t, 4, on.ChunksDeduped)
	require.Len(t, on.PerImage, 3)
	assert.Equal(t, []int{2, 2, 0}, []int{on.PerImage[0].ChunksNew, on.PerImage[1].ChunksNew, on.PerImage[2].ChunksNew})
	assert.Equal(t, []int{0, 2, 2}, []int{on.PerImage[0].ChunksDeduped, on.PerImage[1].ChunksDeduped, on.PerImage[2].ChunksDeduped})
	assert.EqualValues(t, 4, on.DiskFiles/2, "4 unique chunks, 2 copies each")
	assert.Greater(t, on.StorageRatio, 1.5)
	assert.EqualValues(t, 0, on.BloomFalsePos)
	assert.EqualValues(t, 8, on.BloomQueries)

	off := runWorkload(t, "test", items, false)
	assert.Equal(t, 8, off.ChunksWritten, "dedup off writes every chunk")
	assert.Equal(t, 0, off.ChunksDeduped)
	assert.Greater(t, off.PushedBytes, on.PushedBytes)
	assert.EqualValues(t, 0, off.BloomQueries)
	assert.Equal(t, on.DiskBytes, off.DiskBytes, "content addressing: rewriting a chunk replaces the same file")
}

func TestDistOfUsesNearestRankPercentiles(t *testing.T) {
	var s []float64
	for i := 1; i <= 100; i++ {
		s = append(s, float64(i))
	}
	d := distOf(s)
	assert.Equal(t, Dist{N: 100, Min: 1, P50: 50, P95: 95, P99: 99, Max: 100}, d)
	assert.Equal(t, Dist{}, distOf(nil))
	assert.Equal(t, 7.0, distOf([]float64{7}).P99)
}

func TestMedianOfHandlesOddEvenAndEmpty(t *testing.T) {
	assert.Equal(t, 3.0, medianOf([]float64{5, 1, 3}))
	assert.Equal(t, 2.5, medianOf([]float64{4, 1, 3, 2}))
	assert.Equal(t, 0.0, medianOf(nil))
	in := []float64{3, 1, 2}
	medianOf(in)
	assert.Equal(t, []float64{3, 1, 2}, in, "the input is not reordered")
}

func TestSpreadIsReproducibleAndItsIntervalBracketsTheMedian(t *testing.T) {
	v := []float64{10, 11, 12, 13, 14, 15, 30}
	a, b := spreadOf(v), spreadOf(v)
	assert.Equal(t, a, b, "same input, same interval")
	assert.Equal(t, 13.0, a.Median)
	assert.Equal(t, 10.0, a.Min)
	assert.Equal(t, 30.0, a.Max)
	assert.LessOrEqual(t, a.CILo, a.Median)
	assert.GreaterOrEqual(t, a.CIHi, a.Median)
	one := spreadOf([]float64{4})
	assert.Equal(t, 4.0, one.CILo)
	assert.Equal(t, 4.0, one.CIHi)
	assert.Equal(t, Spread{}, spreadOf(nil))
}

func TestRatioIntervalContainsOneForEqualSamplesAndExcludesItForADoubling(t *testing.T) {
	a := []float64{10, 10.4, 9.8, 10.2, 10.1, 9.9, 10.3}
	b := []float64{10.1, 9.9, 10.2, 10.0, 10.3, 9.8, 10.4}
	same := ratioOf(a, b)
	assert.LessOrEqual(t, same.Lo, 1.0)
	assert.GreaterOrEqual(t, same.Hi, 1.0)
	double := make([]float64, len(b))
	for i, x := range b {
		double[i] = 2 * x
	}
	d := ratioOf(double, b)
	assert.InDelta(t, 2.0, d.Value, 0.05)
	assert.Greater(t, d.Lo, 1.5)
	assert.Equal(t, Ratio{}, ratioOf(nil, b))
	assert.Equal(t, Ratio{}, ratioOf(a, []float64{0, 0}), "a zero denominator gives no ratio")
}

func TestLatencyOfPoolsSamplesAndKeepsPerRepetitionPercentiles(t *testing.T) {
	l := latencyOf([][]float64{{1, 2, 3}, nil, {10, 20, 30}})
	assert.Equal(t, 6, l.Pooled.N)
	assert.Equal(t, 30.0, l.Pooled.Max)
	assert.Equal(t, 2, l.RepP50.N, "an empty repetition contributes nothing")
	assert.Equal(t, 11.0, l.RepP50.Median)
}

func TestRepeatRunsOneWarmupThenTheMeasuredRepetitions(t *testing.T) {
	var seen []int
	repeat(t, 3, func(rep int) { seen = append(seen, rep) })
	assert.Equal(t, []int{-1, 0, 1, 2}, seen)
}

func TestRepCountReadsTheEnvironmentAndIgnoresBadValues(t *testing.T) {
	t.Setenv("BENCH_REPS", "")
	assert.Equal(t, 7, repCount())
	t.Setenv("BENCH_REPS", "3")
	assert.Equal(t, 3, repCount())
	for _, bad := range []string{"0", "-2", "x"} {
		t.Setenv("BENCH_REPS", bad)
		assert.Equal(t, 7, repCount(), bad)
	}
}

func TestClusterCloseRemovesItsFilesAndIsIdempotent(t *testing.T) {
	c := newCluster(t, 4, true)
	dirs := append([]string(nil), c.dirs...)
	require.Len(t, dirs, 5, "metadata plus four workers")
	c.Close()
	c.Close()
	for _, d := range dirs {
		_, err := os.Stat(d)
		assert.True(t, os.IsNotExist(err), d)
	}
}
