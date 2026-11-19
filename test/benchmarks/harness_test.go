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
