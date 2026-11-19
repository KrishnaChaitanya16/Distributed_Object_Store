package benchmarks

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mib = 1 << 20

type pushStat struct {
	Push      int     `json:"push"`
	Ms        float64 `json:"ms"`
	PutBytes  int64   `json:"bytes_sent_to_workers"`
	Puts      int64   `json:"chunk_puts"`
	CumMs     float64 `json:"cumulative_ms"`
	CumBytes  int64   `json:"cumulative_bytes_sent_to_workers"`
	DiskBytes int64   `json:"disk_bytes_after"`
}

// pushImages uploads `images` objects of shared+unique 1 MiB chunks one after another and records each push.
func pushImages(tb testing.TB, dedupOn bool, images, shared, unique int) []pushStat {
	tb.Helper()
	c := newCluster(tb, 4, dedupOn)
	var base []byte
	for k := 0; k < shared; k++ {
		base = append(base, noise(mib, int64(1000+k))...)
	}
	var out []pushStat
	var cumMs float64
	var cumBytes int64
	for i := 1; i <= images; i++ {
		img := append([]byte(nil), base...)
		for k := 0; k < unique; k++ {
			img = append(img, noise(mib, int64(50000+i*100+k))...)
		}
		b0, p0 := c.pool.putBytes.Load(), c.pool.puts.Load()
		start := time.Now()
		if _, err := c.uploadBytes(fmt.Sprintf("image-%d", i), img); err != nil {
			tb.Fatal(err)
		}
		d := ms(time.Since(start))
		s := pushStat{Push: i, Ms: d, PutBytes: c.pool.putBytes.Load() - b0, Puts: c.pool.puts.Load() - p0}
		cumMs += d
		cumBytes += s.PutBytes
		s.CumMs, s.CumBytes = cumMs, cumBytes
		s.DiskBytes, _ = c.diskUsage()
		out = append(out, s)
	}
	return out
}

func median(v []float64) float64 { return percentile(v, 50) }

// BenchmarkCompounding_ImagePushes pushes 10 images of 7 shared + 3 unique 1 MiB chunks, three times each way, and keeps the median push times.
func BenchmarkCompounding_ImagePushes(b *testing.B) {
	for n := 0; n < b.N; n++ {
		const reps = 3
		var on, off [][]pushStat
		for r := 0; r < reps; r++ {
			on = append(on, pushImages(b, true, 10, 7, 3))
			off = append(off, pushImages(b, false, 10, 7, 3))
		}
		fold := func(runs [][]pushStat) []pushStat {
			var out []pushStat
			for i := range runs[0] {
				var t, cum []float64
				for _, r := range runs {
					t = append(t, r[i].Ms)
					cum = append(cum, r[i].CumMs)
				}
				s := runs[0][i]
				s.Ms, s.CumMs = median(t), median(cum)
				out = append(out, s)
			}
			return out
		}
		fOn, fOff := fold(on), fold(off)
		b.ReportMetric(fOn[9].CumMs, "dedup_on_total_ms")
		b.ReportMetric(fOff[9].CumMs, "dedup_off_total_ms")
		b.ReportMetric(float64(fOff[9].CumBytes)/float64(fOn[9].CumBytes), "bytes_sent_ratio")
		if out := os.Getenv("BENCH_OUT"); out != "" {
			if err := writeJSON(filepath.Join(out, "compounding.json"), map[string]any{"dedup_on": fOn, "dedup_off": fOff}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

type mediaRun struct {
	DedupOn     bool    `json:"dedup_on"`
	Chunks      int     `json:"chunks"`
	TotalMs     float64 `json:"total_ms"`
	PerChunkMs  float64 `json:"per_chunk_ms"`
	StoredBytes int64   `json:"stored_bytes"`
	LogicalByte int64   `json:"logical_bytes_x_replicas"`
}

// pushRandomMedia uploads `files` objects of fileMiB random MiB each and times the whole upload.
func pushRandomMedia(tb testing.TB, dedupOn bool, files, fileMiB int) mediaRun {
	tb.Helper()
	c := newCluster(tb, 4, dedupOn)
	blobs := make([][]byte, files)
	for i := range blobs {
		blobs[i] = make([]byte, fileMiB*mib)
		_, _ = rand.Read(blobs[i])
	}
	start := time.Now()
	for i, d := range blobs {
		if _, err := c.uploadBytes(fmt.Sprintf("media-%d", i), d); err != nil {
			tb.Fatal(err)
		}
	}
	total := time.Since(start)
	chunks := files * fileMiB
	disk, _ := c.diskUsage()
	return mediaRun{DedupOn: dedupOn, Chunks: chunks, TotalMs: ms(total), PerChunkMs: ms(total) / float64(chunks), StoredBytes: disk, LogicalByte: int64(chunks) * mib * 2}
}

// BenchmarkRandomMedia_Overhead uploads 10 x 5 MiB of random bytes with dedup on and off, alternating, seven times each.
func BenchmarkRandomMedia_Overhead(b *testing.B) {
	for n := 0; n < b.N; n++ {
		const reps = 7
		var on, off []mediaRun
		var onMs, offMs []float64
		for r := 0; r < reps; r++ {
			first, second := true, false
			if r%2 == 1 {
				first, second = false, true
			}
			for _, mode := range []bool{first, second} {
				run := pushRandomMedia(b, mode, 10, 5)
				if mode {
					on, onMs = append(on, run), append(onMs, run.PerChunkMs)
				} else {
					off, offMs = append(off, run), append(offMs, run.PerChunkMs)
				}
			}
		}
		b.ReportMetric(median(onMs), "dedup_on_ms_per_chunk")
		b.ReportMetric(median(offMs), "dedup_off_ms_per_chunk")
		if out := os.Getenv("BENCH_OUT"); out != "" {
			if err := writeJSON(filepath.Join(out, "random-media.json"), map[string]any{"dedup_on": on, "dedup_off": off}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestSharedChunksAreSentOnceWhenDedupIsOn(t *testing.T) {
	on := pushImages(t, true, 3, 4, 2)
	off := pushImages(t, false, 3, 4, 2)
	assert.EqualValues(t, 12*mib, on[0].PutBytes, "first push: 6 chunks x 2 replicas")
	assert.EqualValues(t, 4*mib, on[1].PutBytes, "later pushes send only the 2 unique chunks x 2 replicas")
	assert.EqualValues(t, 4*mib, on[2].PutBytes)
	for _, s := range off {
		assert.EqualValues(t, 12*mib, s.PutBytes, "without dedup every push sends everything")
	}
	assert.Equal(t, on[2].DiskBytes, off[2].DiskBytes, "identical chunks overwrite the same file, so disk is equal; dedup saves transfer")
	assert.Less(t, on[2].CumBytes, off[2].CumBytes)
}

func TestRandomMediaIsStoredInFullWithOrWithoutDedup(t *testing.T) {
	on := pushRandomMedia(t, true, 2, 2)
	off := pushRandomMedia(t, false, 2, 2)
	assert.Equal(t, 4, on.Chunks)
	assert.GreaterOrEqual(t, on.StoredBytes, int64(8*mib), "4 chunks x 2 replicas, random data has nothing to dedupe")
	assert.InDelta(t, off.StoredBytes, on.StoredBytes, float64(64<<10))
}

func TestNoiseIsDeterministicAndDistinctBetweenSeeds(t *testing.T) {
	require.True(t, bytes.Equal(noise(64, 5), noise(64, 5)))
	assert.False(t, bytes.Equal(noise(64, 5), noise(64, 6)))
}
