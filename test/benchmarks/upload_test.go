package benchmarks

import (
	"bytes"
	"crypto/rand"
	"fmt"
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

type pushSide struct {
	DedupOn      bool    `json:"dedup_on"`
	FirstPushMs  Dist    `json:"first_push_ms"`
	LaterPushMs  Latency `json:"pushes_2_to_10_ms"`
	TotalSeconds Spread  `json:"total_seconds_10_pushes"`
	FirstSentMiB Spread  `json:"first_push_sent_mib"`
	LaterSentMiB Spread  `json:"each_later_push_sent_mib"`
	TotalSentMiB Spread  `json:"total_sent_mib"`
	TotalDiskMiB Spread  `json:"total_disk_mib"`
}

type pushReport struct {
	On             pushSide `json:"dedup_on"`
	Off            pushSide `json:"dedup_off"`
	TimeOffOverOn  Ratio    `json:"total_time_off_over_on"`
	LaterPushRatio Ratio    `json:"later_push_median_off_over_on"`
	BytesOffOverOn float64  `json:"total_bytes_off_over_on"`
}

func foldPushes(on bool, runs [][]pushStat) pushSide {
	var first []float64
	var later [][]float64
	var total, sentFirst, sentLater, sent, disk []float64
	for _, r := range runs {
		first = append(first, r[0].Ms)
		var l []float64
		for _, p := range r[1:] {
			l = append(l, p.Ms)
		}
		later = append(later, l)
		last := r[len(r)-1]
		total = append(total, last.CumMs/1000)
		sentFirst = append(sentFirst, float64(r[0].PutBytes)/mib)
		sentLater = append(sentLater, float64(r[1].PutBytes)/mib)
		sent = append(sent, float64(last.CumBytes)/mib)
		disk = append(disk, float64(last.DiskBytes)/mib)
	}
	return pushSide{DedupOn: on, FirstPushMs: distOf(first), LaterPushMs: latencyOf(later), TotalSeconds: spreadOf(total), FirstSentMiB: spreadOf(sentFirst), LaterSentMiB: spreadOf(sentLater), TotalSentMiB: spreadOf(sent), TotalDiskMiB: spreadOf(disk)}
}

func pooledLater(runs [][]pushStat) []float64 {
	var out []float64
	for _, r := range runs {
		for _, p := range r[1:] {
			out = append(out, p.Ms)
		}
	}
	return out
}

func totals(runs [][]pushStat) []float64 {
	out := make([]float64, len(runs))
	for i, r := range runs {
		out[i] = r[len(r)-1].CumMs
	}
	return out
}

// BenchmarkCompounding_ImagePushes pushes 10 images of 7 shared + 3 unique 1 MiB chunks, with dedup on and off.
func BenchmarkCompounding_ImagePushes(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var on, off [][]pushStat
		repeat(b, repCount(), func(rep int) {
			order := []bool{true, false}
			if rep%2 != 0 {
				order = []bool{false, true}
			}
			for _, mode := range order {
				r := pushImages(b, mode, 10, 7, 3)
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
		rep := pushReport{On: foldPushes(true, on), Off: foldPushes(false, off)}
		rep.TimeOffOverOn = ratioOf(totals(off), totals(on))
		rep.LaterPushRatio = ratioOf(pooledLater(off), pooledLater(on))
		rep.BytesOffOverOn = rep.Off.TotalSentMiB.Median / rep.On.TotalSentMiB.Median
		b.ReportMetric(rep.TimeOffOverOn.Value, "total_time_off_over_on")
		b.ReportMetric(rep.On.LaterPushMs.Pooled.P99, "on_later_push_p99_ms")
		saveReport(b, "compounding", rep)
	}
}

type mediaRun struct {
	DedupOn     bool    `json:"dedup_on"`
	Chunks      int     `json:"chunks"`
	TotalMs     float64 `json:"total_ms"`
	PerChunkMs  float64 `json:"per_chunk_ms"`
	StoredBytes int64   `json:"stored_bytes"`
	LogicalByte int64   `json:"logical_bytes_x_replicas"`
	fileMs      []float64
}

// pushRandomMedia uploads `files` objects of fileMiB random MiB each and times every upload.
func pushRandomMedia(tb testing.TB, dedupOn bool, files, fileMiB int) mediaRun {
	tb.Helper()
	c := newCluster(tb, 4, dedupOn)
	blobs := make([][]byte, files)
	for i := range blobs {
		blobs[i] = make([]byte, fileMiB*mib)
		_, _ = rand.Read(blobs[i])
	}
	res := mediaRun{DedupOn: dedupOn, Chunks: files * fileMiB}
	start := time.Now()
	for i, d := range blobs {
		t := time.Now()
		if _, err := c.uploadBytes(fmt.Sprintf("media-%d", i), d); err != nil {
			tb.Fatal(err)
		}
		res.fileMs = append(res.fileMs, ms(time.Since(t)))
	}
	res.TotalMs = ms(time.Since(start))
	res.PerChunkMs = res.TotalMs / float64(res.Chunks)
	res.StoredBytes, _ = c.diskUsage()
	res.LogicalByte = int64(res.Chunks) * mib * 2
	return res
}

type mediaSide struct {
	DedupOn      bool    `json:"dedup_on"`
	FileMs       Latency `json:"upload_ms_per_5mib_file"`
	PerChunkMs   Spread  `json:"ms_per_chunk"`
	StoredMiB    Spread  `json:"stored_mib"`
	StorageRatio Spread  `json:"storage_ratio"`
}

type mediaReport struct {
	Chunks        int       `json:"chunks_per_rep"`
	On            mediaSide `json:"dedup_on"`
	Off           mediaSide `json:"dedup_off"`
	TimeOffOverOn Ratio     `json:"per_chunk_time_off_over_on"`
}

func foldMedia(on bool, runs []mediaRun) mediaSide {
	var per [][]float64
	for _, r := range runs {
		per = append(per, r.fileMs)
	}
	return mediaSide{
		DedupOn: on, FileMs: latencyOf(per),
		PerChunkMs:   spreadOf(col(runs, func(r mediaRun) float64 { return r.PerChunkMs })),
		StoredMiB:    spreadOf(col(runs, func(r mediaRun) float64 { return float64(r.StoredBytes) / mib })),
		StorageRatio: spreadOf(col(runs, func(r mediaRun) float64 { return float64(r.LogicalByte) / float64(r.StoredBytes) })),
	}
}

// BenchmarkRandomMedia_Overhead uploads 10 x 5 MiB of random bytes with dedup on and off.
func BenchmarkRandomMedia_Overhead(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var on, off []mediaRun
		repeat(b, repCount(), func(rep int) {
			order := []bool{true, false}
			if rep%2 != 0 {
				order = []bool{false, true}
			}
			for _, mode := range order {
				r := pushRandomMedia(b, mode, 10, 5)
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
		rep := mediaReport{Chunks: on[0].Chunks, On: foldMedia(true, on), Off: foldMedia(false, off)}
		rep.TimeOffOverOn = ratioOf(col(off, func(r mediaRun) float64 { return r.PerChunkMs }), col(on, func(r mediaRun) float64 { return r.PerChunkMs }))
		b.ReportMetric(rep.On.FileMs.Pooled.P99, "on_p99_ms_per_file")
		b.ReportMetric(rep.Off.FileMs.Pooled.P99, "off_p99_ms_per_file")
		saveReport(b, "random-media", rep)
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
