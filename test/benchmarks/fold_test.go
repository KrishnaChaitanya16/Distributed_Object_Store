package benchmarks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileChunkMsDividesEachFilesTimeByItsChunks(t *testing.T) {
	r := &dedupResult{PerFile: []fileStat{{Bytes: 3 << 20, Ms: 30}, {Bytes: 1, Ms: 5}, {Bytes: 0, Ms: 9}}}
	assert.Equal(t, []float64{10, 5}, fileChunkMs(r), "an empty file has no chunks and is skipped")
}

func TestFoldDedupSummarizesEveryRepetition(t *testing.T) {
	mk := func(sec float64) *dedupResult {
		return &dedupResult{PushedBytes: 10 << 20, DiskBytes: 8 << 20, ChunksDeduped: 2, StorageRatio: 1.25, UploadSeconds: sec, UploadedBytes: 4 << 20, PerFile: []fileStat{{Bytes: 1 << 20, Ms: 20}}}
	}
	s := foldDedup(true, []*dedupResult{mk(1), mk(2), mk(3)})
	assert.True(t, s.DedupOn)
	assert.Equal(t, 10.0, s.SentMiB.Median)
	assert.Equal(t, 2.0, s.UploadSeconds.Median)
	assert.Equal(t, 1.0, s.UploadSeconds.Min)
	assert.Equal(t, 3, s.ChunkMs.Pooled.N)
	assert.InDelta(t, 2.0, s.ThroughputMiBs.Median, 1e-9, "4 MiB in 2 s")
}

func TestFoldAvailabilityPoolsLossAndLatency(t *testing.T) {
	a := &availResult{Replicas: 1, Objects: 100, Unavailable: 20, ExpectedLost: 20, Served: 80, StatusCodes: map[int]int{503: 20}, before: []float64{1, 2}, after: []float64{3, 4}}
	b := &availResult{Replicas: 1, Objects: 100, Unavailable: 30, ExpectedLost: 30, Served: 70, StatusCodes: map[int]int{503: 30}, before: []float64{1, 2}, after: []float64{5, 6}}
	r := foldAvailability("x", []*availResult{a, b})
	assert.InDelta(t, 25.0, r.UnavailablePctPooled, 1e-9)
	assert.Equal(t, 50, r.StatusCodes[503])
	assert.Equal(t, 25.0, r.Unavailable.Median)
	assert.Equal(t, 4, r.After.Pooled.N)
}

func TestFoldHedgeSumsCountsAndKeepsSamples(t *testing.T) {
	cells := []hedgeRun{
		{ThresholdMs: 25, DelayMs: 50, SlowShare: 1, Reads: 10, Hedges: 10, samples: []float64{27, 28}},
		{ThresholdMs: 25, DelayMs: 50, SlowShare: 1, Reads: 10, Hedges: 5, Failed: 1, samples: []float64{26}},
	}
	c := foldHedge(cells)
	assert.Equal(t, 20, c.Reads)
	assert.EqualValues(t, 15, c.Hedges)
	assert.Equal(t, 75.0, c.HedgeRatePct)
	assert.Equal(t, 1, c.Failed)
	assert.Equal(t, 3, c.ReadMs.Pooled.N)
}

func TestFoldHealReportsTheSpreadOfPassTimes(t *testing.T) {
	r := foldHeal([]healResult{{LostChunks: 5, ChunkKiB: 64, PassMs: 10, Restored: 5}, {LostChunks: 5, ChunkKiB: 64, PassMs: 30, Restored: 5}, {LostChunks: 5, ChunkKiB: 64, PassMs: 20, Restored: 5}})
	assert.Equal(t, 20.0, r.PassMs.Median)
	assert.Equal(t, 10.0, r.PassMs.Min)
	assert.Equal(t, 30.0, r.PassMs.Max)
	assert.Equal(t, 5, r.LostChunks)
}

func TestFoldPushesSeparatesTheFirstPushFromTheRest(t *testing.T) {
	run := []pushStat{{Ms: 100, PutBytes: 20 * mib, CumMs: 100, CumBytes: 20 * mib}, {Ms: 40, PutBytes: 6 * mib, CumMs: 140, CumBytes: 26 * mib, DiskBytes: 26 * mib}, {Ms: 50, PutBytes: 6 * mib, CumMs: 190, CumBytes: 32 * mib, DiskBytes: 32 * mib}}
	s := foldPushes(true, [][]pushStat{run, run})
	assert.Equal(t, 2, s.FirstPushMs.N)
	assert.Equal(t, 4, s.LaterPushMs.Pooled.N)
	assert.Equal(t, 6.0, s.LaterSentMiB.Median)
	assert.Equal(t, 32.0, s.TotalSentMiB.Median)
	assert.InDelta(t, 0.19, s.TotalSeconds.Median, 1e-9)
	assert.Equal(t, []float64{40, 50, 40, 50}, pooledLater([][]pushStat{run, run}))
	assert.Equal(t, []float64{190, 190}, totals([][]pushStat{run, run}))
}

func TestFoldMediaComputesTheStorageRatioFromBytes(t *testing.T) {
	r := foldMedia(false, []mediaRun{{Chunks: 4, PerChunkMs: 20, StoredBytes: 8 * mib, LogicalByte: 8 * mib, fileMs: []float64{40, 40}}})
	assert.Equal(t, 1.0, r.StorageRatio.Median)
	assert.Equal(t, 8.0, r.StoredMiB.Median)
	assert.Equal(t, 2, r.FileMs.Pooled.N)
}

func TestFoldScrubKeepsTheCountsPerRepetition(t *testing.T) {
	r := foldScrub([]*scrubRun{{Variant: "data", Objects: 5, MismatchSeen: 4, readMs: []float64{1}}, {Variant: "data", Objects: 5, MismatchSeen: 8, readMs: []float64{2}}})
	assert.Equal(t, 6.0, r.MismatchAtRead.Median)
	assert.Equal(t, 4.0, r.MismatchAtRead.Min)
	assert.Equal(t, "data", r.Corruption)
	assert.Equal(t, 2, r.ReadMs.Pooled.N)
}

func TestSaltedKeysDifferBetweenSetsAndAreStable(t *testing.T) {
	assert.Equal(t, saltedKeys(5, "a"), saltedKeys(5, "a"))
	assert.NotEqual(t, saltedKeys(5, "a"), saltedKeys(5, "b"))
}
