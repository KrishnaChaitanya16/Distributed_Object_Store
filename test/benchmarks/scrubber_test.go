package benchmarks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/scrubber"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
)

type scrubRun struct {
	Variant       string  `json:"corruption"`
	WithScrubber  bool    `json:"scrubber_pass_run"`
	Objects       int     `json:"objects"`
	Chunks        int     `json:"chunks"`
	CopiesOnDisk  int     `json:"copies_on_disk"`
	CorruptCopies int     `json:"corrupt_copies_injected"`
	DirectReads   int     `json:"direct_worker_reads"`
	DirectWrong   int     `json:"direct_reads_returning_wrong_bytes"`
	Served        int     `json:"controller_objects_served_correctly"`
	WrongBytes    int     `json:"controller_objects_served_wrong_bytes"`
	Unavailable   int     `json:"controller_objects_unavailable"`
	MismatchSeen  uint64  `json:"sha256_mismatches_hit_by_readers"`
	Quarantined   int64   `json:"copies_quarantined_by_scrubber"`
	ScrubPassMs   float64 `json:"slowest_scrubber_pass_ms"`
	BadCopiesLeft int     `json:"corrupt_copies_still_served"`
	Under2Healthy int     `json:"chunks_with_fewer_than_2_good_copies"`
	readMs        []float64
}

// seededObject returns the same pseudo-random content for the same i, so two clusters can be loaded with identical data.
func seededObject(i, size int) []byte {
	b := make([]byte, size)
	rand.New(rand.NewSource(int64(1000 + i))).Read(b)
	return b
}

type copyRef struct {
	node *node
	id   string
}

// corruptFooter overwrites the stored CRC32C of a chunk file with 0xFF bytes, as the spec describes.
func corruptFooter(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	_, err = f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, st.Size()-4)
	return err
}

// flipData inverts one byte inside the chunk's data, as bit rot would.
func flipData(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	b := []byte{0}
	if _, err := f.ReadAt(b, 4096); err != nil {
		return err
	}
	b[0] ^= 0xFF
	_, err = f.WriteAt(b, 4096)
	return err
}

// pickVictims chooses n distinct chunks and one copy of each, deterministically.
func pickVictims(c *cluster, n int) []copyRef {
	byChunk := map[string][]*node{}
	for _, w := range c.workers {
		for _, id := range w.chunkIDs() {
			byChunk[id] = append(byChunk[id], w)
		}
	}
	ids := make([]string, 0, len(byChunk))
	for id := range byChunk {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rand.New(rand.NewSource(42)).Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	var out []copyRef
	for _, id := range ids[:n] {
		out = append(out, copyRef{byChunk[id][0], id})
	}
	return out
}

// directRead fetches a chunk straight from a worker, the way a store with no end-to-end check would serve it.
func directRead(n *node, id string) (wrong bool) {
	resp, err := http.Get(n.srv.URL + "/internal/chunks/" + id)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return false
	}
	defer resp.Body.Close()
	h := sha256.New()
	_, _ = io.Copy(h, resp.Body)
	return hex.EncodeToString(h.Sum(nil)) != id
}

// runScrub loads objects of the given size, corrupts 10 chunk copies and reads everything back, with or without a scrubber pass first.
func runScrub(tb testing.TB, variant string, withScrubber bool, objects, size int) *scrubRun {
	tb.Helper()
	const victims = 10
	c := newCluster(tb, 4, true)
	var ids []string
	for i := 0; i < objects; i++ {
		id, err := c.uploadBytes(fmt.Sprintf("o%d", i), seededObject(i, size))
		if err != nil {
			tb.Fatal(err)
		}
		ids = append(ids, id)
	}
	r := &scrubRun{Variant: variant, WithScrubber: withScrubber, Objects: objects}
	for _, w := range c.workers {
		r.CopiesOnDisk += len(w.chunkIDs())
	}
	r.Chunks = r.CopiesOnDisk / replicationFactor

	for _, v := range pickVictims(c, victims) {
		var err error
		if variant == "footer" {
			err = corruptFooter(v.node.chunkPath(v.id))
		} else {
			err = flipData(v.node.chunkPath(v.id))
		}
		if err != nil {
			tb.Fatal(err)
		}
		r.CorruptCopies++
	}

	for _, w := range c.workers {
		for _, id := range w.chunkIDs() {
			r.DirectReads++
			if directRead(w, id) {
				r.DirectWrong++
			}
		}
	}

	if withScrubber {
		for _, w := range c.workers {
			if d := ms(w.scrubNow(tb)); d > r.ScrubPassMs {
				r.ScrubPassMs = d
			}
			r.Quarantined += w.scrub.Status().CorruptQuarantined
		}
	}

	mismatchBefore := events.Default.Counts()["download.corrupt"]
	for i, id := range ids {
		code, body, d := c.download(id)
		switch {
		case code == http.StatusOK && sha256.Sum256(body) == sha256.Sum256(seededObject(i, size)):
			r.Served++
			r.readMs = append(r.readMs, ms(d))
		case code == http.StatusOK:
			r.WrongBytes++
		default:
			r.Unavailable++
		}
	}
	r.MismatchSeen = events.Default.Counts()["download.corrupt"] - mismatchBefore

	// what is left on disk: copies that still fail their checksum, and chunks that lost a good copy
	good := map[string]int{}
	for _, w := range c.workers {
		for _, id := range w.chunkIDs() {
			ok, err := w.store.VerifyCRC(context.Background(), id)
			if err == nil && ok && !directReadWrong(w, id) {
				good[id]++
			} else {
				r.BadCopiesLeft++
			}
		}
	}
	for id := range collectChunks(c) {
		if good[id] < 2 {
			r.Under2Healthy++
		}
	}
	return r
}

func directReadWrong(n *node, id string) bool { return directRead(n, id) }

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

func collectChunks(c *cluster) map[string]bool {
	m := map[string]bool{}
	for _, w := range c.workers {
		for _, id := range w.chunkIDs() {
			m[id] = true
		}
	}
	return m
}

type scrubReport struct {
	Corruption       string  `json:"corruption"`
	ScrubberRan      bool    `json:"scrubber_pass_run"`
	Objects          int     `json:"objects"`
	Chunks           int     `json:"chunks"`
	CorruptInjected  Spread  `json:"corrupt_copies_injected"`
	DirectWrongBytes Spread  `json:"direct_worker_reads_returning_wrong_bytes"`
	ControllerWrong  Spread  `json:"controller_objects_served_wrong_bytes"`
	Unavailable      Spread  `json:"controller_objects_unavailable"`
	MismatchAtRead   Spread  `json:"sha256_mismatches_hit_by_readers"`
	Quarantined      Spread  `json:"copies_quarantined_by_scrubber"`
	BadCopiesLeft    Spread  `json:"corrupt_copies_still_on_disk"`
	Under2Healthy    Spread  `json:"chunks_with_fewer_than_2_good_copies"`
	SlowestPassMs    Spread  `json:"slowest_scrubber_pass_ms"`
	ReadMs           Latency `json:"object_read_ms_after_corruption"`
}

func foldScrub(runs []*scrubRun) scrubReport {
	f := func(g func(*scrubRun) float64) Spread { return spreadOf(col(runs, g)) }
	var reads [][]float64
	for _, r := range runs {
		reads = append(reads, r.readMs)
	}
	return scrubReport{
		Corruption: runs[0].Variant, ScrubberRan: runs[0].WithScrubber, Objects: runs[0].Objects, Chunks: runs[0].Chunks,
		CorruptInjected:  f(func(r *scrubRun) float64 { return float64(r.CorruptCopies) }),
		DirectWrongBytes: f(func(r *scrubRun) float64 { return float64(r.DirectWrong) }),
		ControllerWrong:  f(func(r *scrubRun) float64 { return float64(r.WrongBytes) }),
		Unavailable:      f(func(r *scrubRun) float64 { return float64(r.Unavailable) }),
		MismatchAtRead:   f(func(r *scrubRun) float64 { return float64(r.MismatchSeen) }),
		Quarantined:      f(func(r *scrubRun) float64 { return float64(r.Quarantined) }),
		BadCopiesLeft:    f(func(r *scrubRun) float64 { return float64(r.BadCopiesLeft) }),
		Under2Healthy:    f(func(r *scrubRun) float64 { return float64(r.Under2Healthy) }),
		SlowestPassMs:    f(func(r *scrubRun) float64 { return r.ScrubPassMs }),
		ReadMs:           latencyOf(reads),
	}
}

// benchScrub runs one corruption scenario through the common protocol.
func benchScrub(b *testing.B, name, variant string, withScrubber bool) {
	for i := 0; i < b.N; i++ {
		var runs []*scrubRun
		repeat(b, repCount(), func(rep int) {
			r := runScrub(b, variant, withScrubber, 50, 2<<20)
			if r.WrongBytes != 0 {
				b.Fatalf("%d objects were served with wrong bytes", r.WrongBytes)
			}
			if rep >= 0 {
				runs = append(runs, r)
			}
		})
		rep := foldScrub(runs)
		b.ReportMetric(rep.MismatchAtRead.Median, "sha256_mismatches_at_read")
		b.ReportMetric(rep.ReadMs.Pooled.P99, "read_p99_ms")
		saveReport(b, "scrubber-"+name, rep)
	}
}

func BenchmarkScrubber_Footer_NoScrubber(b *testing.B) {
	benchScrub(b, "footer_no_scrubber", "footer", false)
}

func BenchmarkScrubber_Footer_WithScrubber(b *testing.B) {
	benchScrub(b, "footer_with_scrubber", "footer", true)
}

func BenchmarkScrubber_Data_NoScrubber(b *testing.B) {
	benchScrub(b, "data_no_scrubber", "data", false)
}

func BenchmarkScrubber_Data_WithScrubber(b *testing.B) {
	benchScrub(b, "data_with_scrubber", "data", true)
}

// passDuration writes n chunks of 1 MiB on one worker, corrupts 10 of them and times a scrubber pass with the given pause between chunks.
func passDuration(tb testing.TB, n int, pause time.Duration) (dur time.Duration, scanned, found int) {
	tb.Helper()
	dir, err := os.MkdirTemp("", "minidfs-scrub-")
	require.NoError(tb, err)
	defer os.RemoveAll(dir)
	st, err := storage.New(dir)
	require.NoError(tb, err)
	ctx := context.Background()
	var ids []string
	for i := 0; i < n; i++ {
		data := seededObject(i, 1<<20)
		sum := sha256.Sum256(data)
		id := hex.EncodeToString(sum[:])
		require.NoError(tb, st.Write(ctx, id, bytesReader(data), int64(len(data))))
		ids = append(ids, id)
	}
	for i := 0; i < 10 && i < n; i++ {
		require.NoError(tb, flipData(filepath.Join(dir, "chunks", ids[i*(n/10)]+".bin")))
	}
	sc, err := scrubber.New(st, scrubber.Config{Interval: time.Hour, Pause: pause, Logf: func(string, ...any) {}})
	require.NoError(tb, err)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sc.Run(cctx) //nolint:errcheck
	start := time.Now()
	require.True(tb, sc.Trigger())
	for {
		s := sc.Status()
		if s.Passes >= 1 && !s.Running {
			return time.Since(start), s.LastPassScanned, s.LastPassCorrupt
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// BenchmarkScrubber_PassDuration times one full scrubber pass over 1000 chunks of 1 MiB, with no pause and with the default 10 ms pause between chunks.
func BenchmarkScrubber_PassDuration(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var fast, paced, rate []float64
		var found []float64
		repeat(b, repCount(), func(rep int) {
			f, scanned, bad := passDuration(b, 1000, 0)
			p, _, _ := passDuration(b, 1000, 10*time.Millisecond)
			if bad != 10 {
				b.Fatalf("found %d corrupt chunks, want 10", bad)
			}
			if rep >= 0 {
				fast, paced = append(fast, f.Seconds()), append(paced, p.Seconds())
				rate = append(rate, float64(scanned)/f.Seconds())
				found = append(found, float64(bad))
			}
		})
		out := map[string]any{"chunks": 1000, "chunk_mib": 1, "no_pause_seconds": spreadOf(fast), "pause_10ms_seconds": spreadOf(paced), "no_pause_chunks_per_s": spreadOf(rate), "corrupt_found": spreadOf(found)}
		b.ReportMetric(medianOf(fast), "pass_no_pause_s")
		b.ReportMetric(medianOf(paced), "pass_10ms_pause_s")
		saveReport(b, "scrubber-pass_duration", out)
	}
}

// --- checks on the benchmark's own bookkeeping ---

func TestCorruptionHelpersChangeExactlyWhatTheyClaim(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := storage.New(dir)
	require.NoError(t, err)
	data := seededObject(1, 64<<10)
	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:])
	require.NoError(t, st.Write(ctx, id, bytesReader(data), int64(len(data))))
	path := filepath.Join(dir, "chunks", id+".bin")

	ok, err := st.VerifyCRC(ctx, id)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, corruptFooter(path))
	ok, _ = st.VerifyCRC(ctx, id)
	assert.False(t, ok, "a damaged footer fails the CRC check")
	rc, err := st.Read(ctx, id)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, data, got, "but the data a reader receives is untouched")

	require.NoError(t, st.Write(ctx, id, bytesReader(data), int64(len(data))))
	require.NoError(t, flipData(path))
	rc, _ = st.Read(ctx, id)
	got, _ = io.ReadAll(rc)
	rc.Close()
	assert.NotEqual(t, data, got, "bit rot changes the bytes a reader receives")
	assert.Equal(t, len(data), len(got))
}

func TestScrubRunFindsTheInjectedCorruptionAndNeverServesWrongBytes(t *testing.T) {
	const objects, size = 20, 1 << 20
	for _, variant := range []string{"footer", "data"} {
		without := runScrub(t, variant, false, objects, size)
		with := runScrub(t, variant, true, objects, size)
		assert.Equal(t, 10, without.CorruptCopies)
		assert.Zero(t, without.WrongBytes, variant+": the controller never returns wrong bytes")
		assert.Zero(t, with.WrongBytes)
		assert.Equal(t, objects, with.Served, variant+": every object still downloads")
		assert.Equal(t, objects, without.Served)
		assert.EqualValues(t, 10, with.Quarantined, variant+": the scrubber finds every injected copy")
		assert.Zero(t, without.Quarantined)
		assert.Zero(t, with.BadCopiesLeft, "nothing corrupt is left on disk to be served")
		assert.Equal(t, 10, with.Under2Healthy, "the scrubber alone does not restore copies; the reconciler does")
		if variant == "footer" {
			assert.Zero(t, without.DirectWrong, "footer damage does not change the data a reader gets")
			assert.Equal(t, 10, without.BadCopiesLeft, "and no read ever notices it, so it stays")
		} else {
			assert.Equal(t, 10, without.DirectWrong, "reading the workers directly would return 10 corrupt chunks")
			assert.Equal(t, 10, with.DirectWrong, "measured before the scrubber runs, so the same in both")
			assert.Greater(t, without.BadCopiesLeft, 0, "copies on secondaries are never read, so they rot unnoticed")
			assert.Less(t, without.BadCopiesLeft, 10, "copies on primaries were healed by read-repair when downloaded")
		}
	}
}
