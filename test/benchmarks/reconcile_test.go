package benchmarks

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/gc"
)

type healResult struct {
	LostChunks        int     `json:"chunks_lost_by_restarted_worker"`
	ChunkKiB          int     `json:"chunk_kib"`
	SingleCopyBefore  int     `json:"chunks_with_one_copy_before"`
	SingleCopyNoRecon int     `json:"chunks_with_one_copy_after_wait_without_reconciler"`
	WaitMs            float64 `json:"wait_without_reconciler_ms"`
	PassMs            float64 `json:"reconciler_pass_ms"`
	Restored          int     `json:"replicas_restored"`
	ChunksPerSec      float64 `json:"chunks_per_sec"`
	MiBPerSec         float64 `json:"mib_per_sec"`
	TwoCopiesAfter    int     `json:"chunks_with_two_copies_after"`
	Unreadable        int     `json:"objects_unreadable_after"`
	LostReported      int     `json:"lost_chunks_reported"`
}

// copiesOf counts the workers that hold a physical copy of the chunk.
func (c *cluster) copiesOf(chunkID string) int {
	n := 0
	for _, w := range c.workers {
		if _, err := os.Stat(w.chunkPath(chunkID)); err == nil {
			n++
		}
	}
	return n
}

// fillWorker uploads single-chunk objects until the named worker holds `want` chunks and returns the objects.
func fillWorker(tb testing.TB, c *cluster, worker string, want, size int) []objectRec {
	tb.Helper()
	w := c.node(worker)
	var objs []objectRec
	held := 0
	for i := 0; held < want; i++ {
		if i > 40*want+100 {
			tb.Fatalf("worker %s only reached %d of %d chunks", worker, held, want)
		}
		data := make([]byte, size)
		_, _ = rand.Read(data)
		id, err := c.uploadBytes(fmt.Sprintf("r%d", i), data)
		if err != nil {
			tb.Fatal(err)
		}
		objs = append(objs, objectRec{id: id, sum: sha256.Sum256(data)})
		if _, err := os.Stat(w.chunkPath(c.objectChunks(id)[0].ChunkID)); err == nil {
			held++
		}
	}
	return objs
}

// wipe empties a worker's chunk directory, as a restarted worker with a fresh disk would be.
func wipe(tb testing.TB, n *node) []string {
	tb.Helper()
	ids := n.chunkIDs()
	for _, id := range ids {
		if err := os.Remove(n.chunkPath(id)); err != nil {
			tb.Fatal(err)
		}
	}
	return ids
}

// runHeal loses `lost` chunks on worker2, waits without a reconciler, then times one reconciler pass.
func runHeal(tb testing.TB, lost, size int, wait time.Duration) healResult {
	tb.Helper()
	c := newCluster(tb, 4, true)
	objs := fillWorker(tb, c, "worker2", lost, size)
	ids := wipe(tb, c.node("worker2"))
	require.Len(tb, ids, lost)
	res := healResult{LostChunks: len(ids), ChunkKiB: size >> 10, WaitMs: ms(wait)}
	for _, id := range ids {
		if c.copiesOf(id) == 1 {
			res.SingleCopyBefore++
		}
	}
	time.Sleep(wait)
	for _, id := range ids {
		if c.copiesOf(id) == 1 {
			res.SingleCopyNoRecon++
		}
	}
	rec, err := gc.NewReconciler(c.meta, c.pool, c.table, gc.NewCollector(c.meta, c.pool, c.table), gc.Config{Interval: time.Hour, StagedTimeout: time.Hour, OrphanGrace: time.Hour})
	require.NoError(tb, err)
	start := time.Now()
	rep := rec.Pass(context.Background())
	d := time.Since(start)
	res.PassMs = ms(d)
	res.Restored, res.LostReported = rep.ReplicasRestored, rep.LostChunks
	res.ChunksPerSec = float64(res.Restored) / d.Seconds()
	res.MiBPerSec = float64(res.Restored*size) / float64(1<<20) / d.Seconds()
	for _, id := range ids {
		if c.copiesOf(id) == 2 {
			res.TwoCopiesAfter++
		}
	}
	for _, o := range objs {
		if code, body, _ := c.download(o.id); code != http.StatusOK || sha256.Sum256(body) != o.sum {
			res.Unreadable++
		}
	}
	return res
}

func BenchmarkReconciler_TimeToHeal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var runs []healResult
		for _, n := range []int{10, 100, 500} {
			r := runHeal(b, n, 64<<10, 3*time.Second)
			runs = append(runs, r)
			b.ReportMetric(r.PassMs, fmt.Sprintf("pass_ms_%dchunks", n))
			b.ReportMetric(r.ChunksPerSec, fmt.Sprintf("chunks_per_s_%d", n))
		}
		r := runHeal(b, 100, 1<<20, 3*time.Second)
		runs = append(runs, r)
		b.ReportMetric(r.MiBPerSec, "mib_per_s_100x1MiB")
		if out := os.Getenv("BENCH_OUT"); out != "" {
			if err := writeJSON(filepath.Join(out, "reconciler.json"), runs); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestHealRestoresEveryLostChunkAndLeavesBaselineAtOneCopy(t *testing.T) {
	r := runHeal(t, 12, 16<<10, 200*time.Millisecond)
	assert.Equal(t, 12, r.SingleCopyBefore)
	assert.Equal(t, 12, r.SingleCopyNoRecon, "nothing heals without the reconciler")
	assert.Equal(t, 12, r.Restored)
	assert.Equal(t, 12, r.TwoCopiesAfter)
	assert.Zero(t, r.Unreadable)
	assert.Zero(t, r.LostReported)
}

func TestSecondPassAfterHealRestoresNothing(t *testing.T) {
	c := newCluster(t, 4, true)
	fillWorker(t, c, "worker2", 5, 8<<10)
	wipe(t, c.node("worker2"))
	rec, err := gc.NewReconciler(c.meta, c.pool, c.table, gc.NewCollector(c.meta, c.pool, c.table), gc.Config{Interval: time.Hour, StagedTimeout: time.Hour, OrphanGrace: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, 5, rec.Pass(context.Background()).ReplicasRestored)
	assert.Zero(t, rec.Pass(context.Background()).ReplicasRestored)
}
