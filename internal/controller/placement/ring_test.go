package placement

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func fourWorkerRing() *Ring {
	r := NewRing()
	r.AddWorker("w1", "localhost:9001")
	r.AddWorker("w2", "localhost:9002")
	r.AddWorker("w3", "localhost:9003")
	r.AddWorker("w4", "localhost:9004")
	return r
}

// --- Basic placement ---

func TestPrimary_ReturnsWorker(t *testing.T) {
	r := fourWorkerRing()
	primary, err := r.Primary("chunk-abc")
	require.NoError(t, err)
	assert.NotEmpty(t, primary)
}

func TestSecondary_DifferentFromPrimary(t *testing.T) {
	r := fourWorkerRing()
	primary, err := r.Primary("chunk-abc")
	require.NoError(t, err)

	secondary, err := r.Secondary("chunk-abc", primary)
	require.NoError(t, err)
	assert.NotEqual(t, primary, secondary)
}

// --- Determinism ---

func TestPlacement_IsDeterministic(t *testing.T) {
	r := fourWorkerRing()
	primary1, _ := r.Primary("chunk-xyz")
	primary2, _ := r.Primary("chunk-xyz")
	assert.Equal(t, primary1, primary2)

	secondary1, _ := r.Secondary("chunk-xyz", primary1)
	secondary2, _ := r.Secondary("chunk-xyz", primary1)
	assert.Equal(t, secondary1, secondary2)
}

// --- Primary != Secondary for 100 chunk IDs ---

func TestPlacement_PrimaryNotEqualSecondary_100Chunks(t *testing.T) {
	r := fourWorkerRing()
	for i := 0; i < 100; i++ {
		chunkID := fmt.Sprintf("chunk-%04d", i)
		primary, err := r.Primary(chunkID)
		require.NoError(t, err)
		secondary, err := r.Secondary(chunkID, primary)
		require.NoError(t, err)
		assert.NotEqualf(t, primary, secondary,
			"chunk %s: primary and secondary must be on different workers", chunkID)
	}
}

// --- Load distribution ---

func TestPlacement_LoadDistribution(t *testing.T) {
	r := fourWorkerRing()
	counts := make(map[string]int)

	for i := 0; i < 1000; i++ {
		primary, err := r.Primary(fmt.Sprintf("chunk-%04d", i))
		require.NoError(t, err)
		counts[primary]++
	}

	// Each worker should get between 15% and 35% of chunks (expected ~25%).
	// CV ≈ 1/√50 ≈ 14% — allow generous bounds for unit test stability.
	for workerID, count := range counts {
		pct := float64(count) / 1000.0 * 100
		assert.Greaterf(t, pct, 15.0, "worker %s under-loaded: %.1f%%", workerID, pct)
		assert.Lessf(t, pct, 35.0, "worker %s over-loaded: %.1f%%", workerID, pct)
	}
}

// --- AddWorker / RemoveWorker ---

func TestAddWorker_Idempotent(t *testing.T) {
	r := NewRing()
	r.AddWorker("w1", "localhost:9001")
	r.AddWorker("w1", "localhost:9001") // duplicate

	assert.Len(t, r.vnodes, vnodesPerWorker, "duplicate AddWorker must not add extra vnodes")
}

func TestRemoveWorker(t *testing.T) {
	r := fourWorkerRing()
	r.RemoveWorker("w1")

	workers := r.Workers()
	assert.Len(t, workers, 3)
	for _, w := range workers {
		assert.NotEqual(t, "w1", w)
	}
	assert.Len(t, r.vnodes, 3*vnodesPerWorker)
}

func TestRemoveWorker_PlacementNeverReturnsRemovedWorker(t *testing.T) {
	r := fourWorkerRing()
	r.RemoveWorker("w2")

	for i := 0; i < 200; i++ {
		primary, err := r.Primary(fmt.Sprintf("chunk-%04d", i))
		require.NoError(t, err)
		assert.NotEqual(t, "w2", primary)

		secondary, err := r.Secondary(fmt.Sprintf("chunk-%04d", i), primary)
		require.NoError(t, err)
		assert.NotEqual(t, "w2", secondary)
	}
}

// --- Error cases ---

func TestPrimary_EmptyRing(t *testing.T) {
	r := NewRing()
	_, err := r.Primary("chunk-abc")
	assert.ErrorIs(t, err, models.ErrNoWorkers)
}

func TestSecondary_OnlyOneWorker(t *testing.T) {
	r := NewRing()
	r.AddWorker("w1", "localhost:9001")
	_, err := r.Secondary("chunk-abc", "w1")
	assert.ErrorIs(t, err, models.ErrNoWorkers)
}

func TestWorkers_ReturnsAll(t *testing.T) {
	r := fourWorkerRing()
	workers := r.Workers()
	assert.Len(t, workers, 4)
}

// --- introspection used by the web UI ---

func TestSnapshotListsEveryVnodeSortedWithItsIndex(t *testing.T) {
	r := NewRing()
	for _, id := range []string{"w1", "w2", "w3"} {
		r.AddWorker(id, id+":1")
	}
	pts := r.Snapshot()
	require.Len(t, pts, 3*vnodesPerWorker)
	for i := 1; i < len(pts); i++ {
		assert.LessOrEqual(t, pts[i-1].Hash, pts[i].Hash)
	}
	per := map[string]map[int]bool{}
	for _, p := range pts {
		if per[p.WorkerID] == nil {
			per[p.WorkerID] = map[int]bool{}
		}
		per[p.WorkerID][p.Index] = true
		assert.Equal(t, vnodeHash(p.WorkerID, p.Index), p.Hash, "the index identifies the hash")
	}
	for _, id := range []string{"w1", "w2", "w3"} {
		assert.Len(t, per[id], vnodesPerWorker)
	}
}

func TestSnapshotIsACopyAndEmptyRingGivesEmpty(t *testing.T) {
	r := NewRing()
	assert.Empty(t, r.Snapshot())
	r.AddWorker("w1", "x")
	pts := r.Snapshot()
	pts[0].WorkerID = "mutated"
	assert.NotEqual(t, "mutated", r.Snapshot()[0].WorkerID)
	r.RemoveWorker("w1")
	assert.Empty(t, r.Snapshot())
}

func TestVnodePointsWorkWithoutMembership(t *testing.T) {
	pts := VnodePoints("ghost")
	require.Len(t, pts, vnodesPerWorker)
	for i := 1; i < len(pts); i++ {
		assert.LessOrEqual(t, pts[i-1].Hash, pts[i].Hash)
	}
	r := NewRing()
	r.AddWorker("ghost", "x")
	assert.Equal(t, pts, r.Snapshot(), "the same points the ring would hold")
	assert.Equal(t, vnodesPerWorker, VnodesPerWorker())
}

func TestKeyHashMatchesPlacementInput(t *testing.T) {
	assert.Equal(t, keyHash("abc"), KeyHash("abc"))
	assert.NotEqual(t, KeyHash("abc"), KeyHash("abd"))
}

func TestLocateAgreesWithPrimaryAndSecondary(t *testing.T) {
	r := NewRing()
	for _, id := range []string{"w1", "w2", "w3", "w4"} {
		r.AddWorker(id, id+":1")
	}
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("chunk-%d", i)
		w := r.Locate(key)
		p, err := r.Primary(key)
		require.NoError(t, err)
		s, err := r.Secondary(key, p)
		require.NoError(t, err)
		assert.Equal(t, p, w.Primary, key)
		assert.Equal(t, s, w.Secondary, key)
		assert.Equal(t, KeyHash(key), w.KeyHash)
		require.NotEmpty(t, w.Steps)
		assert.Equal(t, p, w.Steps[0].WorkerID)
		assert.Equal(t, s, w.Steps[len(w.Steps)-1].WorkerID, "the walk ends on the secondary")
		for _, st := range w.Steps[1 : len(w.Steps)-1] {
			assert.Equal(t, p, st.WorkerID, "every vnode passed on the way belongs to the primary")
		}
	}
}

func TestLocateWrapsAroundTheRing(t *testing.T) {
	r := NewRing()
	r.AddWorker("w1", "x")
	r.AddWorker("w2", "x")
	pts := r.Snapshot()
	last := pts[len(pts)-1]
	var key string
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("wrap-%d", i)
		if KeyHash(k) > last.Hash {
			key = k
			break
		}
	}
	require.NotEmpty(t, key, "need a key hashing past the last vnode")
	w := r.Locate(key)
	assert.Equal(t, pts[0].Hash, w.Steps[0].Hash, "past the last vnode the walk wraps to the first")
}

func TestLocateEdgeCases(t *testing.T) {
	r := NewRing()
	w := r.Locate("k")
	assert.Empty(t, w.Steps)
	assert.Equal(t, "", w.Primary)
	assert.Equal(t, KeyHash("k"), w.KeyHash)

	r.AddWorker("only", "x")
	w = r.Locate("k")
	assert.Equal(t, "only", w.Primary)
	assert.Equal(t, "", w.Secondary, "one worker cannot hold a second copy")
	assert.Len(t, w.Steps, vnodesPerWorker, "the walk went all the way round looking for a second worker")
}
