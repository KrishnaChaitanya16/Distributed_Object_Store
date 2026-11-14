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
