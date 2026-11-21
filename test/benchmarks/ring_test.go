package benchmarks

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/placement"
)

type moveResult struct {
	From            int     `json:"workers_before"`
	To              int     `json:"workers_after"`
	Chunks          int     `json:"chunks"`
	RingMoved       int     `json:"ring_primary_moved"`
	RingPct         float64 `json:"ring_moved_pct"`
	ModuloMoved     int     `json:"modulo_primary_moved"`
	ModuloPct       float64 `json:"modulo_moved_pct"`
	IdealPct        float64 `json:"ideal_pct"`
	RingPairMovedPc float64 `json:"ring_primary_or_secondary_changed_pct"`
}

func chunkKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		sum := sha256.Sum256([]byte(fmt.Sprintf("chunk-%d", i)))
		keys[i] = hex.EncodeToString(sum[:])
	}
	return keys
}

func ringOf(n int) *placement.Ring {
	r := placement.NewRing()
	for i := 1; i <= n; i++ {
		r.AddWorker(fmt.Sprintf("worker%d", i), fmt.Sprintf("10.0.0.%d:8080", i))
	}
	return r
}

// moduloOwner is the naive placement: first 8 bytes of the chunk hash modulo the worker count.
func moduloOwner(key string, n int) int {
	raw, _ := hex.DecodeString(key)
	return int(binary.BigEndian.Uint64(raw[:8]) % uint64(n))
}

func measureMove(keys []string, from, to int) moveResult {
	before, after := ringOf(from), ringOf(to)
	res := moveResult{From: from, To: to, Chunks: len(keys), IdealPct: 100 * float64(to-from) / float64(to)}
	pair := 0
	for _, k := range keys {
		wb, wa := before.Locate(k), after.Locate(k)
		if wb.Primary != wa.Primary {
			res.RingMoved++
		}
		if wb.Primary != wa.Primary || wb.Secondary != wa.Secondary {
			pair++
		}
		if moduloOwner(k, from) != moduloOwner(k, to) {
			res.ModuloMoved++
		}
	}
	res.RingPct = 100 * float64(res.RingMoved) / float64(len(keys))
	res.ModuloPct = 100 * float64(res.ModuloMoved) / float64(len(keys))
	res.RingPairMovedPc = 100 * float64(pair) / float64(len(keys))
	return res
}

type spread struct {
	Workers int     `json:"workers"`
	Chunks  int     `json:"chunks"`
	MinPct  float64 `json:"min_share_pct"`
	MaxPct  float64 `json:"max_share_pct"`
	FairPct float64 `json:"fair_share_pct"`
	CV      float64 `json:"coefficient_of_variation_pct"`
}

func measureSpread(keys []string, n int) spread {
	r := ringOf(n)
	count := map[string]int{}
	for _, k := range keys {
		count[r.Locate(k).Primary]++
	}
	var shares []float64
	for i := 1; i <= n; i++ {
		shares = append(shares, 100*float64(count[fmt.Sprintf("worker%d", i)])/float64(len(keys)))
	}
	sort.Float64s(shares)
	mean := 100 / float64(n)
	var ss float64
	for _, s := range shares {
		ss += (s - mean) * (s - mean)
	}
	return spread{Workers: n, Chunks: len(keys), MinPct: shares[0], MaxPct: shares[n-1], FairPct: mean, CV: 100 * math.Sqrt(ss/float64(n)) / mean}
}

func saltedKeys(n int, salt string) []string {
	keys := make([]string, n)
	for i := range keys {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s-chunk-%d", salt, i)))
		keys[i] = hex.EncodeToString(sum[:])
	}
	return keys
}

type movementReport struct {
	From, To    int
	Ideal       float64 `json:"ideal_pct"`
	RingPct     Spread  `json:"ring_primary_moved_pct"`
	ModuloPct   Spread  `json:"modulo_primary_moved_pct"`
	RingPairPct Spread  `json:"ring_primary_or_secondary_changed_pct"`
	RingMoved   Spread  `json:"ring_chunks_moved"`
	ModuloMoved Spread  `json:"modulo_chunks_moved"`
}

type spreadReport struct {
	Workers int     `json:"workers"`
	FairPct float64 `json:"fair_share_pct"`
	MinPct  Spread  `json:"min_share_pct"`
	MaxPct  Spread  `json:"max_share_pct"`
	CVPct   Spread  `json:"coefficient_of_variation_pct"`
}

// BenchmarkRing_ChunkMovement counts how many of 10,000 chunks change primary worker when one worker is added.
// It is pure computation, so a repetition is a different set of 10,000 chunk ids rather than a re-run of the same one, and there is no warm-up.
func BenchmarkRing_ChunkMovement(b *testing.B) {
	pairs := [][2]int{{4, 5}, {9, 10}, {19, 20}, {39, 40}}
	sizes := []int{4, 10, 20}
	for n := 0; n < b.N; n++ {
		moves := make([][]moveResult, len(pairs))
		spreads := make([][]spread, len(sizes))
		for rep := 0; rep < repCount(); rep++ {
			keys := saltedKeys(10000, fmt.Sprintf("set%d", rep))
			for k, p := range pairs {
				moves[k] = append(moves[k], measureMove(keys, p[0], p[1]))
			}
			for k, w := range sizes {
				spreads[k] = append(spreads[k], measureSpread(keys, w))
			}
		}
		var out struct {
			Movement []movementReport `json:"movement"`
			Balance  []spreadReport   `json:"balance"`
		}
		for k, p := range pairs {
			m := moves[k]
			r := movementReport{From: p[0], To: p[1], Ideal: m[0].IdealPct,
				RingPct:     spreadOf(col(m, func(x moveResult) float64 { return x.RingPct })),
				ModuloPct:   spreadOf(col(m, func(x moveResult) float64 { return x.ModuloPct })),
				RingPairPct: spreadOf(col(m, func(x moveResult) float64 { return x.RingPairMovedPc })),
				RingMoved:   spreadOf(col(m, func(x moveResult) float64 { return float64(x.RingMoved) })),
				ModuloMoved: spreadOf(col(m, func(x moveResult) float64 { return float64(x.ModuloMoved) })),
			}
			out.Movement = append(out.Movement, r)
			b.ReportMetric(r.RingPct.Median, fmt.Sprintf("ring_moved_pct_%d_to_%d", p[0], p[1]))
			b.ReportMetric(r.ModuloPct.Median, fmt.Sprintf("modulo_moved_pct_%d_to_%d", p[0], p[1]))
		}
		for k, w := range sizes {
			sp := spreads[k]
			out.Balance = append(out.Balance, spreadReport{Workers: w, FairPct: sp[0].FairPct,
				MinPct: spreadOf(col(sp, func(x spread) float64 { return x.MinPct })),
				MaxPct: spreadOf(col(sp, func(x spread) float64 { return x.MaxPct })),
				CVPct:  spreadOf(col(sp, func(x spread) float64 { return x.CV })),
			})
		}
		saveReport(b, "ring", out)
	}
}

func TestMovementNeverExceedsTheModuloBaselineAndNothingMovesWhenSizeIsUnchanged(t *testing.T) {
	keys := chunkKeys(2000)
	same := measureMove(keys, 4, 4)
	assert.Zero(t, same.RingMoved)
	assert.Zero(t, same.ModuloMoved)
	r := measureMove(keys, 4, 5)
	assert.Less(t, r.RingMoved, r.ModuloMoved)
	assert.Greater(t, r.RingMoved, 0)
}

func TestModuloOwnerIsStableAndInRange(t *testing.T) {
	k := chunkKeys(1)[0]
	require.Equal(t, moduloOwner(k, 7), moduloOwner(k, 7))
	for _, key := range chunkKeys(200) {
		o := moduloOwner(key, 5)
		assert.True(t, o >= 0 && o < 5)
	}
}

func TestEveryChunkMovingOnTheRingMovesToTheNewWorker(t *testing.T) {
	before, after := ringOf(4), ringOf(5)
	for _, k := range chunkKeys(2000) {
		if b, a := before.Locate(k).Primary, after.Locate(k).Primary; b != a {
			assert.Equal(t, "worker5", a, "consistent hashing only moves chunks onto the added worker")
		}
	}
}
