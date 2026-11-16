package dedup

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoFalseNegatives(t *testing.T) {
	f := New(1000, 0.01)
	for i := 0; i < 500; i++ {
		f.Add(fmt.Sprintf("chunk-%d", i))
	}
	for i := 0; i < 500; i++ {
		require.True(t, f.MightExist(fmt.Sprintf("chunk-%d", i)))
	}
}

func TestObserveCountsEveryAnswer(t *testing.T) {
	f := New(1000, 0.01)
	f.Add("known")
	assert.True(t, f.Observe("known"))
	assert.False(t, f.Observe("fresh-1"))
	assert.False(t, f.Observe("fresh-2"))
	assert.Equal(t, Counters{Queries: 3, Maybe: 1, DefinitelyNew: 2}, f.Counters())
}

func TestRecordOutcomeOnlyJudgesMaybeAnswers(t *testing.T) {
	f := New(1000, 0.01)
	f.RecordOutcome(false, false)
	f.RecordOutcome(false, true)
	assert.Equal(t, Counters{}, f.Counters(), "a definite answer needs no judging")
	f.RecordOutcome(true, true)
	f.RecordOutcome(true, true)
	f.RecordOutcome(true, false)
	c := f.Counters()
	assert.Equal(t, uint64(2), c.TruePositive)
	assert.Equal(t, uint64(1), c.FalsePositive)
}

func TestStatsOfAnEmptyFilter(t *testing.T) {
	f := New(1000, 0.01)
	st := f.Stats(64)
	assert.Zero(t, st.SetBits)
	assert.Zero(t, st.FillRatio)
	assert.Zero(t, st.EstimatedFP)
	assert.Zero(t, st.Items)
	assert.Greater(t, st.Bits, uint(0))
	assert.Greater(t, st.HashFuncs, uint(0))
	assert.Len(t, st.Cells, 64)
	for _, c := range st.Cells {
		assert.Zero(t, c)
	}
}

func TestStatsCellsSumToSetBitsAndFillGrows(t *testing.T) {
	f := New(1000, 0.01)
	var prev float64
	for i := 0; i < 400; i++ {
		f.Add(fmt.Sprintf("c-%d", i))
		if i%100 == 99 {
			st := f.Stats(100)
			var sum uint
			for _, c := range st.Cells {
				sum += c
			}
			assert.Equal(t, st.SetBits, sum, "folding loses no bits")
			assert.Greater(t, st.FillRatio, prev)
			assert.InDelta(t, float64(i+1), float64(st.Items), float64(i+1)*0.1, "the item estimate tracks the real count")
			prev = st.FillRatio
		}
	}
	st := f.Stats(100)
	assert.Greater(t, st.EstimatedFP, 0.0)
	assert.Less(t, st.EstimatedFP, 0.05, "well under capacity the false-positive rate stays near the design value")
}

func TestStatsCellCountEdgeCases(t *testing.T) {
	f := New(1000, 0.01)
	f.Add("x")
	assert.Len(t, f.Stats(0).Cells, 1)
	assert.Len(t, f.Stats(-3).Cells, 1)
	st := f.Stats(1 << 30)
	assert.LessOrEqual(t, uint(len(st.Cells)), st.Bits, "never more cells than bits")
	assert.Equal(t, uint(1), st.BitsPerCell)
	one := f.Stats(1)
	assert.Equal(t, one.SetBits, one.Cells[0])
}

func TestProbeShowsTheBitsAChunkSets(t *testing.T) {
	f := New(1000, 0.01)
	before := f.Probe("chunk-a", 128)
	assert.False(t, before.MightExist)
	require.Len(t, before.Positions, int(f.Stats(1).HashFuncs))
	for _, s := range before.Set {
		assert.False(t, s, "an empty filter has no bit set")
	}
	f.Add("chunk-a")
	after := f.Probe("chunk-a", 128)
	assert.True(t, after.MightExist)
	assert.Equal(t, before.Positions, after.Positions, "positions depend only on the chunk id")
	for _, s := range after.Set {
		assert.True(t, s, "adding a chunk sets all its bits")
	}
	st := f.Stats(128)
	for i, c := range after.Cells {
		assert.Greater(t, st.Cells[c], uint(0), "bit %d lies in a cell that is lit", i)
		assert.Equal(t, int(after.Positions[i]/st.BitsPerCell), c)
	}
}

func TestProbeOfAbsentChunkCanShowPartialBits(t *testing.T) {
	f := New(200, 0.01)
	for i := 0; i < 150; i++ {
		f.Add(fmt.Sprintf("c-%d", i))
	}
	partial := false
	for i := 0; i < 2000 && !partial; i++ {
		p := f.Probe(fmt.Sprintf("absent-%d", i), 64)
		if p.MightExist {
			continue
		}
		some, all := false, true
		for _, s := range p.Set {
			some = some || s
			all = all && s
		}
		partial = some && !all
	}
	assert.True(t, partial, "an absent chunk usually hits some set bits but not all of them")
}

func TestProbeAgreesWithMightExist(t *testing.T) {
	f := New(500, 0.01)
	for i := 0; i < 300; i++ {
		f.Add(fmt.Sprintf("c-%d", i))
	}
	for i := 0; i < 600; i++ {
		id := fmt.Sprintf("c-%d", i)
		p := f.Probe(id, 32)
		all := true
		for _, s := range p.Set {
			all = all && s
		}
		assert.Equal(t, f.MightExist(id), p.MightExist, id)
		assert.Equal(t, p.MightExist, all, id)
	}
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	f := New(10000, 0.01)
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				id := fmt.Sprintf("g%d-%d", g, i)
				m := f.Observe(id)
				f.RecordOutcome(m, false)
				f.Add(id)
				f.Stats(32)
				f.Probe(id, 32)
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, uint64(1800), f.Counters().Queries)
}
