// Package dedup provides an in-memory bloom filter for O(1) chunk existence checks.
// It wraps github.com/bits-and-blooms/bloom/v3 and implements models.ExistenceFilter.
//
// False positives are possible (MightExist returns true for absent chunks) but false
// negatives are impossible — a chunk that was Add()ed will always pass MightExist.
// The upload path uses it to skip a bbolt round-trip for chunks that are definitely new.
package dedup

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
)

// Filter is a thread-safe bloom filter.
type Filter struct {
	mu sync.RWMutex
	bf *bloom.BloomFilter

	queries, maybe, definitelyNew, truePositive, falsePositive atomic.Uint64
}

// New creates a Filter sized for n expected elements at p false-positive rate.
// Reasonable defaults: n=1_000_000, p=0.01 (1 %).
func New(n uint, p float64) *Filter {
	return &Filter{bf: bloom.NewWithEstimates(n, p)}
}

// MightExist returns true if chunkID was possibly Add()ed, false if definitely not.
func (f *Filter) MightExist(chunkID string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.bf.TestString(chunkID)
}

// Add records chunkID in the filter.
func (f *Filter) Add(chunkID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bf.AddString(chunkID)
}

// Counters describe how the filter has been used since start.
type Counters struct {
	Queries       uint64 `json:"queries"`
	Maybe         uint64 `json:"maybe"`
	DefinitelyNew uint64 `json:"definitely_new"`
	TruePositive  uint64 `json:"true_positive"`
	FalsePositive uint64 `json:"false_positive"`
}

// Stats describes the filter's size and how full it is.
type Stats struct {
	Bits        uint     `json:"bits"`
	HashFuncs   uint     `json:"hash_funcs"`
	Items       uint32   `json:"items"`
	SetBits     uint     `json:"set_bits"`
	FillRatio   float64  `json:"fill_ratio"`
	EstimatedFP float64  `json:"estimated_false_positive"`
	Cells       []uint   `json:"cells"`
	BitsPerCell uint     `json:"bits_per_cell"`
	Counters    Counters `json:"counters"`
}

// Probe shows where a chunk ID lands in the filter: the bit positions its hash functions pick and whether each is set.
type Probe struct {
	ChunkID    string `json:"chunk_id"`
	Positions  []uint `json:"positions"`
	Set        []bool `json:"set"`
	Cells      []int  `json:"cells"`
	MightExist bool   `json:"might_exist"`
}

// Observe tests chunkID like MightExist and also records the answer in the counters.
func (f *Filter) Observe(chunkID string) bool {
	maybe := f.MightExist(chunkID)
	f.queries.Add(1)
	if maybe {
		f.maybe.Add(1)
	} else {
		f.definitelyNew.Add(1)
	}
	return maybe
}

// RecordOutcome notes whether a "maybe" answer was right: existed says whether the authoritative store really had the chunk.
func (f *Filter) RecordOutcome(maybe, existed bool) {
	if !maybe {
		return
	}
	if existed {
		f.truePositive.Add(1)
	} else {
		f.falsePositive.Add(1)
	}
}

// Counters returns the usage counters.
func (f *Filter) Counters() Counters {
	return Counters{
		Queries:       f.queries.Load(),
		Maybe:         f.maybe.Load(),
		DefinitelyNew: f.definitelyNew.Load(),
		TruePositive:  f.truePositive.Load(),
		FalsePositive: f.falsePositive.Load(),
	}
}

// Stats summarises the filter; the bit array is folded into at most cells groups, each reported as its number of set bits.
func (f *Filter) Stats(cells int) Stats {
	f.mu.RLock()
	defer f.mu.RUnlock()
	m, k := f.bf.Cap(), f.bf.K()
	bits := f.bf.BitSet()
	set := bits.Count()
	st := Stats{Bits: m, HashFuncs: k, Items: f.bf.ApproximatedSize(), SetBits: set, Counters: f.Counters()}
	if m > 0 {
		st.FillRatio = float64(set) / float64(m)
		st.EstimatedFP = math.Pow(st.FillRatio, float64(k))
	}
	if cells < 1 {
		cells = 1
	}
	if uint(cells) > m {
		cells = int(m)
	}
	if cells == 0 {
		return st
	}
	per := (m + uint(cells) - 1) / uint(cells)
	st.BitsPerCell = per
	st.Cells = make([]uint, cells)
	for i := uint(0); i < m; i++ {
		if bits.Test(i) {
			st.Cells[i/per]++
		}
	}
	return st
}

// Probe reports the bit positions chunkID hashes to and whether the filter has them all set; cells is the folding Stats would use.
func (f *Filter) Probe(chunkID string, cells int) Probe {
	f.mu.RLock()
	defer f.mu.RUnlock()
	m, k := f.bf.Cap(), f.bf.K()
	p := Probe{ChunkID: chunkID, MightExist: f.bf.TestString(chunkID)}
	if m == 0 {
		return p
	}
	if cells < 1 {
		cells = 1
	}
	if uint(cells) > m {
		cells = int(m)
	}
	per := (m + uint(cells) - 1) / uint(cells)
	for _, loc := range bloom.Locations([]byte(chunkID), k) {
		pos := uint(loc % uint64(m))
		p.Positions = append(p.Positions, pos)
		p.Set = append(p.Set, f.bf.BitSet().Test(pos))
		p.Cells = append(p.Cells, int(pos/per))
	}
	return p
}
