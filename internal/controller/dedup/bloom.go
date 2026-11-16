// Package dedup provides an in-memory bloom filter for O(1) chunk existence checks.
// It wraps github.com/bits-and-blooms/bloom/v3 and implements models.ExistenceFilter.
//
// False positives are possible (MightExist returns true for absent chunks) but false
// negatives are impossible — a chunk that was Add()ed will always pass MightExist.
// The upload path uses it to skip a bbolt round-trip for chunks that are definitely new.
package dedup

import (
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

// Filter is a thread-safe bloom filter.
type Filter struct {
	mu sync.RWMutex
	bf *bloom.BloomFilter
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
