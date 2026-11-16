// Package chunklock serialises controller jobs that move or delete copies of the same chunk.
package chunklock

import (
	"hash/fnv"
	"sync"
)

const stripes = 1024

var locks [stripes]sync.Mutex

// Lock blocks until the chunk's stripe is free and returns the matching unlock.
func Lock(chunkID string) (unlock func()) {
	mu := &locks[stripe(chunkID)]
	mu.Lock()
	return mu.Unlock
}

func stripe(chunkID string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(chunkID))
	return h.Sum32() % stripes
}
