package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const vnodesPerWorker = 50

type vnode struct {
	hash     uint32
	workerID string
}

// Ring is a consistent hash ring with virtual nodes.
// All methods are goroutine-safe.
type Ring struct {
	mu      sync.RWMutex
	vnodes  []vnode          // sorted by hash
	workers map[string]string // workerID → address
}

func NewRing() *Ring {
	return &Ring{
		workers: make(map[string]string),
	}
}

// AddWorker adds a worker and its 50 virtual nodes to the ring.
// No-op if the worker is already present.
func (r *Ring) AddWorker(workerID, address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.workers[workerID]; exists {
		return
	}
	r.workers[workerID] = address

	for i := 0; i < vnodesPerWorker; i++ {
		h := vnodeHash(workerID, i)
		r.vnodes = append(r.vnodes, vnode{hash: h, workerID: workerID})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
}

// RemoveWorker removes a worker and all its virtual nodes from the ring.
func (r *Ring) RemoveWorker(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.workers, workerID)

	filtered := r.vnodes[:0]
	for _, v := range r.vnodes {
		if v.workerID != workerID {
			filtered = append(filtered, v)
		}
	}
	r.vnodes = filtered
}

// Workers returns the IDs of all workers currently in the ring.
func (r *Ring) Workers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.workers))
	for id := range r.workers {
		ids = append(ids, id)
	}
	return ids
}

// lookup finds the first clockwise vnode for the given key,
// skipping vnodes whose workerID is in the exclude set.
// Returns empty string if no suitable vnode is found.
func (r *Ring) lookup(key string, exclude map[string]struct{}) string {
	if len(r.vnodes) == 0 {
		return ""
	}

	h := keyHash(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})

	// Walk clockwise from idx, wrapping around once.
	n := len(r.vnodes)
	for i := 0; i < n; i++ {
		v := r.vnodes[(idx+i)%n]
		if _, excluded := exclude[v.workerID]; !excluded {
			return v.workerID
		}
	}
	return ""
}

// vnodeHash produces a stable uint32 for a worker's i-th virtual node.
func vnodeHash(workerID string, i int) uint32 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", workerID, i)))
	return binary.BigEndian.Uint32(h[:4])
}

// keyHash produces a uint32 for a chunk key.
func keyHash(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}
