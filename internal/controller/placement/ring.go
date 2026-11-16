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
	index    int
}

// Ring is a consistent hash ring with virtual nodes.
// All methods are goroutine-safe.
type Ring struct {
	mu      sync.RWMutex
	vnodes  []vnode           // sorted by hash
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
		r.vnodes = append(r.vnodes, vnode{hash: h, workerID: workerID, index: i})
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

// Point is one virtual node's position on the ring.
type Point struct {
	Hash     uint32 `json:"hash"`
	WorkerID string `json:"worker"`
	Index    int    `json:"index"`
}

// Walk is the clockwise search that places one key: the hash it starts from and every virtual node passed until two distinct workers were found.
type Walk struct {
	KeyHash   uint32  `json:"key_hash"`
	Steps     []Point `json:"steps"`
	Primary   string  `json:"primary"`
	Secondary string  `json:"secondary"`
}

// VnodesPerWorker is how many virtual nodes each worker places on the ring.
func VnodesPerWorker() int { return vnodesPerWorker }

// KeyHash is the ring position a key (a chunk ID) hashes to.
func KeyHash(key string) uint32 { return keyHash(key) }

// VnodePoints returns the virtual nodes a worker places, or would place, on the ring, sorted by position; it works for workers that are not currently members.
func VnodePoints(workerID string) []Point {
	pts := make([]Point, vnodesPerWorker)
	for i := range pts {
		pts[i] = Point{Hash: vnodeHash(workerID, i), WorkerID: workerID, Index: i}
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].Hash < pts[j].Hash })
	return pts
}

// Snapshot returns every virtual node currently on the ring, sorted by position.
func (r *Ring) Snapshot() []Point {
	r.mu.RLock()
	defer r.mu.RUnlock()
	pts := make([]Point, len(r.vnodes))
	for i, v := range r.vnodes {
		pts[i] = Point{Hash: v.hash, WorkerID: v.workerID, Index: v.index}
	}
	return pts
}

// Locate replays how key is placed: from its hash clockwise, the first virtual node is the primary and the next one on a different worker is the secondary.
func (r *Ring) Locate(key string) Walk {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w := Walk{KeyHash: keyHash(key), Steps: []Point{}}
	n := len(r.vnodes)
	if n == 0 {
		return w
	}
	idx := sort.Search(n, func(i int) bool { return r.vnodes[i].hash >= w.KeyHash })
	for i := 0; i < n; i++ {
		v := r.vnodes[(idx+i)%n]
		w.Steps = append(w.Steps, Point{Hash: v.hash, WorkerID: v.workerID, Index: v.index})
		if w.Primary == "" {
			w.Primary = v.workerID
			continue
		}
		if v.workerID != w.Primary {
			w.Secondary = v.workerID
			break
		}
	}
	return w
}
