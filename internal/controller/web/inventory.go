package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// workerInv is what one online worker reports about itself.
type workerInv struct {
	Chunks   map[string]bool
	OK       bool
	Error    string
	Scrubber map[string]any
}

// inventory is a point-in-time view of every online worker's disk.
type inventory struct {
	At      time.Time
	Workers map[string]*workerInv
}

// inventoryCache asks the workers what they hold, at most once per ttl, so many browsers polling never multiply the load.
type inventoryCache struct {
	s   *Server
	ttl time.Duration
	mu  sync.Mutex
	cur *inventory
}

func newInventoryCache(s *Server, ttl time.Duration) *inventoryCache {
	return &inventoryCache{s: s, ttl: ttl}
}

func (c *inventoryCache) get(ctx context.Context) *inventory {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur != nil && time.Since(c.cur.At) < c.ttl {
		return c.cur
	}
	c.cur = c.s.collectInventory(ctx)
	return c.cur
}

// forget drops the cached view so the next read sees the effect of a change the UI itself just made.
func (c *inventoryCache) forget() {
	c.mu.Lock()
	c.cur = nil
	c.mu.Unlock()
}

// collectInventory queries every worker the lease table considers ONLINE, in parallel; a worker that fails is recorded as such, never as empty.
func (s *Server) collectInventory(ctx context.Context) *inventory {
	inv := &inventory{At: time.Now(), Workers: map[string]*workerInv{}}
	records, err := s.d.Meta.ListWorkers(ctx)
	if err != nil {
		return inv
	}
	lease := s.d.Health.Snapshot()
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, w := range records {
		if lease[w.WorkerID].Status != models.WorkerOnline {
			continue
		}
		wg.Add(1)
		go func(w *models.WorkerRecord) {
			defer wg.Done()
			addr := fmt.Sprintf("%s:%d", w.Address, w.Port)
			wi := &workerInv{}
			cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
			defer cancel()
			ids, err := s.d.Workers.ListChunks(cctx, addr, w.WorkerID)
			if err != nil {
				wi.Error = err.Error()
			} else {
				wi.OK = true
				wi.Chunks = make(map[string]bool, len(ids))
				for _, id := range ids {
					wi.Chunks[id] = true
				}
			}
			wi.Scrubber = s.fetchScrubber(cctx, addr)
			mu.Lock()
			inv.Workers[w.WorkerID] = wi
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	return inv
}

// fetchScrubber reads a worker's scrubber status; nil if it has no scrubber or cannot be reached.
func (s *Server) fetchScrubber(ctx context.Context, addr string) map[string]any {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/internal/scrubber/status", nil)
	if err != nil {
		return nil
	}
	resp, err := s.d.HTTP.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16)) //nolint:errcheck
		return nil
	}
	var out map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil
	}
	return out
}
