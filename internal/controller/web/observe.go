package web

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// observer turns things the controller does not log itself into events: workers going up or down, and corruption the scrubbers found.
type observer struct {
	mu         sync.Mutex
	health     map[string]models.WorkerStatus
	scrubSeen  map[string]bool
	scrubKnown map[string]bool
}

func newObserver() *observer {
	return &observer{health: map[string]models.WorkerStatus{}, scrubSeen: map[string]bool{}, scrubKnown: map[string]bool{}}
}

// changesDisks lists the events after which what workers hold has changed, so a cached inventory is out of date.
var changesDisks = map[string]bool{
	"upload.durable": true, "upload.commit": true, "upload.fail": true, "object.delete": true,
	"rebalance.copy": true, "gc.restore": true, "gc.excess": true, "gc.chunk_deleted": true,
	"download.repair": true, "scrub.quarantine": true, "worker.online": true, "worker.offline": true,
}

// invalidateOnChanges drops the cached inventory whenever an event says the disks changed, so the UI never shows a fresh upload as missing.
func (s *Server) invalidateOnChanges(ctx context.Context) {
	wake, cancel := s.d.Bus.Subscribe()
	defer cancel()
	last := s.d.Bus.LastSeq()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			for _, ev := range s.d.Bus.Since(last, 0) {
				last = ev.Seq
				if changesDisks[ev.Kind] {
					s.inv.forget()
				}
			}
		}
	}
}

// Run watches worker health every 500ms and scrubber reports every 2s until ctx is cancelled.
func (s *Server) Run(ctx context.Context) {
	go s.invalidateOnChanges(ctx)
	obs := newObserver()
	healthTick := time.NewTicker(500 * time.Millisecond)
	defer healthTick.Stop()
	scrubTick := time.NewTicker(2 * time.Second)
	defer scrubTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-healthTick.C:
			s.checkHealth(obs)
		case <-scrubTick.C:
			s.checkScrubbers(ctx, obs)
		}
	}
}

// checkHealth emits worker.online / worker.offline for every status flip since the last call; the first sighting of a worker is silent.
func (s *Server) checkHealth(obs *observer) {
	obs.mu.Lock()
	defer obs.mu.Unlock()
	for id, l := range s.d.Health.Snapshot() {
		prev, seen := obs.health[id]
		obs.health[id] = l.Status
		if !seen || prev == l.Status {
			continue
		}
		switch l.Status {
		case models.WorkerOffline:
			s.d.Bus.Emit("worker.offline", "no heartbeat within the lease, worker declared OFFLINE", "worker", id)
		case models.WorkerOnline:
			s.d.Bus.Emit("worker.online", "heartbeat received again, worker is ONLINE", "worker", id)
		}
	}
}

// checkScrubbers emits scrub.quarantine for every corrupt chunk a worker's scrubber reports that was not seen before; a worker's first report is silent.
func (s *Server) checkScrubbers(ctx context.Context, obs *observer) {
	inv := s.inv.get(ctx)
	obs.mu.Lock()
	defer obs.mu.Unlock()
	for id, wi := range inv.Workers {
		if wi.Scrubber == nil {
			continue
		}
		recent, _ := wi.Scrubber["recent_corruption"].([]any)
		first := !obs.scrubKnown[id]
		obs.scrubKnown[id] = true
		for i := len(recent) - 1; i >= 0; i-- {
			item, _ := recent[i].(map[string]any)
			chunk, _ := item["chunk_id"].(string)
			at, _ := item["at"].(string)
			if chunk == "" {
				continue
			}
			key := fmt.Sprintf("%s|%s|%s", id, chunk, at)
			if obs.scrubSeen[key] {
				continue
			}
			obs.scrubSeen[key] = true
			if first {
				continue
			}
			s.d.Bus.Emit("scrub.quarantine", "scrubber found a corrupt copy and moved it to quarantine", "worker", id, "chunk_id", chunk, "at", at)
		}
	}
}
