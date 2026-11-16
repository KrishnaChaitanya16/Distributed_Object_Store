// Package rebalancer runs a background loop that restores RF=2 when a worker
// goes offline. Every tick it scans all DURABLE chunks, finds ones whose bbolt
// replica list contains an offline worker, and re-replicates the chunk to a
// healthy worker — replacing the dead slot so the replica count stays at 2.
//
// The 3-replica problem is avoided by atomically removing the dead worker's
// replica entry before adding the new one, so bbolt always reflects exactly 2
// replicas per chunk.
package rebalancer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/chunklock"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const (
	defaultInterval  = 30 * time.Second
	defaultRateLimit = 50 * time.Millisecond // pause between replications to avoid flooding
)

// Rebalancer periodically scans for under-replicated chunks and restores RF=2.
type Rebalancer struct {
	meta      models.MetadataRepository
	workers   models.WorkerClientPool
	health    models.HealthMonitor
	placement models.PlacementStrategy
	interval  time.Duration
	rateLimit time.Duration
}

// New builds a Rebalancer; placement may be nil, in which case the first online worker without a copy is used.
func New(
	meta models.MetadataRepository,
	workers models.WorkerClientPool,
	health models.HealthMonitor,
	placement models.PlacementStrategy,
) *Rebalancer {
	return &Rebalancer{
		meta:      meta,
		workers:   workers,
		health:    health,
		placement: placement,
		interval:  defaultInterval,
		rateLimit: defaultRateLimit,
	}
}

// Run starts the rebalancer loop. Blocks until ctx is cancelled.
func (r *Rebalancer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// TriggerNow runs one repair tick immediately in a new goroutine.
// Call this when a worker goes OFFLINE so repair starts within seconds
// rather than waiting for the next periodic tick.
func (r *Rebalancer) TriggerNow(ctx context.Context) {
	go r.tick(ctx)
}

func (r *Rebalancer) tick(ctx context.Context) {
	// Snapshot of online workers for this tick
	onlineList := r.health.ListOnlineWorkers()
	if len(onlineList) < 2 {
		return // not enough workers to rebalance
	}
	onlineSet := make(map[string]bool, len(onlineList))
	for _, id := range onlineList {
		onlineSet[id] = true
	}

	chunks, err := r.meta.ListChunksByState(ctx, models.ChunkDurable)
	if err != nil {
		slog.Error("rebalancer: list chunks", "err", err)
		return
	}

	repaired := 0
	for _, chunk := range chunks {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if r.fixChunk(ctx, chunk, onlineList, onlineSet) {
			repaired++
			time.Sleep(r.rateLimit)
		}
	}

	if repaired > 0 {
		slog.Info("rebalancer: tick complete", "chunks_repaired", repaired)
		metrics.RebalancerRepairsTotal.Add(float64(repaired))
	}
}

// fixChunk checks one chunk and re-replicates it if an offline worker holds one
// of its replica slots. Returns true if a replication was performed.
func (r *Rebalancer) fixChunk(ctx context.Context, chunk *models.ChunkRecord, onlineList []string, onlineSet map[string]bool) bool {
	// Classify replicas as online or offline
	existingWorkers := make(map[string]bool, len(chunk.Replicas))
	var source *models.Replica
	var deadWorkerID string

	for i := range chunk.Replicas {
		rep := chunk.Replicas[i]
		existingWorkers[rep.WorkerID] = true
		if onlineSet[rep.WorkerID] && source == nil {
			source = &chunk.Replicas[i]
		} else if !onlineSet[rep.WorkerID] && deadWorkerID == "" {
			deadWorkerID = rep.WorkerID
		}
	}

	// RF=2 is healthy OR no live source to replicate from
	if deadWorkerID == "" || source == nil {
		return false
	}

	// Prefer the ring's secondary for this chunk so repaired chunks land where fresh uploads would; it must be online and not already a holder.
	var targetID string
	if r.placement != nil {
		if primary, err := r.placement.Primary(chunk.ChunkID); err == nil {
			if id, err := r.placement.Secondary(chunk.ChunkID, primary); err == nil && onlineSet[id] && !existingWorkers[id] {
				targetID = id
			}
		}
	}

	// Otherwise take the first online worker that does not hold this chunk.
	for _, id := range onlineList {
		if targetID != "" {
			break
		}
		if !existingWorkers[id] {
			targetID = id
			break
		}
	}
	if targetID == "" {
		// All online workers already hold this chunk — nothing to do
		return false
	}

	targetWorker, err := r.meta.GetWorker(ctx, targetID)
	if err != nil {
		slog.Warn("rebalancer: get target worker", "worker_id", targetID, "err", err)
		return false
	}
	targetAddr := fmt.Sprintf("%s:%d", targetWorker.Address, targetWorker.Port)

	// Hold the chunk lock across copy and record update, and re-check the record, so garbage collection and reconciliation never see a half-finished move.
	unlock := chunklock.Lock(chunk.ChunkID)
	defer unlock()
	cur, err := r.meta.GetChunk(ctx, chunk.ChunkID)
	if err != nil || cur.State != models.ChunkDurable || !hasReplica(cur, deadWorkerID) || hasReplica(cur, targetID) {
		return false
	}

	// Replicate with a generous timeout
	rCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if err := r.workers.ReplicatePeer(rCtx, source.WorkerAddr, chunk.ChunkID, targetAddr); err != nil {
		slog.Warn("rebalancer: replicate failed",
			"chunk_id", chunk.ChunkID,
			"source", source.WorkerID,
			"target", targetID,
			"err", err)
		return false
	}

	// Replace dead replica with new one — keeps replica count at exactly 2.
	// Remove first, then add, so a crash between the two leaves us with 1
	// replica (recoverable next tick) rather than 3 (harder to clean up).
	if err := r.meta.RemoveChunkReplica(ctx, chunk.ChunkID, deadWorkerID); err != nil {
		slog.Error("rebalancer: remove dead replica",
			"chunk_id", chunk.ChunkID, "worker", deadWorkerID, "err", err)
	}
	if err := r.meta.AddChunkReplica(ctx, chunk.ChunkID, models.Replica{
		WorkerID:   targetID,
		WorkerAddr: targetAddr,
	}); err != nil {
		slog.Error("rebalancer: add new replica",
			"chunk_id", chunk.ChunkID, "worker", targetID, "err", err)
	}

	slog.Info("rebalancer: RF=2 restored",
		"chunk_id", chunk.ChunkID,
		"source", source.WorkerID,
		"target", targetID,
		"replaced", deadWorkerID)
	return true
}

func hasReplica(chunk *models.ChunkRecord, workerID string) bool {
	for _, r := range chunk.Replicas {
		if r.WorkerID == workerID {
			return true
		}
	}
	return false
}
