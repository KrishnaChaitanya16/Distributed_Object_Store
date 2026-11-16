// Package gc removes unreferenced chunks and keeps every live chunk at exactly its listed replicas.
package gc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/chunklock"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// Collector deletes chunks that no COMMITTED or STAGED object references.
type Collector struct {
	meta    models.MetadataRepository
	workers models.WorkerClientPool
	health  models.HealthMonitor
}

func NewCollector(meta models.MetadataRepository, workers models.WorkerClientPool, health models.HealthMonitor) *Collector {
	return &Collector{meta: meta, workers: workers, health: health}
}

// Collect claims the unreferenced candidates in one transaction, then deletes their copies; holders adds copies bbolt does not list.
func (c *Collector) Collect(ctx context.Context, chunkIDs []string, holders map[string][]models.Replica) (int, error) {
	if len(chunkIDs) == 0 {
		return 0, nil
	}
	claimed, err := c.meta.ClaimChunksForDeletion(ctx, chunkIDs, holders)
	if err != nil {
		return 0, fmt.Errorf("claim chunks for deletion: %w", err)
	}
	deleted := 0
	var errs []error
	for _, rec := range claimed {
		if err := c.sweep(ctx, rec); err != nil {
			errs = append(errs, err)
			continue
		}
		deleted++
	}
	metrics.GCChunksDeletedTotal.Add(float64(deleted))
	return deleted, errors.Join(errs...)
}

// sweep deletes every reachable copy and drops the record only when no online copy can remain; otherwise the record stays PENDING_DELETE for the next pass.
func (c *Collector) sweep(ctx context.Context, rec *models.ChunkRecord) error {
	unlock := chunklock.Lock(rec.ChunkID)
	defer unlock()
	var errs []error
	skipped := 0
	for _, rep := range rec.Replicas {
		if c.health.GetWorkerStatus(rep.WorkerID) != models.WorkerOnline {
			skipped++
			continue
		}
		if err := c.workers.DeleteChunk(ctx, workerAddr(ctx, c.meta, rep), rec.ChunkID); err != nil {
			errs = append(errs, fmt.Errorf("delete chunk %s on %s: %w", rec.ChunkID, rep.WorkerID, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := c.meta.RemoveDeletedChunk(ctx, rec.ChunkID); err != nil {
		return fmt.Errorf("remove chunk record %s: %w", rec.ChunkID, err)
	}
	slog.Info("gc: chunk deleted", "chunk_id", rec.ChunkID, "copies", len(rec.Replicas)-skipped, "offline_skipped", skipped)
	return nil
}

// workerAddr prefers the worker's current registration over the address cached in the replica.
func workerAddr(ctx context.Context, meta models.MetadataRepository, rep models.Replica) string {
	if w, err := meta.GetWorker(ctx, rep.WorkerID); err == nil {
		return fmt.Sprintf("%s:%d", w.Address, w.Port)
	}
	return rep.WorkerAddr
}
