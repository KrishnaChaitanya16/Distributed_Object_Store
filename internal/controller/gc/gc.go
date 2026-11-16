// Package gc cleans up objects and chunks that were left in an inconsistent
// state by controller crashes or worker failures during upload.
//
// Two independent passes run on every tick:
//
//  1. Stale-object pass: finds STAGED or FAILED objects older than StagedTTL,
//     decrements the ref-count of every chunk they reference, deletes chunks
//     from workers when the ref-count reaches zero, then removes the manifest.
//
//  2. Orphan-chunk pass: finds PENDING chunks older than PendingTTL (left by
//     mid-fanout crashes where SaveChunkRefs never ran) and DURABLE chunks
//     whose ref-count is already zero (left by a crash between
//     UpdateChunkState and IncrementChunkRefCount).  Both are deleted from
//     workers and from bbolt.
package gc

import (
	"context"
	"log/slog"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const (
	DefaultStagedTTL  = 10 * time.Minute // STAGED objects older than this are dead
	DefaultPendingTTL = 10 * time.Minute // PENDING chunks older than this are orphans
	DefaultInterval   = 5 * time.Minute  // how often the GC ticker fires
)

// GC runs the two background cleanup passes.
type GC struct {
	meta       models.MetadataRepository
	workers    models.WorkerClientPool
	StagedTTL  time.Duration
	PendingTTL time.Duration
	Interval   time.Duration
}

// New constructs a GC with production defaults.
func New(meta models.MetadataRepository, workers models.WorkerClientPool) *GC {
	return &GC{
		meta:       meta,
		workers:    workers,
		StagedTTL:  DefaultStagedTTL,
		PendingTTL: DefaultPendingTTL,
		Interval:   DefaultInterval,
	}
}

// Run starts the GC loop. Blocks until ctx is cancelled.
func (g *GC) Run(ctx context.Context) {
	ticker := time.NewTicker(g.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.Tick(ctx)
		}
	}
}

// Tick runs both GC passes once. Exported so tests can drive it directly.
func (g *GC) Tick(ctx context.Context) {
	g.cleanStaleObjects(ctx)
	g.cleanOrphanChunks(ctx)
}

// ── Pass 1: stale STAGED / FAILED objects ────────────────────────────────────

func (g *GC) cleanStaleObjects(ctx context.Context) {
	now := time.Now().UTC()

	for _, status := range []models.ObjectStatus{models.StatusStaged, models.StatusFailed, models.StatusDeleted} {
		objects, err := g.meta.ListObjectsByStatus(ctx, status)
		if err != nil {
			slog.Error("gc: list objects by status", "status", status, "err", err)
			continue
		}

		for _, obj := range objects {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// STAGED objects need to be old enough that no active upload is still in progress.
			// FAILED and DELETED objects are always stale — clean immediately.
			if status == models.StatusStaged && now.Sub(obj.CreatedAt) < g.StagedTTL {
				continue
			}

			g.deleteObject(ctx, obj)
		}
	}
}

// deleteObject decrements ref-counts for every chunk the object references,
// removes from workers the chunks whose count reached zero, then deletes the manifest.
func (g *GC) deleteObject(ctx context.Context, obj *models.ObjectManifest) {
	refs, err := g.meta.GetChunkRefs(ctx, obj.ID)
	if err != nil {
		slog.Warn("gc: get chunk refs", "object_id", obj.ID, "err", err)
		// Proceed to delete the manifest anyway — chunks will be caught by orphan pass.
	}

	for _, ref := range refs {
		g.releaseChunk(ctx, ref.ChunkID)
	}

	if err := g.meta.DeleteObject(ctx, obj.ID); err != nil {
		slog.Error("gc: delete object manifest", "object_id", obj.ID, "err", err)
		return
	}
	slog.Info("gc: stale object deleted", "object_id", obj.ID, "status", obj.Status, "chunks", len(refs))
}

// releaseChunk decrements the ref-count for chunkID. When it reaches zero the
// chunk is removed from every worker that holds it and then from bbolt.
func (g *GC) releaseChunk(ctx context.Context, chunkID string) {
	newCount, err := g.meta.DecrementChunkRefCount(ctx, chunkID)
	if err != nil {
		slog.Warn("gc: decrement ref count", "chunk_id", chunkID, "err", err)
		return
	}
	if newCount > 0 {
		return // still referenced by another committed object
	}
	g.deleteChunkFromWorkers(ctx, chunkID)
}

// deleteChunkFromWorkers fetches the chunk record, tells every replica worker
// to delete the chunk, then removes the record from bbolt.
func (g *GC) deleteChunkFromWorkers(ctx context.Context, chunkID string) {
	rec, err := g.meta.GetChunk(ctx, chunkID)
	if err != nil {
		slog.Warn("gc: get chunk for deletion", "chunk_id", chunkID, "err", err)
		return
	}

	for _, replica := range rec.Replicas {
		dCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := g.workers.DeleteChunk(dCtx, replica.WorkerAddr, chunkID); err != nil {
			slog.Warn("gc: delete chunk from worker",
				"chunk_id", chunkID, "worker", replica.WorkerID, "err", err)
		}
		cancel()
	}

	if err := g.meta.DeleteChunk(ctx, chunkID); err != nil {
		slog.Error("gc: delete chunk from bbolt", "chunk_id", chunkID, "err", err)
	} else {
		slog.Info("gc: orphan chunk deleted", "chunk_id", chunkID)
	}
}

// ── Pass 2: orphan chunks ────────────────────────────────────────────────────

func (g *GC) cleanOrphanChunks(ctx context.Context) {
	now := time.Now().UTC()

	// PENDING chunks: left by a crash between SaveChunk and UpdateChunkState
	// (or between PutChunk(primary) and PutChunk(secondary)).
	// A PENDING chunk that is still PENDING after PendingTTL will never be
	// committed — the upload that created it is long dead.
	pending, err := g.meta.ListChunksByState(ctx, models.ChunkPending)
	if err != nil {
		slog.Error("gc: list pending chunks", "err", err)
	} else {
		for _, chunk := range pending {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if now.Sub(chunk.CreatedAt) >= g.PendingTTL {
				g.deleteChunkFromWorkers(ctx, chunk.ChunkID)
			}
		}
	}

	// DURABLE chunks with ref_count == 0: left by a crash between
	// UpdateChunkState(DURABLE) and IncrementChunkRefCount. The object that
	// was being uploaded is STAGED and will be caught by Pass 1 on this same
	// tick (or was already cleaned). The chunk itself has no live owner.
	durable, err := g.meta.ListChunksByState(ctx, models.ChunkDurable)
	if err != nil {
		slog.Error("gc: list durable chunks", "err", err)
		return
	}
	for _, chunk := range durable {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if chunk.RefCount == 0 {
			g.deleteChunkFromWorkers(ctx, chunk.ChunkID)
		}
	}
}
