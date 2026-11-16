package gc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/chunklock"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metrics"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// replicationFactor is the number of healthy listed copies every live chunk should have.
const replicationFactor = 2

var ErrAlreadyRunning = errors.New("reconciler is already running")

type Config struct {
	// Interval is the gap between the end of one pass and the start of the next.
	Interval time.Duration
	// StagedTimeout is how long a STAGED upload may stay idle before it is marked FAILED.
	StagedTimeout time.Duration
	// JoinPoll is how often the online set is checked so a worker that joins or rejoins is reconciled at once.
	JoinPoll time.Duration
	// OrphanGrace is how long a file unknown to bbolt must keep being seen before it is deleted.
	OrphanGrace time.Duration
}

// Report describes one pass.
type Report struct {
	StartedAt          time.Time `json:"started_at"`
	DurationMs         int64     `json:"duration_ms"`
	WorkersScanned     int       `json:"workers_scanned"`
	StaleUploadsFailed int       `json:"stale_uploads_failed"`
	ChunksDeleted      int       `json:"chunks_deleted"`
	ExcessDeleted      int       `json:"excess_deleted"`
	ReplicasRestored   int       `json:"replicas_restored"`
	ChunksAdopted      int       `json:"chunks_adopted"`
	ObjectsPurged      int       `json:"objects_purged"`
	LostChunks         int       `json:"lost_chunks"`
	Errors             int       `json:"errors"`
	LastError          string    `json:"last_error,omitempty"`
}

type Status struct {
	Running              bool    `json:"running"`
	Passes               int64   `json:"passes"`
	IntervalSeconds      float64 `json:"interval_seconds"`
	StagedTimeoutSeconds float64 `json:"staged_timeout_seconds"`
	OrphanGraceSeconds   float64 `json:"orphan_grace_seconds"`
	LastPass             *Report `json:"last_pass,omitempty"`
}

// Reconciler compares worker inventories with bbolt and repairs every difference.
type Reconciler struct {
	meta    models.MetadataRepository
	workers models.WorkerClientPool
	health  models.HealthMonitor
	col     *Collector
	cfg     Config
	trigger chan struct{}
	started atomic.Bool
	passMu  sync.Mutex
	// orphanSince records when each file unknown to bbolt was first seen; only touched under passMu.
	orphanSince map[string]time.Time

	mu sync.Mutex
	st Status
}

func NewReconciler(meta models.MetadataRepository, workers models.WorkerClientPool, health models.HealthMonitor, col *Collector, cfg Config) (*Reconciler, error) {
	if meta == nil || workers == nil || health == nil || col == nil {
		return nil, errors.New("reconciler needs metadata, workers, health and a collector")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("gc interval must be positive, got %s", cfg.Interval)
	}
	if cfg.StagedTimeout <= 0 {
		return nil, fmt.Errorf("staged timeout must be positive, got %s", cfg.StagedTimeout)
	}
	if cfg.OrphanGrace < 0 {
		return nil, fmt.Errorf("orphan grace must not be negative, got %s", cfg.OrphanGrace)
	}
	if cfg.JoinPoll <= 0 {
		cfg.JoinPoll = 2 * time.Second
	}
	return &Reconciler{
		meta: meta, workers: workers, health: health, col: col, cfg: cfg,
		trigger:     make(chan struct{}, 1),
		orphanSince: make(map[string]time.Time),
		st:          Status{IntervalSeconds: cfg.Interval.Seconds(), StagedTimeoutSeconds: cfg.StagedTimeout.Seconds(), OrphanGraceSeconds: cfg.OrphanGrace.Seconds()},
	}, nil
}

// Run reconciles every Interval, on Trigger, and whenever a worker joins the online set, until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	timer := time.NewTimer(r.cfg.Interval)
	defer timer.Stop()
	poll := time.NewTicker(r.cfg.JoinPoll)
	defer poll.Stop()
	known := toSet(r.health.ListOnlineWorkers())
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		case <-r.trigger:
			timer.Stop()
		case <-poll.C:
			now := toSet(r.health.ListOnlineWorkers())
			joined := false
			for id := range now {
				if !known[id] {
					joined = true
					slog.Info("gc: worker joined, reconciling now", "worker_id", id)
				}
			}
			known = now
			if !joined {
				continue
			}
			timer.Stop()
		}
		r.Pass(ctx)
		if ctx.Err() != nil {
			return nil
		}
		timer.Reset(r.cfg.Interval)
	}
}

// Trigger asks for a pass now; it returns false when one is already queued.
func (r *Reconciler) Trigger() bool {
	select {
	case r.trigger <- struct{}{}:
		return true
	default:
		return false
	}
}

func (r *Reconciler) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.st
	if r.st.LastPass != nil {
		last := *r.st.LastPass
		st.LastPass = &last
	}
	return st
}

// Pass runs one full reconciliation; concurrent callers are serialised.
func (r *Reconciler) Pass(ctx context.Context) Report {
	r.passMu.Lock()
	defer r.passMu.Unlock()
	r.update(func(st *Status) { st.Running = true })
	p := &pass{r: r, ctx: ctx, rep: Report{StartedAt: time.Now().UTC()}}
	p.run()
	p.rep.DurationMs = time.Since(p.rep.StartedAt).Milliseconds()
	rep := p.rep
	r.update(func(st *Status) {
		st.Running = false
		st.Passes++
		st.LastPass = &rep
	})
	if rep.ChunksDeleted+rep.ExcessDeleted+rep.ReplicasRestored+rep.ChunksAdopted+rep.StaleUploadsFailed+rep.ObjectsPurged+rep.LostChunks+rep.Errors > 0 {
		slog.Info("gc: pass complete", "chunks_deleted", rep.ChunksDeleted, "excess_deleted", rep.ExcessDeleted,
			"replicas_restored", rep.ReplicasRestored, "chunks_adopted", rep.ChunksAdopted, "stale_uploads_failed", rep.StaleUploadsFailed,
			"objects_purged", rep.ObjectsPurged, "lost_chunks", rep.LostChunks, "errors", rep.Errors, "duration_ms", rep.DurationMs)
	}
	return rep
}

func (r *Reconciler) update(f func(*Status)) {
	r.mu.Lock()
	f(&r.st)
	r.mu.Unlock()
}

// pass holds one pass's snapshot of the cluster.
type pass struct {
	r       *Reconciler
	ctx     context.Context
	rep     Report
	online  map[string]bool
	addrs   map[string]string
	inv     map[string]map[string]bool
	holders map[string][]models.Replica
}

func (p *pass) fail(op string, err error) {
	p.rep.Errors++
	p.rep.LastError = op + ": " + err.Error()
	slog.Warn("gc: "+op, "err", err)
}

func (p *pass) run() {
	m := p.r.meta
	if ids, err := m.FailStaleUploads(p.ctx, time.Now().UTC().Add(-p.r.cfg.StagedTimeout)); err != nil {
		p.fail("expire stale uploads", err)
	} else if len(ids) > 0 {
		p.rep.StaleUploadsFailed = len(ids)
		metrics.GCStaleUploadsTotal.Add(float64(len(ids)))
		slog.Warn("gc: stale uploads marked FAILED", "object_ids", ids)
	}

	p.scanWorkers()

	if adopted, err := m.AdoptChunks(p.ctx, p.holders); err != nil {
		p.fail("adopt chunks", err)
	} else if len(adopted) > 0 {
		p.rep.ChunksAdopted = len(adopted)
		metrics.ReconcileChunksAdoptedTotal.Add(float64(len(adopted)))
		slog.Warn("gc: rebuilt missing chunk records from worker inventories", "chunk_ids", adopted)
	}

	records, err := m.ListAllChunks(p.ctx)
	if err != nil {
		p.fail("list chunks", err)
		return
	}
	live, err := m.LiveChunkRefs(p.ctx)
	if err != nil {
		p.fail("count live references", err)
		return
	}

	recorded := make(map[string]bool, len(records))
	var candidates []string
	for _, rec := range records {
		recorded[rec.ChunkID] = true
		if live[rec.ChunkID] == 0 {
			candidates = append(candidates, rec.ChunkID)
		}
	}
	now := time.Now()
	orphans := make(map[string]time.Time)
	for _, id := range sortedChunkIDs(p.holders) {
		if recorded[id] || live[id] > 0 {
			continue
		}
		since, ok := p.r.orphanSince[id]
		if !ok {
			since = now
		}
		orphans[id] = since
		if now.Sub(since) >= p.r.cfg.OrphanGrace {
			candidates = append(candidates, id)
		}
	}
	p.r.orphanSince = orphans
	n, err := p.r.col.Collect(p.ctx, candidates, p.holders)
	p.rep.ChunksDeleted = n
	if err != nil {
		p.fail("collect garbage", err)
	}

	lost := 0
	for _, rec := range records {
		if rec.State == models.ChunkDurable && live[rec.ChunkID] > 0 && !p.reconcile(rec) {
			lost++
		}
	}
	for id := range live {
		if !recorded[id] && len(p.holders[id]) == 0 {
			lost++
			slog.Error("gc: referenced chunk has no record and no copy on any online worker", "chunk_id", id)
		}
	}
	p.rep.LostChunks = lost
	metrics.ChunksLost.Set(float64(lost))

	if n, err := m.PurgeDeadObjects(p.ctx); err != nil {
		p.fail("purge dead objects", err)
	} else {
		p.rep.ObjectsPurged = n
		metrics.GCObjectsPurgedTotal.Add(float64(n))
	}
	if err := m.SyncRefCounts(p.ctx); err != nil {
		p.fail("sync ref counts", err)
	}
}

// scanWorkers lists every online worker's chunks; a worker whose list fails is treated as unknown, never as empty.
func (p *pass) scanWorkers() {
	p.online = toSet(p.r.health.ListOnlineWorkers())
	p.addrs = make(map[string]string)
	p.inv = make(map[string]map[string]bool)
	p.holders = make(map[string][]models.Replica)
	ids := make([]string, 0, len(p.online))
	for id := range p.online {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		w, err := p.r.meta.GetWorker(p.ctx, id)
		if err != nil {
			p.fail("look up worker "+id, err)
			continue
		}
		addr := fmt.Sprintf("%s:%d", w.Address, w.Port)
		chunks, err := p.r.workers.ListChunks(p.ctx, addr, id)
		if err != nil {
			p.fail("list chunks on "+id, err)
			continue
		}
		p.addrs[id] = addr
		set := make(map[string]bool, len(chunks))
		for _, c := range chunks {
			set[c] = true
			p.holders[c] = append(p.holders[c], models.Replica{WorkerID: id, WorkerAddr: addr})
		}
		p.inv[id] = set
		p.rep.WorkersScanned++
	}
}

// has reports whether a scanned worker holds the chunk; known is false when the worker was not scanned.
func (p *pass) has(workerID, chunkID string) (has, known bool) {
	set, ok := p.inv[workerID]
	if !ok {
		return false, false
	}
	return set[chunkID], true
}

// reconcile restores missing listed copies, tops up under-replicated records and removes unlisted copies; it returns false if the chunk is lost.
func (p *pass) reconcile(rec *models.ChunkRecord) bool {
	id := rec.ChunkID
	var sources []models.Replica
	listed := make(map[string]bool, len(rec.Replicas))
	for _, r := range rec.Replicas {
		listed[r.WorkerID] = true
	}
	for _, h := range p.holders[id] {
		if listed[h.WorkerID] {
			sources = append([]models.Replica{h}, sources...)
		} else {
			sources = append(sources, h)
		}
	}
	if len(sources) == 0 {
		for _, r := range rec.Replicas {
			if _, known := p.has(r.WorkerID, id); !known {
				return true
			}
		}
		slog.Error("gc: chunk has no copy on any online worker", "chunk_id", id, "replicas", rec.Replicas)
		return false
	}

	for _, r := range rec.Replicas {
		if has, known := p.has(r.WorkerID, id); known && !has {
			p.restore(id, sources[0], r.WorkerID)
		}
	}
	if len(rec.Replicas) < replicationFactor {
		p.topUp(rec, sources)
	}
	for _, h := range p.holders[id] {
		if !listed[h.WorkerID] {
			p.removeExcess(id, h.WorkerID)
		}
	}
	return true
}

// restore copies a chunk back onto a listed replica that lost it, for example after the scrubber quarantined it.
func (p *pass) restore(chunkID string, src models.Replica, target string) {
	unlock := chunklock.Lock(chunkID)
	defer unlock()
	rec, err := p.r.meta.GetChunk(p.ctx, chunkID)
	if err != nil || rec.State != models.ChunkDurable || !listedIn(rec, target) || src.WorkerID == target {
		return
	}
	if err := p.r.workers.ReplicatePeer(p.ctx, src.WorkerAddr, chunkID, p.addrs[target]); err != nil {
		p.fail("restore "+chunkID+" on "+target, err)
		return
	}
	p.inv[target][chunkID] = true
	p.holders[chunkID] = append(p.holders[chunkID], models.Replica{WorkerID: target, WorkerAddr: p.addrs[target]})
	p.rep.ReplicasRestored++
	metrics.ReconcileReplicasRestoredTotal.Inc()
	slog.Info("gc: missing replica restored", "chunk_id", chunkID, "source", src.WorkerID, "target", target)
}

// topUp brings a record listing fewer than replicationFactor workers back up, adopting an existing copy before copying to a new worker.
func (p *pass) topUp(rec *models.ChunkRecord, sources []models.Replica) {
	unlock := chunklock.Lock(rec.ChunkID)
	defer unlock()
	cur, err := p.r.meta.GetChunk(p.ctx, rec.ChunkID)
	if err != nil || cur.State != models.ChunkDurable {
		return
	}
	need := replicationFactor - len(cur.Replicas)
	for _, s := range sources {
		if need == 0 {
			return
		}
		if listedIn(cur, s.WorkerID) {
			continue
		}
		if err := p.r.meta.AddChunkReplica(p.ctx, rec.ChunkID, s); err != nil {
			p.fail("adopt copy of "+rec.ChunkID+" on "+s.WorkerID, err)
			return
		}
		cur.Replicas = append(cur.Replicas, s)
		need--
		p.rep.ReplicasRestored++
		metrics.ReconcileReplicasRestoredTotal.Inc()
		slog.Info("gc: existing copy listed as replica", "chunk_id", rec.ChunkID, "worker_id", s.WorkerID)
	}
	ids := make([]string, 0, len(p.inv))
	for id := range p.inv {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, target := range ids {
		if need == 0 {
			return
		}
		if listedIn(cur, target) || p.inv[target][rec.ChunkID] {
			continue
		}
		if err := p.r.workers.ReplicatePeer(p.ctx, sources[0].WorkerAddr, rec.ChunkID, p.addrs[target]); err != nil {
			p.fail("copy "+rec.ChunkID+" to "+target, err)
			continue
		}
		rep := models.Replica{WorkerID: target, WorkerAddr: p.addrs[target]}
		if err := p.r.meta.AddChunkReplica(p.ctx, rec.ChunkID, rep); err != nil {
			p.fail("list new replica of "+rec.ChunkID+" on "+target, err)
			return
		}
		cur.Replicas = append(cur.Replicas, rep)
		p.inv[target][rec.ChunkID] = true
		need--
		p.rep.ReplicasRestored++
		metrics.ReconcileReplicasRestoredTotal.Inc()
		slog.Info("gc: under-replicated chunk copied to new worker", "chunk_id", rec.ChunkID, "target", target)
	}
}

// removeExcess deletes an unlisted copy, but only while replicationFactor listed replicas are confirmed to hold the chunk.
func (p *pass) removeExcess(chunkID, workerID string) {
	unlock := chunklock.Lock(chunkID)
	defer unlock()
	rec, err := p.r.meta.GetChunk(p.ctx, chunkID)
	if err != nil || rec.State != models.ChunkDurable || listedIn(rec, workerID) {
		return
	}
	healthy := 0
	for _, r := range rec.Replicas {
		if has, _ := p.has(r.WorkerID, chunkID); has && p.online[r.WorkerID] {
			healthy++
		}
	}
	if healthy < replicationFactor {
		return
	}
	if err := p.r.workers.DeleteChunk(p.ctx, p.addrs[workerID], chunkID); err != nil {
		p.fail("delete excess copy of "+chunkID+" on "+workerID, err)
		return
	}
	delete(p.inv[workerID], chunkID)
	p.rep.ExcessDeleted++
	metrics.ReconcileExcessDeletedTotal.Inc()
	slog.Info("gc: excess copy deleted", "chunk_id", chunkID, "worker_id", workerID, "listed_replicas", len(rec.Replicas))
}

func listedIn(rec *models.ChunkRecord, workerID string) bool {
	for _, r := range rec.Replicas {
		if r.WorkerID == workerID {
			return true
		}
	}
	return false
}

func toSet(ids []string) map[string]bool {
	s := make(map[string]bool, len(ids))
	for _, id := range ids {
		s[id] = true
	}
	return s
}

func sortedChunkIDs(m map[string][]models.Replica) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
