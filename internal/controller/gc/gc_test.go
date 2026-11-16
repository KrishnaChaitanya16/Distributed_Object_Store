package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/metadata"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// cluster is a fake set of workers keyed by address "<id>:1".
type cluster struct {
	mu        sync.Mutex
	disk      map[string]map[string]bool
	online    map[string]bool
	listErr   map[string]error
	deleteErr map[string]error
	copyErr   map[string]error
	calls     []string
	onList    func(workerID string)
	onDelete  func(workerID, chunkID string)
}

func newCluster(workers ...string) *cluster {
	c := &cluster{disk: map[string]map[string]bool{}, online: map[string]bool{}, listErr: map[string]error{}, deleteErr: map[string]error{}, copyErr: map[string]error{}}
	for _, w := range workers {
		c.disk[w] = map[string]bool{}
		c.online[w] = true
	}
	return c
}

func id(addr string) string { return strings.TrimSuffix(addr, ":1") }

func (c *cluster) put(worker string, chunks ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range chunks {
		c.disk[worker][ch] = true
	}
}

func (c *cluster) holds(worker, chunk string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.disk[worker][chunk]
}

func (c *cluster) setOnline(worker string, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.online[worker] = on
}

func (c *cluster) log(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, fmt.Sprintf(format, args...))
}

func (c *cluster) callsWith(prefix string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, s := range c.calls {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	return out
}

func (c *cluster) PutChunk(_ context.Context, addr, chunk string, _ io.Reader, _ int64) error {
	c.put(id(addr), chunk)
	return nil
}

func (c *cluster) GetChunk(context.Context, string, string) (io.ReadCloser, error) {
	return nil, errors.New("not used")
}

func (c *cluster) DeleteChunk(_ context.Context, addr, chunk string) error {
	w := id(addr)
	if c.onDelete != nil {
		c.onDelete(w, chunk)
	}
	c.log("DEL %s %s", w, chunk)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.deleteErr[w]; err != nil {
		return err
	}
	delete(c.disk[w], chunk)
	return nil
}

func (c *cluster) ReplicatePeer(_ context.Context, src, chunk, dst string) error {
	s, d := id(src), id(dst)
	c.log("COPY %s %s %s", chunk, s, d)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.copyErr[d]; err != nil {
		return err
	}
	if !c.disk[s][chunk] {
		return errors.New("source does not hold chunk")
	}
	c.disk[d][chunk] = true
	return nil
}

func (c *cluster) ListChunks(_ context.Context, addr, worker string) ([]string, error) {
	if c.onList != nil {
		c.onList(worker)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.listErr[worker]; err != nil {
		return nil, err
	}
	var out []string
	for ch := range c.disk[worker] {
		out = append(out, ch)
	}
	sort.Strings(out)
	return out, nil
}

func (c *cluster) RecordHeartbeat(string) {}
func (c *cluster) Subscribe() <-chan models.WorkerOfflineEvent {
	return nil
}
func (c *cluster) GetWorkerStatus(w string) models.WorkerStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.online[w] {
		return models.WorkerOnline
	}
	return models.WorkerOffline
}
func (c *cluster) ListOnlineWorkers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for w, on := range c.online {
		if on {
			out = append(out, w)
		}
	}
	sort.Strings(out)
	return out
}

type env struct {
	t     *testing.T
	ctx   context.Context
	store *metadata.MetadataStore
	c     *cluster
	col   *Collector
	rec   *Reconciler
}

func newEnv(t *testing.T, workers ...string) *env {
	t.Helper()
	store, err := metadata.NewMetadataStore(t.TempDir() + "/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()
	for _, w := range workers {
		require.NoError(t, store.SaveWorker(ctx, &models.WorkerRecord{WorkerID: w, Address: w, Port: 1}))
	}
	c := newCluster(workers...)
	col := NewCollector(store, c, c)
	rec, err := NewReconciler(store, c, c, col, Config{Interval: time.Hour, StagedTimeout: time.Minute, JoinPoll: 10 * time.Millisecond})
	require.NoError(t, err)
	return &env{t: t, ctx: ctx, store: store, c: c, col: col, rec: rec}
}

func reps(workers ...string) []models.Replica {
	out := make([]models.Replica, len(workers))
	for i, w := range workers {
		out[i] = models.Replica{WorkerID: w, WorkerAddr: w + ":1"}
	}
	return out
}

// chunk stores a record and puts the bytes on every listed worker.
func (e *env) chunk(chunkID string, state models.ChunkState, workers ...string) {
	e.t.Helper()
	require.NoError(e.t, e.store.SaveChunk(e.ctx, &models.ChunkRecord{ChunkID: chunkID, State: state, Replicas: reps(workers...)}))
	for _, w := range workers {
		e.c.put(w, chunkID)
	}
}

func (e *env) object(objectID string, status models.ObjectStatus, chunks ...string) {
	e.t.Helper()
	require.NoError(e.t, e.store.SaveObject(e.ctx, &models.ObjectManifest{ID: objectID, Status: status, ModifiedAt: time.Now().UTC()}))
	refs := make([]models.ChunkRef, len(chunks))
	for i, ch := range chunks {
		refs[i] = models.ChunkRef{ChunkID: ch, Index: i}
	}
	require.NoError(e.t, e.store.SaveChunkRefs(e.ctx, objectID, refs))
}

func (e *env) state(chunkID string) models.ChunkState {
	r, err := e.store.GetChunk(e.ctx, chunkID)
	if errors.Is(err, models.ErrNotFound) {
		return ""
	}
	require.NoError(e.t, err)
	return r.State
}

func (e *env) listed(chunkID string) []string {
	r, err := e.store.GetChunk(e.ctx, chunkID)
	require.NoError(e.t, err)
	var out []string
	for _, x := range r.Replicas {
		out = append(out, x.WorkerID)
	}
	return out
}

func (e *env) holders(chunkID string) []string {
	var out []string
	for _, w := range []string{"w1", "w2", "w3", "w4"} {
		if e.c.holds(w, chunkID) {
			out = append(out, w)
		}
	}
	return out
}

// --- NewReconciler ---

func TestNewReconcilerValidates(t *testing.T) {
	e := newEnv(t, "w1")
	ok := Config{Interval: time.Second, StagedTimeout: time.Second}
	_, err := NewReconciler(nil, e.c, e.c, e.col, ok)
	assert.Error(t, err)
	_, err = NewReconciler(e.store, e.c, e.c, nil, ok)
	assert.Error(t, err)
	_, err = NewReconciler(e.store, e.c, e.c, e.col, Config{StagedTimeout: time.Second})
	assert.Error(t, err)
	_, err = NewReconciler(e.store, e.c, e.c, e.col, Config{Interval: -time.Second, StagedTimeout: time.Second})
	assert.Error(t, err)
	_, err = NewReconciler(e.store, e.c, e.c, e.col, Config{Interval: time.Second})
	assert.Error(t, err)
	_, err = NewReconciler(e.store, e.c, e.c, e.col, Config{Interval: time.Second, StagedTimeout: time.Second, OrphanGrace: -time.Second})
	assert.Error(t, err)
	r, err := NewReconciler(e.store, e.c, e.c, e.col, ok)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, r.cfg.JoinPoll, "join poll defaults to the heartbeat interval")
}

// --- Collector ---

func TestCollectDeletesOnlyUnreferencedChunks(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("live", models.StatusCommitted, "shared")
	e.object("dead", models.StatusDeleted, "shared", "only")
	e.chunk("shared", models.ChunkDurable, "w1", "w2")
	e.chunk("only", models.ChunkDurable, "w2", "w3")

	n, err := e.col.Collect(e.ctx, []string{"shared", "only"}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, models.ChunkState(""), e.state("only"))
	assert.Empty(t, e.holders("only"))
	assert.Equal(t, models.ChunkDurable, e.state("shared"))
	assert.Equal(t, []string{"w1", "w2"}, e.holders("shared"))
	assert.Empty(t, e.c.callsWith("DEL w1 shared"))
}

func TestCollectSkipsOfflineReplicaButFinishes(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.chunk("c", models.ChunkDurable, "w1", "w2")
	e.c.setOnline("w2", false)
	n, err := e.col.Collect(e.ctx, []string{"c"}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, models.ChunkState(""), e.state("c"))
	assert.Equal(t, []string{"w2"}, e.holders("c"), "the offline copy is left for the orphan sweep when w2 returns")
	assert.Empty(t, e.c.callsWith("DEL w2"))
}

func TestCollectKeepsClaimWhenADeleteFailsAndRetries(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.chunk("c", models.ChunkDurable, "w1", "w2")
	e.c.deleteErr["w2"] = errors.New("disk busy")
	n, err := e.col.Collect(e.ctx, []string{"c"}, nil)
	assert.Error(t, err)
	assert.Zero(t, n)
	assert.Equal(t, models.ChunkPendingDelete, e.state("c"), "the record must survive so nobody re-uploads onto a half-deleted chunk")

	delete(e.c.deleteErr, "w2")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ChunksDeleted, "the next pass finishes the sweep")
	assert.Equal(t, models.ChunkState(""), e.state("c"))
	assert.Empty(t, e.holders("c"))
}

func TestCollectEmptyIsNoop(t *testing.T) {
	e := newEnv(t, "w1")
	n, err := e.col.Collect(e.ctx, nil, nil)
	assert.NoError(t, err)
	assert.Zero(t, n)
}

// --- Pass: steady state ---

func TestPassOnHealthyClusterDoesNothing(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "a", "b")
	e.chunk("a", models.ChunkDurable, "w1", "w2")
	e.chunk("b", models.ChunkDurable, "w2", "w3")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, Report{StartedAt: rep.StartedAt, DurationMs: rep.DurationMs, WorkersScanned: 3}, rep)
	assert.Empty(t, e.c.callsWith("DEL"))
	assert.Empty(t, e.c.callsWith("COPY"))
}

func TestPassOnEmptyCluster(t *testing.T) {
	e := newEnv(t)
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.Errors)
	assert.Zero(t, rep.WorkersScanned)
}

// --- Pass: excess copies (the rejoining-worker case) ---

func TestPassDeletesCopyLeftOnRejoinedWorker(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w2", "w3")
	e.c.put("w1", "c")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ExcessDeleted)
	assert.Equal(t, []string{"w2", "w3"}, e.holders("c"))
	assert.Equal(t, []string{"w2", "w3"}, e.listed("c"))
	assert.Equal(t, 0, e.rec.Pass(e.ctx).ExcessDeleted, "idempotent")
}

func TestPassKeepsExcessWhileAListedReplicaIsOffline(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w2", "w3")
	e.c.put("w1", "c")
	e.c.setOnline("w3", false)
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.ExcessDeleted)
	assert.True(t, e.c.holds("w1", "c"), "with only one healthy listed copy the extra copy is precious")
}

func TestPassKeepsExcessWhenListedReplicaCannotBeScanned(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w2", "w3")
	e.c.put("w1", "c")
	e.c.listErr["w3"] = errors.New("timeout")
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.ExcessDeleted)
	assert.True(t, e.c.holds("w1", "c"))
	assert.Equal(t, 1, rep.Errors)
	assert.Contains(t, rep.LastError, "w3")
}

func TestPassRestoresMissingReplicaThenDropsExcess(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w2", "w3")
	e.c.put("w1", "c")
	require.NoError(t, e.c.DeleteChunk(e.ctx, "w3:1", "c"))
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ReplicasRestored)
	assert.Equal(t, 1, rep.ExcessDeleted)
	assert.Equal(t, []string{"w2", "w3"}, e.holders("c"))
}

// TestPassRechecksRecordBeforeDeletingExcess lists the "excess" worker after the pass read the records, as a racing rebalancer would; that copy must survive.
func TestPassRechecksRecordBeforeDeletingExcess(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w2", "w3")
	e.c.put("w1", "c")
	e.c.put("w2", "garbage")
	swapped := false
	e.c.onDelete = func(w, ch string) {
		if ch == "garbage" && !swapped {
			swapped = true
			require.NoError(t, e.store.RemoveChunkReplica(e.ctx, "c", "w3"))
			require.NoError(t, e.store.AddChunkReplica(e.ctx, "c", reps("w1")[0]))
		}
	}
	rep := e.rec.Pass(e.ctx)
	require.True(t, swapped, "the swap must happen between reading records and acting on them")
	assert.True(t, e.c.holds("w1", "c"), "w1 became a listed replica after the records were read and must keep its copy")
	assert.Empty(t, e.c.callsWith("DEL w1 c"))
	assert.Zero(t, rep.ExcessDeleted, "w3 was not excess in this pass's snapshot")
	assert.Equal(t, 1, e.rec.Pass(e.ctx).ExcessDeleted, "the next pass drops w3, now unlisted")
	assert.Equal(t, []string{"w1", "w2"}, e.holders("c"))
}

// --- Pass: missing and under-replicated copies ---

func TestPassRestoresQuarantinedReplica(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w1", "w2")
	require.NoError(t, e.c.DeleteChunk(e.ctx, "w1:1", "c"))
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ReplicasRestored)
	assert.Equal(t, []string{"w1", "w2"}, e.holders("c"))
	assert.Equal(t, []string{"COPY c w2 w1"}, e.c.callsWith("COPY"))
}

func TestPassDoesNotRestoreOntoUnscannedWorker(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w1", "w2")
	e.c.listErr["w1"] = errors.New("timeout")
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.ReplicasRestored)
	assert.Empty(t, e.c.callsWith("COPY"), "an unknown inventory is never read as empty")
	assert.Zero(t, rep.LostChunks)
}

func TestPassRestoreFailureIsReportedAndRetried(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w1", "w2")
	require.NoError(t, e.c.DeleteChunk(e.ctx, "w1:1", "c"))
	e.c.copyErr["w1"] = errors.New("disk full")
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.ReplicasRestored)
	assert.Equal(t, 1, rep.Errors)
	delete(e.c.copyErr, "w1")
	assert.Equal(t, 1, e.rec.Pass(e.ctx).ReplicasRestored)
}

func TestPassListsExistingCopyForUnderReplicatedRecord(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w2")
	e.c.put("w3", "c")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ReplicasRestored)
	assert.Zero(t, rep.ExcessDeleted, "the extra copy is adopted, not deleted")
	assert.ElementsMatch(t, []string{"w2", "w3"}, e.listed("c"))
	assert.Empty(t, e.c.callsWith("COPY"), "no bytes need to move")
}

func TestPassCopiesUnderReplicatedRecordToNewWorker(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w2")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ReplicasRestored)
	assert.Len(t, e.listed("c"), 2)
	assert.Len(t, e.holders("c"), 2)
	assert.Zero(t, e.rec.Pass(e.ctx).ReplicasRestored, "stable afterwards")
}

func TestPassUnderReplicatedWithOnlyOneWorkerStaysPut(t *testing.T) {
	e := newEnv(t, "w1")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w1")
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.ReplicasRestored)
	assert.Zero(t, rep.Errors)
}

// --- Pass: garbage ---

func TestPassDeletesOrphanFileUnknownToBbolt(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.c.put("w1", "orphan")
	e.c.put("w2", "orphan")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ChunksDeleted)
	assert.Empty(t, e.holders("orphan"))
	assert.Equal(t, models.ChunkState(""), e.state("orphan"))
}

func TestOrphanGraceDelaysDeletion(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.rec.cfg.OrphanGrace = 150 * time.Millisecond
	e.c.put("w1", "orphan", "brief")
	assert.Zero(t, e.rec.Pass(e.ctx).ChunksDeleted, "first sighting starts the clock")
	assert.True(t, e.c.holds("w1", "orphan"))

	require.NoError(t, e.c.DeleteChunk(e.ctx, "w1:1", "brief"))
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 1, e.rec.Pass(e.ctx).ChunksDeleted, "the orphan seen for the whole grace goes")
	assert.False(t, e.c.holds("w1", "orphan"))
	e.c.put("w1", "brief")
	assert.Zero(t, e.rec.Pass(e.ctx).ChunksDeleted)
	assert.True(t, e.c.holds("w1", "brief"), "a file that vanished and came back restarts its clock")
}

func TestOrphanThatGainsAReferenceIsNeverDeleted(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.rec.cfg.OrphanGrace = 50 * time.Millisecond
	e.c.put("w1", "c")
	e.c.put("w2", "c")
	e.rec.Pass(e.ctx)
	e.object("late", models.StatusCommitted, "c")
	time.Sleep(80 * time.Millisecond)
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.ChunksDeleted)
	assert.Equal(t, 1, rep.ChunksAdopted)
	assert.Equal(t, []string{"w1", "w2"}, e.holders("c"))
}

func TestPassAdoptsReferencedChunkWhoseRecordWasLost(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.object("o", models.StatusCommitted, "c")
	e.c.put("w1", "c")
	e.c.put("w3", "c")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.ChunksAdopted)
	assert.Zero(t, rep.ChunksDeleted, "a referenced chunk is never garbage")
	assert.Equal(t, models.ChunkDurable, e.state("c"))
	assert.Equal(t, []string{"w1", "w3"}, e.holders("c"))
}

func TestPassDeletesChunksOfDeletedAndFailedObjectsAndPurgesThem(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.object("deleted", models.StatusDeleted, "a")
	e.object("failed", models.StatusFailed, "b")
	e.object("live", models.StatusCommitted, "keep")
	e.chunk("a", models.ChunkDurable, "w1", "w2")
	e.chunk("b", models.ChunkPending, "w1")
	e.chunk("keep", models.ChunkDurable, "w1", "w2")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 2, rep.ChunksDeleted)
	assert.Equal(t, 2, rep.ObjectsPurged)
	for _, id := range []string{"deleted", "failed"} {
		_, err := e.store.GetObject(e.ctx, id)
		assert.ErrorIs(t, err, models.ErrNotFound)
	}
	assert.Equal(t, []string{"w1", "w2"}, e.holders("keep"))
}

func TestPassExpiresStaleUploadAndFreesItsChunks(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	require.NoError(t, e.store.SaveObject(e.ctx, &models.ObjectManifest{ID: "stuck", Status: models.StatusStaged, ModifiedAt: time.Now().Add(-time.Hour).UTC()}))
	require.NoError(t, e.store.SaveChunkRefs(e.ctx, "stuck", []models.ChunkRef{{ChunkID: "half"}}))
	e.chunk("half", models.ChunkPending, "w1")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.StaleUploadsFailed)
	assert.Equal(t, 1, rep.ChunksDeleted)
	assert.Equal(t, 1, rep.ObjectsPurged)
	assert.Empty(t, e.holders("half"))
}

func TestPassLeavesActiveUploadAlone(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.object("uploading", models.StatusStaged, "half")
	e.chunk("half", models.ChunkPending, "w1")
	rep := e.rec.Pass(e.ctx)
	assert.Zero(t, rep.StaleUploadsFailed)
	assert.Zero(t, rep.ChunksDeleted)
	assert.Equal(t, models.ChunkPending, e.state("half"))
	assert.True(t, e.c.holds("w1", "half"))
	assert.Empty(t, e.c.callsWith("COPY"), "PENDING chunks are the upload's business")
}

func TestPassSyncsRefCounts(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.object("a", models.StatusCommitted, "c")
	e.object("b", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w1", "w2")
	e.rec.Pass(e.ctx)
	r, _ := e.store.GetChunk(e.ctx, "c")
	assert.Equal(t, 2, r.RefCount)
}

// --- Pass: lost data ---

func TestPassReportsLostChunk(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.object("o", models.StatusCommitted, "gone", "norecord")
	require.NoError(t, e.store.SaveChunk(e.ctx, &models.ChunkRecord{ChunkID: "gone", State: models.ChunkDurable, Replicas: reps("w1", "w2")}))
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 2, rep.LostChunks)
	assert.Zero(t, rep.ChunksDeleted)
	assert.Equal(t, models.ChunkDurable, e.state("gone"), "a lost chunk's record is kept for operators")
}

func TestPassDoesNotCallChunkLostWhileAReplicaIsOffline(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	e.object("o", models.StatusCommitted, "c")
	e.chunk("c", models.ChunkDurable, "w1", "w2")
	require.NoError(t, e.c.DeleteChunk(e.ctx, "w1:1", "c"))
	e.c.setOnline("w2", false)
	assert.Zero(t, e.rec.Pass(e.ctx).LostChunks)
}

// --- Run, Trigger, Status ---

func TestRunTriggerAndJoinAndStop(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	e.c.setOnline("w3", false)
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan error, 1)
	go func() { done <- e.rec.Run(ctx) }()

	waitPasses := func(n int64) {
		t.Helper()
		require.Eventually(t, func() bool { return e.rec.Status().Passes >= n }, 3*time.Second, 5*time.Millisecond)
	}
	assert.True(t, e.rec.Trigger())
	waitPasses(1)

	e.c.put("w3", "orphan")
	e.c.setOnline("w3", true)
	waitPasses(2)
	require.Eventually(t, func() bool { return !e.c.holds("w3", "orphan") }, 3*time.Second, 5*time.Millisecond, "a rejoining worker is reconciled without waiting an interval")

	assert.ErrorIs(t, e.rec.Run(ctx), ErrAlreadyRunning)
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestTriggerCoalesces(t *testing.T) {
	e := newEnv(t, "w1")
	assert.True(t, e.rec.Trigger())
	assert.False(t, e.rec.Trigger())
}

func TestStatusJSONAndCopy(t *testing.T) {
	e := newEnv(t, "w1")
	st := e.rec.Status()
	assert.Nil(t, st.LastPass)
	b, _ := json.Marshal(st)
	assert.JSONEq(t, `{"running":false,"passes":0,"interval_seconds":3600,"staged_timeout_seconds":60,"orphan_grace_seconds":0}`, string(b))
	e.rec.Pass(e.ctx)
	st = e.rec.Status()
	require.NotNil(t, st.LastPass)
	st.LastPass.ChunksDeleted = 99
	assert.Zero(t, e.rec.Status().LastPass.ChunksDeleted, "Status returns a copy")
	b, _ = json.Marshal(e.rec.Status())
	for _, k := range []string{"workers_scanned", "chunks_deleted", "excess_deleted", "replicas_restored", "lost_chunks", "objects_purged", "stale_uploads_failed", "chunks_adopted", "errors"} {
		assert.Contains(t, string(b), `"`+k+`"`)
	}
}

func TestConcurrentPassesAndCollectsAreSafe(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	for i := 0; i < 30; i++ {
		ch := fmt.Sprintf("c%d", i)
		e.chunk(ch, models.ChunkDurable, "w1", "w2")
		e.c.put("w3", ch)
		if i%2 == 0 {
			e.object("o"+ch, models.StatusCommitted, ch)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); e.rec.Pass(e.ctx) }()
		go func(i int) {
			defer wg.Done()
			e.col.Collect(e.ctx, []string{fmt.Sprintf("c%d", i*2+1)}, nil) //nolint:errcheck
		}(i)
	}
	wg.Wait()
	e.rec.Pass(e.ctx)
	for i := 0; i < 30; i++ {
		ch := fmt.Sprintf("c%d", i)
		if i%2 == 0 {
			assert.Equal(t, []string{"w1", "w2"}, e.holders(ch), ch)
		} else {
			assert.Empty(t, e.holders(ch), ch)
			assert.Equal(t, models.ChunkState(""), e.state(ch), ch)
		}
	}
}

// --- scenarios ported from the earlier GC implementation ---

func TestPassStagedManifestWithNoChunksIsExpiredAndPurged(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	require.NoError(t, e.store.SaveObject(e.ctx, &models.ObjectManifest{ID: "empty", Status: models.StatusStaged, ModifiedAt: time.Now().Add(-time.Hour).UTC()}))
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.StaleUploadsFailed)
	assert.Equal(t, 1, rep.ObjectsPurged)
	_, err := e.store.GetObject(e.ctx, "empty")
	assert.ErrorIs(t, err, models.ErrNotFound)
}

func TestPassPendingChunksWithoutAnyReferenceAreDeleted(t *testing.T) {
	e := newEnv(t, "w1", "w2", "w3")
	for _, id := range []string{"p1", "p2", "p3"} {
		e.chunk(id, models.ChunkPending, "w1", "w2")
	}
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 3, rep.ChunksDeleted)
	for _, id := range []string{"p1", "p2", "p3"} {
		assert.Empty(t, e.holders(id), id)
		assert.Equal(t, models.ChunkState(""), e.state(id), id)
	}
}

func TestPassDurableChunkNobodyReferencesIsDeletedEvenWithStaleRefCount(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	require.NoError(t, e.store.SaveChunk(e.ctx, &models.ChunkRecord{ChunkID: "d", State: models.ChunkDurable, RefCount: 7, Replicas: reps("w1", "w2")}))
	e.c.put("w1", "d")
	e.c.put("w2", "d")
	assert.Equal(t, 1, e.rec.Pass(e.ctx).ChunksDeleted, "ref_count is a cache; references decide")
	assert.Empty(t, e.holders("d"))
}

func TestPassExpiredUploadWithDurableChunksIsFullyCleaned(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	require.NoError(t, e.store.SaveObject(e.ctx, &models.ObjectManifest{ID: "stuck", Status: models.StatusStaged, ModifiedAt: time.Now().Add(-time.Hour).UTC()}))
	require.NoError(t, e.store.SaveChunkRefs(e.ctx, "stuck", []models.ChunkRef{{ChunkID: "a", Index: 0}, {ChunkID: "b", Index: 1}, {ChunkID: "shared", Index: 2}}))
	e.chunk("a", models.ChunkDurable, "w1", "w2")
	e.chunk("b", models.ChunkDurable, "w1", "w2")
	e.chunk("shared", models.ChunkDurable, "w1", "w2")
	e.object("keeper", models.StatusCommitted, "shared")
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 2, rep.ChunksDeleted)
	assert.Empty(t, e.holders("a"))
	assert.Empty(t, e.holders("b"))
	assert.Equal(t, []string{"w1", "w2"}, e.holders("shared"), "a chunk shared with a live object stays")
}

func TestPassWithCancelledContextReturnsWithoutPanicking(t *testing.T) {
	e := newEnv(t, "w1", "w2")
	for i := 0; i < 20; i++ {
		e.chunk(fmt.Sprintf("c%d", i), models.ChunkPending, "w1", "w2")
	}
	ctx, cancel := context.WithCancel(e.ctx)
	cancel()
	assert.NotPanics(t, func() { e.rec.Pass(ctx) })
	done := make(chan error, 1)
	go func() { done <- e.rec.Run(ctx) }()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run ignored the cancelled context")
	}
}

func TestPassPurgesStaleIdempotencyLocksButKeepsCommittedKeys(t *testing.T) {
	e := newEnv(t, "w1")
	e.rec.cfg.StagedTimeout = time.Millisecond
	_, err := e.store.AcquireIdempotencyLock(e.ctx, "crashed")
	require.NoError(t, err)
	_, err = e.store.AcquireIdempotencyLock(e.ctx, "done")
	require.NoError(t, err)
	require.NoError(t, e.store.CommitIdempotencyResult(e.ctx, "done", 201, []byte("{}")))
	time.Sleep(20 * time.Millisecond)
	rep := e.rec.Pass(e.ctx)
	assert.Equal(t, 1, rep.KeysPurged)
	existing, err := e.store.AcquireIdempotencyLock(e.ctx, "crashed")
	require.NoError(t, err)
	assert.Nil(t, existing, "the crashed request's key can be reused")
	existing, err = e.store.AcquireIdempotencyLock(e.ctx, "done")
	require.NoError(t, err)
	require.NotNil(t, existing)
	assert.Equal(t, "COMMITTED", existing.Status)
}
