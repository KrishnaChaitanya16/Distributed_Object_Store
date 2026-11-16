package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

func stagedObject(t *testing.T, s *MetadataStore, id string) {
	t.Helper()
	require.NoError(t, s.SaveObject(context.Background(), testObject(id, models.StatusStaged)))
}

func objectWithRefs(t *testing.T, s *MetadataStore, id string, status models.ObjectStatus, chunks ...string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.SaveObject(ctx, testObject(id, status)))
	refs := make([]ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = ChunkRef{ChunkID: c, Index: i}
	}
	require.NoError(t, s.SaveChunkRefs(ctx, id, refs))
}

func chunkIn(t *testing.T, s *MetadataStore, id string, state models.ChunkState, workers ...string) {
	t.Helper()
	reps := make([]models.Replica, len(workers))
	for i, w := range workers {
		reps[i] = models.Replica{WorkerID: w, WorkerAddr: w + ":1"}
	}
	require.NoError(t, s.SaveChunk(context.Background(), &models.ChunkRecord{ChunkID: id, State: state, Replicas: reps}))
}

func stateOf(t *testing.T, s *MetadataStore, id string) models.ChunkState {
	t.Helper()
	c, err := s.GetChunk(context.Background(), id)
	if err == models.ErrNotFound {
		return ""
	}
	require.NoError(t, err)
	return c.State
}

// --- AddChunkRef ---

func TestAddChunkRef_RequiresStagedObject(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	ref := ChunkRef{ChunkID: "c1"}
	_, err := s.AddChunkRef(ctx, "missing", ref)
	assert.ErrorIs(t, err, models.ErrNotStaged)
	for _, st := range []models.ObjectStatus{models.StatusCommitted, models.StatusFailed, models.StatusDeleted} {
		require.NoError(t, s.SaveObject(ctx, testObject("o-"+string(st), st)))
		_, err := s.AddChunkRef(ctx, "o-"+string(st), ref)
		assert.ErrorIs(t, err, models.ErrNotStaged, st)
		refs, _ := s.GetChunkRefs(ctx, "o-"+string(st))
		assert.Empty(t, refs, "no ref may be written for a %s object", st)
	}
}

func TestAddChunkRef_PersistsRefTouchesObjectAndReturnsRecord(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	obj := testObject("o1", models.StatusStaged)
	obj.ModifiedAt = time.Now().Add(-time.Hour).UTC()
	require.NoError(t, s.SaveObject(ctx, obj))

	rec, err := s.AddChunkRef(ctx, "o1", ChunkRef{ChunkID: "c1", Index: 0, Size: 7})
	require.NoError(t, err)
	assert.Nil(t, rec, "no record yet")

	chunkIn(t, s, "c2", models.ChunkDurable, "w1", "w2")
	rec, err = s.AddChunkRef(ctx, "o1", ChunkRef{ChunkID: "c2", Index: 1})
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, models.ChunkDurable, rec.State)

	refs, err := s.GetChunkRefs(ctx, "o1")
	require.NoError(t, err)
	require.Len(t, refs, 2)
	assert.Equal(t, "c1", refs[0].ChunkID)
	assert.Equal(t, 7, refs[0].Size)
	got, _ := s.GetObject(ctx, "o1")
	assert.WithinDuration(t, time.Now(), got.ModifiedAt, 5*time.Second, "an active upload must look fresh to the stale-upload sweep")
}

// --- PrepareChunk ---

func TestPrepareChunk_States(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	want := &models.ChunkRecord{ChunkID: "new", SizeBytes: 9, State: models.ChunkDurable, Replicas: []models.Replica{{WorkerID: "w1"}, {WorkerID: "w2"}}}

	got, err := s.PrepareChunk(ctx, want, false)
	require.NoError(t, err)
	assert.Equal(t, models.ChunkPending, got.State, "a fresh record always starts PENDING")
	assert.Equal(t, models.ChunkPending, stateOf(t, s, "new"))

	chunkIn(t, s, "pend", models.ChunkPending, "w1", "w2")
	got, err = s.PrepareChunk(ctx, &models.ChunkRecord{ChunkID: "pend", Replicas: []models.Replica{{WorkerID: "w3"}, {WorkerID: "w4"}}}, false)
	require.NoError(t, err)
	assert.Equal(t, "w1", got.Replicas[0].WorkerID, "existing PENDING keeps its replicas unless replace is set")

	got, err = s.PrepareChunk(ctx, &models.ChunkRecord{ChunkID: "pend", SizeBytes: 3, Replicas: []models.Replica{{WorkerID: "w3"}, {WorkerID: "w4"}}}, true)
	require.NoError(t, err)
	assert.Equal(t, "w3", got.Replicas[0].WorkerID)
	stored, _ := s.GetChunk(ctx, "pend")
	assert.Equal(t, "w4", stored.Replicas[1].WorkerID)

	chunkIn(t, s, "dur", models.ChunkDurable, "w1", "w2")
	got, err = s.PrepareChunk(ctx, &models.ChunkRecord{ChunkID: "dur", Replicas: []models.Replica{{WorkerID: "w3"}}}, true)
	require.NoError(t, err)
	assert.Equal(t, models.ChunkDurable, got.State)
	assert.Equal(t, "w1", got.Replicas[0].WorkerID, "a DURABLE record is never rewritten")

	chunkIn(t, s, "pd", models.ChunkPendingDelete, "w1")
	_, err = s.PrepareChunk(ctx, &models.ChunkRecord{ChunkID: "pd"}, true)
	assert.ErrorIs(t, err, models.ErrChunkBusy)
}

// --- MarkChunkDurable ---

func TestMarkChunkDurable(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	chunkIn(t, s, "p", models.ChunkPending)
	require.NoError(t, s.MarkChunkDurable(ctx, "p"))
	assert.Equal(t, models.ChunkDurable, stateOf(t, s, "p"))
	require.NoError(t, s.MarkChunkDurable(ctx, "p"), "idempotent")
	chunkIn(t, s, "pd", models.ChunkPendingDelete)
	assert.ErrorIs(t, s.MarkChunkDurable(ctx, "pd"), models.ErrChunkBusy)
	assert.Equal(t, models.ChunkPendingDelete, stateOf(t, s, "pd"))
	assert.ErrorIs(t, s.MarkChunkDurable(ctx, "none"), models.ErrNotFound)
}

// --- CommitObject / MarkObjectDeleted ---

func TestCommitObject_OnlyFromStaged(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	stagedObject(t, s, "o1")
	obj, _ := s.GetObject(ctx, "o1")
	obj.SizeBytes = 42
	require.NoError(t, s.CommitObject(ctx, obj))
	got, _ := s.GetObject(ctx, "o1")
	assert.Equal(t, models.StatusCommitted, got.Status)
	assert.Equal(t, int64(42), got.SizeBytes)

	assert.ErrorIs(t, s.CommitObject(ctx, obj), models.ErrNotStaged, "double commit")
	require.NoError(t, s.SaveObject(ctx, testObject("f", models.StatusFailed)))
	assert.ErrorIs(t, s.CommitObject(ctx, testObject("f", models.StatusStaged)), models.ErrNotStaged, "an expired upload cannot commit")
	got, _ = s.GetObject(ctx, "f")
	assert.Equal(t, models.StatusFailed, got.Status)
	assert.ErrorIs(t, s.CommitObject(ctx, testObject("missing", models.StatusStaged)), models.ErrNotStaged)
}

func TestMarkObjectDeleted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	require.NoError(t, s.SaveObject(ctx, testObject("c", models.StatusCommitted)))
	require.NoError(t, s.MarkObjectDeleted(ctx, "c"))
	got, _ := s.GetObject(ctx, "c")
	assert.Equal(t, models.StatusDeleted, got.Status)
	assert.ErrorIs(t, s.MarkObjectDeleted(ctx, "c"), models.ErrNotFound)
	for _, st := range []models.ObjectStatus{models.StatusStaged, models.StatusFailed} {
		require.NoError(t, s.SaveObject(ctx, testObject("x"+string(st), st)))
		assert.ErrorIs(t, s.MarkObjectDeleted(ctx, "x"+string(st)), models.ErrNotFound)
	}
	assert.ErrorIs(t, s.MarkObjectDeleted(ctx, "missing"), models.ErrNotFound)
}

func TestMarkObjectDeleted_ConcurrentExactlyOneWins(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.SaveObject(context.Background(), testObject("c", models.StatusCommitted)))
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.MarkObjectDeleted(context.Background(), "c") == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), wins.Load())
}

// --- FailStaleUploads ---

func TestFailStaleUploads(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	old := time.Now().Add(-time.Hour).UTC()
	for id, st := range map[string]models.ObjectStatus{"stale": models.StatusStaged, "oldCommitted": models.StatusCommitted, "oldFailed": models.StatusFailed} {
		o := testObject(id, st)
		o.ModifiedAt = old
		require.NoError(t, s.SaveObject(ctx, o))
	}
	fresh := testObject("fresh", models.StatusStaged)
	fresh.ModifiedAt = time.Now().UTC()
	require.NoError(t, s.SaveObject(ctx, fresh))
	active := testObject("active", models.StatusStaged)
	active.ModifiedAt = old
	require.NoError(t, s.SaveObject(ctx, active))
	_, err := s.AddChunkRef(ctx, "active", ChunkRef{ChunkID: "c"})
	require.NoError(t, err)

	ids, err := s.FailStaleUploads(ctx, time.Now().Add(-time.Minute))
	require.NoError(t, err)
	assert.Equal(t, []string{"stale"}, ids)
	for id, want := range map[string]models.ObjectStatus{"stale": models.StatusFailed, "oldCommitted": models.StatusCommitted, "fresh": models.StatusStaged, "active": models.StatusStaged} {
		got, _ := s.GetObject(ctx, id)
		assert.Equal(t, want, got.Status, id)
	}
	ids, err = s.FailStaleUploads(ctx, time.Now().Add(-time.Minute))
	require.NoError(t, err)
	assert.Empty(t, ids, "second run finds nothing")
}

// --- LiveChunkRefs ---

func TestLiveChunkRefs_CountsCommittedAndStagedOnly(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	objectWithRefs(t, s, "a", models.StatusCommitted, "x", "y", "x")
	objectWithRefs(t, s, "ab", models.StatusStaged, "y")
	objectWithRefs(t, s, "abc", models.StatusDeleted, "z", "x")
	objectWithRefs(t, s, "abcd", models.StatusFailed, "w")
	live, err := s.LiveChunkRefs(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"x": 2, "y": 2}, live)
}

// --- ClaimChunksForDeletion ---

func TestClaimChunksForDeletion(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	objectWithRefs(t, s, "live", models.StatusCommitted, "used")
	objectWithRefs(t, s, "inflight", models.StatusStaged, "uploading")
	objectWithRefs(t, s, "gone", models.StatusDeleted, "garbage", "used")
	chunkIn(t, s, "used", models.ChunkDurable, "w1", "w2")
	chunkIn(t, s, "uploading", models.ChunkPending, "w1", "w2")
	chunkIn(t, s, "garbage", models.ChunkDurable, "w1", "w2")
	chunkIn(t, s, "abandoned", models.ChunkPending, "w3")
	chunkIn(t, s, "half", models.ChunkPendingDelete, "w4")

	holders := map[string][]models.Replica{
		"garbage": {{WorkerID: "w3", WorkerAddr: "w3:1"}, {WorkerID: "w1", WorkerAddr: "w1:1"}},
		"orphan":  {{WorkerID: "w2", WorkerAddr: "w2:1"}},
	}
	claimed, err := s.ClaimChunksForDeletion(ctx, []string{"used", "uploading", "garbage", "garbage", "abandoned", "half", "orphan", "unknown"}, holders)
	require.NoError(t, err)

	byID := map[string]*models.ChunkRecord{}
	for _, c := range claimed {
		byID[c.ChunkID] = c
	}
	assert.Len(t, claimed, 4, "garbage, abandoned, half and orphan; duplicates and live chunks never")
	for _, id := range []string{"garbage", "abandoned", "half", "orphan"} {
		require.Contains(t, byID, id)
		assert.Equal(t, models.ChunkPendingDelete, stateOf(t, s, id), id)
	}
	assert.Equal(t, models.ChunkDurable, stateOf(t, s, "used"))
	assert.Equal(t, models.ChunkPending, stateOf(t, s, "uploading"))
	assert.Equal(t, models.ChunkState(""), stateOf(t, s, "unknown"), "a file nobody holds and nobody knows creates no record")

	var ws []string
	for _, r := range byID["garbage"].Replicas {
		ws = append(ws, r.WorkerID)
	}
	assert.Equal(t, []string{"w1", "w2", "w3"}, ws, "listed replicas first, extra holders merged once")
	assert.Equal(t, "w2", byID["orphan"].Replicas[0].WorkerID)
}

// TestClaimNeverRacesAnUpload interleaves the claim with an upload registering the same chunk; whichever transaction runs first, the upload must either keep the chunk live or see it is being deleted.
func TestClaimNeverRacesAnUpload(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		obj := fmt.Sprintf("up%d", i)
		chunk := fmt.Sprintf("c%d", i)
		chunkIn(t, s, chunk, models.ChunkDurable, "w1", "w2")
		stagedObject(t, s, obj)

		var claimed []*models.ChunkRecord
		var rec *models.ChunkRecord
		var cErr, aErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); claimed, cErr = s.ClaimChunksForDeletion(ctx, []string{chunk}, nil) }()
		go func() { defer wg.Done(); rec, aErr = s.AddChunkRef(ctx, obj, ChunkRef{ChunkID: chunk}) }()
		wg.Wait()
		require.NoError(t, cErr)
		require.NoError(t, aErr)
		if len(claimed) == 1 {
			require.NotNil(t, rec)
			assert.Equal(t, models.ChunkPendingDelete, rec.State, "upload registered after the claim must see the chunk is being deleted")
		} else {
			assert.Equal(t, models.ChunkDurable, rec.State, "a claim after the upload registered must leave the chunk alone")
		}
	}
}

// --- RemoveDeletedChunk ---

func TestRemoveDeletedChunk(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	chunkIn(t, s, "pd", models.ChunkPendingDelete)
	require.NoError(t, s.RemoveDeletedChunk(ctx, "pd"))
	assert.Equal(t, models.ChunkState(""), stateOf(t, s, "pd"))
	require.NoError(t, s.RemoveDeletedChunk(ctx, "pd"), "already gone is fine")
	chunkIn(t, s, "d", models.ChunkDurable)
	assert.ErrorIs(t, s.RemoveDeletedChunk(ctx, "d"), models.ErrChunkBusy)
	assert.Equal(t, models.ChunkDurable, stateOf(t, s, "d"))
}

// --- AdoptChunks ---

func TestAdoptChunks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	objectWithRefs(t, s, "o", models.StatusCommitted, "lostrecord", "known", "lostrecord")
	chunkIn(t, s, "known", models.ChunkDurable, "w1", "w2")
	holders := map[string][]models.Replica{
		"lostrecord": {{WorkerID: "w2"}, {WorkerID: "w3"}},
		"known":      {{WorkerID: "w4"}},
		"unused":     {{WorkerID: "w1"}},
	}
	adopted, err := s.AdoptChunks(ctx, holders)
	require.NoError(t, err)
	assert.Equal(t, []string{"lostrecord"}, adopted)
	rec, err := s.GetChunk(ctx, "lostrecord")
	require.NoError(t, err)
	assert.Equal(t, models.ChunkDurable, rec.State)
	assert.Equal(t, 2, rec.RefCount)
	assert.Len(t, rec.Replicas, 2)
	known, _ := s.GetChunk(ctx, "known")
	assert.Len(t, known.Replicas, 2, "existing records are never touched")
	assert.Equal(t, models.ChunkState(""), stateOf(t, s, "unused"), "unreferenced files are garbage, not adopted")
}

// --- PurgeDeadObjects ---

func TestPurgeDeadObjects_RemovesOnlyDeadManifestsAndTheirRefs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	objectWithRefs(t, s, "abc", models.StatusDeleted, "x")
	objectWithRefs(t, s, "abcd", models.StatusCommitted, "y")
	objectWithRefs(t, s, "ab", models.StatusFailed, "z")
	objectWithRefs(t, s, "s", models.StatusStaged, "w")
	n, err := s.PurgeDeadObjects(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	for _, id := range []string{"abc", "ab"} {
		_, err := s.GetObject(ctx, id)
		assert.ErrorIs(t, err, models.ErrNotFound, id)
		refs, _ := s.GetChunkRefs(ctx, id)
		assert.Empty(t, refs, id)
	}
	refs, _ := s.GetChunkRefs(ctx, "abcd")
	assert.Len(t, refs, 1, "a neighbouring key prefix must survive")
	refs, _ = s.GetChunkRefs(ctx, "s")
	assert.Len(t, refs, 1)
	n, err = s.PurgeDeadObjects(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// --- inline ref counting ---

func TestCommitAndDeleteMaintainRefCountsAtomically(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	chunkIn(t, s, "x", models.ChunkPending)
	chunkIn(t, s, "y", models.ChunkPending)
	for _, o := range []string{"o1", "o2"} {
		stagedObject(t, s, o)
		for i, c := range []string{"x", "y", "x"} {
			_, err := s.AddChunkRef(ctx, o, ChunkRef{ChunkID: c, Index: i})
			require.NoError(t, err)
		}
	}
	count := func(id string) int {
		c, err := s.GetChunk(ctx, id)
		require.NoError(t, err)
		return c.RefCount
	}
	assert.Equal(t, 0, count("x"), "an upload in progress is not yet counted")

	o1, _ := s.GetObject(ctx, "o1")
	require.NoError(t, s.CommitObject(ctx, o1))
	assert.Equal(t, 2, count("x"), "one per reference, so a chunk used twice counts twice")
	assert.Equal(t, 1, count("y"))
	o2, _ := s.GetObject(ctx, "o2")
	require.NoError(t, s.CommitObject(ctx, o2))
	assert.Equal(t, 4, count("x"))

	require.NoError(t, s.MarkObjectDeleted(ctx, "o1"))
	assert.Equal(t, 2, count("x"))
	assert.Equal(t, 1, count("y"))
	assert.ErrorIs(t, s.MarkObjectDeleted(ctx, "o1"), models.ErrNotFound)
	assert.Equal(t, 2, count("x"), "a rejected second delete changes nothing")
	require.NoError(t, s.MarkObjectDeleted(ctx, "o2"))
	assert.Equal(t, 0, count("x"))
	assert.Equal(t, 0, count("y"))
}

func TestRefCountsNeverGoNegativeAndSkipMissingRecords(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	objectWithRefs(t, s, "o", models.StatusCommitted, "known", "norecord")
	chunkIn(t, s, "known", models.ChunkDurable)
	require.NoError(t, s.MarkObjectDeleted(ctx, "o"))
	c, _ := s.GetChunk(ctx, "known")
	assert.Equal(t, 0, c.RefCount, "floor at zero")

	stagedObject(t, s, "s")
	_, err := s.AddChunkRef(ctx, "s", ChunkRef{ChunkID: "ghost"})
	require.NoError(t, err)
	obj, _ := s.GetObject(ctx, "s")
	require.NoError(t, s.CommitObject(ctx, obj), "a lost chunk record must not block a commit")
}

func TestConcurrentCommitsCountExactly(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	chunkIn(t, s, "shared", models.ChunkDurable)
	const n = 25
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("o%d", i)
		stagedObject(t, s, id)
		_, err := s.AddChunkRef(ctx, id, ChunkRef{ChunkID: "shared"})
		require.NoError(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			obj, _ := s.GetObject(ctx, id)
			assert.NoError(t, s.CommitObject(ctx, obj))
		}()
	}
	wg.Wait()
	c, _ := s.GetChunk(ctx, "shared")
	assert.Equal(t, n, c.RefCount)
}

// --- SyncRefCounts ---

func TestSyncRefCounts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	objectWithRefs(t, s, "o1", models.StatusCommitted, "a", "b")
	objectWithRefs(t, s, "o2", models.StatusStaged, "a")
	objectWithRefs(t, s, "o3", models.StatusDeleted, "b")
	for _, id := range []string{"a", "b", "c"} {
		chunkIn(t, s, id, models.ChunkDurable)
	}
	chunkIn(t, s, "pd", models.ChunkPendingDelete)
	require.NoError(t, s.IncrementChunkRefCount(ctx, "c"))
	require.NoError(t, s.SyncRefCounts(ctx))
	for id, want := range map[string]int{"a": 1, "b": 1, "c": 0, "pd": 0} {
		c, _ := s.GetChunk(ctx, id)
		assert.Equal(t, want, c.RefCount, id)
	}
}

// --- idempotency key release and purge ---

func putKey(t *testing.T, s *MetadataStore, key, status string, created time.Time) {
	t.Helper()
	rec := models.IdempotencyRecord{Key: key, Status: status, ExpiresAt: created.Add(idempotencyTTL)}
	data, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketIdempotency).Put([]byte(key), data) }))
}

func keyStatus(t *testing.T, s *MetadataStore, key string) string {
	t.Helper()
	var st string
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		if d := tx.Bucket(bucketIdempotency).Get([]byte(key)); d != nil {
			var rec models.IdempotencyRecord
			require.NoError(t, json.Unmarshal(d, &rec))
			st = rec.Status
		}
		return nil
	}))
	return st
}

func TestReleaseIdempotencyLock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, err := s.AcquireIdempotencyLock(ctx, "k")
	require.NoError(t, err)
	require.NoError(t, s.ReleaseIdempotencyLock(ctx, "k"))
	assert.Equal(t, "", keyStatus(t, s, "k"))
	existing, err := s.AcquireIdempotencyLock(ctx, "k")
	require.NoError(t, err)
	assert.Nil(t, existing, "a released key can be acquired again")

	require.NoError(t, s.CommitIdempotencyResult(ctx, "k", 201, []byte(`{"object_id":"x"}`)))
	require.NoError(t, s.ReleaseIdempotencyLock(ctx, "k"))
	assert.Equal(t, "COMMITTED", keyStatus(t, s, "k"), "a committed key must never be released")
	require.NoError(t, s.ReleaseIdempotencyLock(ctx, "never-seen"), "unknown key is not an error")
}

func TestPurgeStaleIdempotencyLocks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	putKey(t, s, "old-processing", "PROCESSING", now.Add(-time.Hour))
	putKey(t, s, "fresh-processing", "PROCESSING", now.Add(-time.Second))
	putKey(t, s, "old-committed", "COMMITTED", now.Add(-time.Hour))
	n, err := s.PurgeStaleIdempotencyLocks(ctx, now.Add(-time.Minute))
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, "", keyStatus(t, s, "old-processing"))
	assert.Equal(t, "PROCESSING", keyStatus(t, s, "fresh-processing"), "a request that may still be running keeps its key")
	assert.Equal(t, "COMMITTED", keyStatus(t, s, "old-committed"), "a committed key must keep replaying")
	n, err = s.PurgeStaleIdempotencyLocks(ctx, now.Add(-time.Minute))
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestPurgeExpiredIdempotencyKeysDropsOnlyExpired(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	putKey(t, s, "expired", "COMMITTED", now.Add(-25*time.Hour))
	putKey(t, s, "alive", "COMMITTED", now.Add(-time.Hour))
	require.NoError(t, s.PurgeExpiredIdempotencyKeys(context.Background()))
	assert.Equal(t, "", keyStatus(t, s, "expired"))
	assert.Equal(t, "COMMITTED", keyStatus(t, s, "alive"))
}
