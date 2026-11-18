// Real-node tests for the five priority correctness scenarios identified in the
// production-readiness review:
//   1. Scrubber quarantine + read fallback to secondary (Scenario 4)
//   2. Scrubber quarantine + reconciler healing (Scenario 4)
//   3. Controller crash: orphan PENDING chunk from stale STAGED upload (Scenario 2)
//   4. Controller crash: orphan DURABLE chunk from stale STAGED upload (Scenario 2)
//   5. Dedup + all refs deleted + GC → chunk cleaned up (Scenario 1)
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/scrubber"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// corruptChunkCRC overwrites the last 4 bytes of a .bin chunk file with 0xFF,
// guaranteeing the stored CRC32C footer no longer matches the data.
func corruptChunkCRC(t *testing.T, chunkPath string) {
	t.Helper()
	f, err := os.OpenFile(chunkPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer f.Close()
	info, err := f.Stat()
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(4), "chunk file must be larger than 4 bytes")
	_, err = f.Seek(info.Size()-4, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	require.NoError(t, err)
}

// runOneScrubPass creates a fresh Scrubber for the given store, fires one pass,
// waits up to 10 s for it to complete, then cancels the scrubber goroutine.
func runOneScrubPass(t *testing.T, store scrubber.Store) {
	t.Helper()
	scrub, err := scrubber.New(store, scrubber.Config{
		Interval: time.Hour, // won't fire on its own timer
		Pause:    0,
		Logf:     t.Logf,
	})
	require.NoError(t, err)
	scrub.Trigger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		scrub.Run(ctx) //nolint:errcheck
		close(done)
	}()
	require.Eventually(t, func() bool {
		return scrub.Status().Passes >= 1
	}, 10*time.Second, 5*time.Millisecond, "scrubber must complete one pass")
	cancel()
	<-done
}

// primaryNodeFor returns the workerNode that is the primary for the first chunk
// of objectID.  Fails the test if not found.
func primaryNodeFor(t *testing.T, rc *reconCluster, objectID string) (*workerNode, string) {
	t.Helper()
	refs, err := rc.meta.GetChunkRefs(context.Background(), objectID)
	require.NoError(t, err)
	require.NotEmpty(t, refs)
	chunkID := refs[0].ChunkID
	primaryID := refs[0].PrimaryID
	for _, w := range rc.workers {
		if w.id == primaryID {
			return w, chunkID
		}
	}
	t.Fatalf("primary worker %s not found", primaryID)
	return nil, ""
}

// ── §4 Scrubber: quarantine corrupt chunk → download falls back to secondary ──

// TestScrubber_QuarantinesCorruptChunk_DownloadFallsBackToSecondary
// Corrupts the CRC footer of a chunk on the primary worker, runs the scrubber,
// and verifies the system still serves the file (via the secondary replica).
func TestScrubber_QuarantinesCorruptChunk_DownloadFallsBackToSecondary(t *testing.T) {
	rc := newReconCluster(t, 4)

	content := bytes.Repeat([]byte("scrubber-fallback-to-secondary-unique-k9"), 5)
	up := rc.upload(t, "scrub-fallback.bin", content)

	primaryNode, chunkID := primaryNodeFor(t, rc, up.ObjectID)
	chunkPath := filepath.Join(primaryNode.dataDir, "chunks", chunkID+".bin")

	// Corrupt the CRC footer — VerifyCRC will now return false.
	corruptChunkCRC(t, chunkPath)

	// Run scrubber directly on the primary worker's store.
	runOneScrubPass(t, primaryNode.store)

	// Chunk file must be gone from chunks/ and present in quarantine/.
	_, statErr := os.Stat(chunkPath)
	assert.True(t, os.IsNotExist(statErr),
		"corrupt chunk must be gone from chunks/ after scrubber quarantine")

	quarantineDir := filepath.Join(primaryNode.dataDir, "quarantine")
	entries, err := os.ReadDir(quarantineDir)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), chunkID) {
			found = true
			break
		}
	}
	assert.True(t, found, "quarantined chunk must appear in quarantine/")

	// Download must succeed: controller falls back to secondary replica.
	status, got := rc.download(t, up.ObjectID)
	require.Equal(t, http.StatusOK, status,
		"download must succeed via secondary after primary chunk quarantined")
	assert.Equal(t, content, got, "downloaded content must be byte-exact")
}

// ── §4 Scrubber: quarantine → reconciler heals the missing replica ─────────

// TestScrubber_QuarantineFollowedByReconcilerHeal
// After the scrubber quarantines a corrupt chunk, the reconciler detects the
// missing primary replica and re-replicates from the secondary.
func TestScrubber_QuarantineFollowedByReconcilerHeal(t *testing.T) {
	rc := newReconCluster(t, 4)

	content := bytes.Repeat([]byte("scrubber-heal-after-quarantine-unique-zz17"), 5)
	up := rc.upload(t, "heal-after-quarantine.bin", content)

	primaryNode, chunkID := primaryNodeFor(t, rc, up.ObjectID)
	chunkPath := filepath.Join(primaryNode.dataDir, "chunks", chunkID+".bin")

	corruptChunkCRC(t, chunkPath)
	runOneScrubPass(t, primaryNode.store)

	_, statErr := os.Stat(chunkPath)
	require.True(t, os.IsNotExist(statErr),
		"chunk must be quarantined before reconciler test")

	// Reconciler detects the missing primary replica and heals from the secondary.
	report := rc.triggerRecon(t)
	assert.GreaterOrEqual(t, report.ReplicasRestored, 1,
		"reconciler must restore at least 1 replica after quarantine")

	// After healing, the file must still download correctly.
	status, got := rc.download(t, up.ObjectID)
	require.Equal(t, http.StatusOK, status,
		"download must work after reconciler heals the quarantined chunk")
	assert.Equal(t, content, got, "content must be byte-exact after healing")
}

// ── §4 Scrubber: quarantine during concurrent downloads ───────────────────

// TestScrubber_QuarantineDuringConcurrentDownloads
// While 20 goroutines download the same file, the scrubber quarantines the
// primary chunk.  Every download must succeed; none may return corrupted bytes.
func TestScrubber_QuarantineDuringConcurrentDownloads(t *testing.T) {
	rc := newReconCluster(t, 4)

	content := bytes.Repeat([]byte("scrubber-concurrent-dl-quarantine-unique-m3"), 5)
	up := rc.upload(t, "concurrent-scrub.bin", content)

	primaryNode, chunkID := primaryNodeFor(t, rc, up.ObjectID)
	chunkPath := filepath.Join(primaryNode.dataDir, "chunks", chunkID+".bin")

	corruptChunkCRC(t, chunkPath)

	// Start concurrent downloads and scrubber simultaneously.
	var wg sync.WaitGroup
	errors := make(chan error, 25)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, got := rc.download(t, up.ObjectID)
			if status != http.StatusOK {
				errors <- fmt.Errorf("download returned %d", status)
				return
			}
			if !bytes.Equal(got, content) {
				errors <- fmt.Errorf("content mismatch: got %d bytes", len(got))
			}
		}()
	}

	// Run scrubber in parallel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		runOneScrubPass(t, primaryNode.store)
	}()

	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

// ── §2 Controller crash: orphan PENDING chunk cleaned by reconciler ─────────

// TestCrash_OrphanPendingChunk_CleanedByReconciler
// Simulates a controller crash after writing the chunk to the primary worker
// but before marking it DURABLE or committing the manifest.  State:
//   - STAGED manifest (15 min old, past the 10 min StagedTimeout)
//   - PENDING chunk record in bbolt
//   - Chunk file on primary worker's disk
//
// After one reconciler pass the stale STAGED manifest is expired, the PENDING
// chunk has no live references, and the GC removes the chunk from the worker.
func TestCrash_OrphanPendingChunk_CleanedByReconciler(t *testing.T) {
	cc := newCrashCluster(t, 4)
	ctx := context.Background()

	// Unique data → valid SHA256 chunk ID.
	data := bytes.Repeat([]byte("crash-orphan-pending-chunk-inject-unique-data-k7a"), 10)
	sum := sha256.Sum256(data)
	chunkID := hex.EncodeToString(sum[:])

	primaryWorker := cc.workers[0]
	secondaryWorker := cc.workers[1]

	// Write chunk bytes to primary worker's disk via the storage API.
	require.NoError(t,
		primaryWorker.store.Write(ctx, chunkID, bytes.NewReader(data), int64(len(data))),
		"must write chunk to primary worker disk")

	// Inject a STAGED manifest with an old creation time (simulates mid-upload crash).
	staleObjectID := "stale-upload-pending-" + chunkID[:8]
	require.NoError(t, cc.meta.SaveObject(ctx, &models.ObjectManifest{
		ID:         staleObjectID,
		Name:       "orphan-pending.bin",
		Status:     models.StatusStaged,
		ChunkCount: 1,
		SizeBytes:  int64(len(data)),
		CreatedAt:  time.Now().Add(-15 * time.Minute),
		ModifiedAt: time.Now().Add(-15 * time.Minute),
	}))

	// Register the chunk ref (links STAGED manifest → chunk).
	// AddChunkRef updates ModifiedAt to now; re-save with the old timestamp so
	// FailStaleUploads still treats it as expired (checks ModifiedAt).
	_, err := cc.meta.AddChunkRef(ctx, staleObjectID, models.ChunkRef{
		ChunkID:     chunkID,
		Index:       0,
		Offset:      0,
		Size:        len(data),
		PrimaryID:   primaryWorker.id,
		SecondaryID: secondaryWorker.id,
	})
	require.NoError(t, err)
	require.NoError(t, cc.meta.SaveObject(ctx, &models.ObjectManifest{
		ID:         staleObjectID,
		Name:       "orphan-pending.bin",
		Status:     models.StatusStaged,
		ChunkCount: 1,
		SizeBytes:  int64(len(data)),
		CreatedAt:  time.Now().Add(-15 * time.Minute),
		ModifiedAt: time.Now().Add(-15 * time.Minute),
	}))

	// Create PENDING chunk record in bbolt (crash happened before MarkChunkDurable).
	_, err = cc.meta.PrepareChunk(ctx, &models.ChunkRecord{
		ChunkID:   chunkID,
		SizeBytes: len(data),
		State:     models.ChunkPending,
		Replicas: []models.Replica{
			{
				WorkerID:   primaryWorker.id,
				WorkerAddr: strings.TrimPrefix(primaryWorker.srv.URL, "http://"),
			},
		},
		CreatedAt: time.Now().Add(-15 * time.Minute),
	}, false)
	require.NoError(t, err)

	// Sanity: chunk file exists before reconciler.
	chunkPath := filepath.Join(primaryWorker.dataDir, "chunks", chunkID+".bin")
	_, err = os.Stat(chunkPath)
	require.NoError(t, err, "chunk file must exist before reconciler runs")

	// Run reconciler: FailStaleUploads expires the STAGED manifest (>10 min old),
	// LiveChunkRefs returns 0 for the chunk, GC claims and deletes it.
	report := cc.triggerRecon(t)
	assert.GreaterOrEqual(t, report.StaleUploadsFailed, 1,
		"reconciler must expire the stale STAGED manifest")

	// Chunk file must be deleted from the primary worker's disk.
	require.Eventually(t, func() bool {
		_, err := os.Stat(chunkPath)
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond,
		"GC must delete the orphan PENDING chunk from the primary worker")

	// Stale manifest must not be downloadable.
	status, _ := cc.download(t, staleObjectID)
	assert.Equal(t, http.StatusNotFound, status,
		"stale STAGED manifest must not be downloadable after expiry")
}

// ── §2 Controller crash: orphan DURABLE chunk cleaned by reconciler ─────────

// TestCrash_OrphanDurableChunk_CleanedByReconciler
// Simulates a controller crash after both worker replicas received the chunk and
// MarkChunkDurable succeeded, but before CommitObject.  State:
//   - STAGED manifest (15 min old)
//   - DURABLE chunk record in bbolt
//   - Chunk files on both primary and secondary workers
//
// After one reconciler pass the stale STAGED manifest is expired, the DURABLE
// chunk has no live references (all refs were from the now-FAILED manifest),
// and the GC removes the chunk from both workers.
func TestCrash_OrphanDurableChunk_CleanedByReconciler(t *testing.T) {
	cc := newCrashCluster(t, 4)
	ctx := context.Background()

	data := bytes.Repeat([]byte("crash-orphan-durable-chunk-inject-unique-yz99b"), 10)
	sum := sha256.Sum256(data)
	chunkID := hex.EncodeToString(sum[:])

	primaryWorker := cc.workers[0]
	secondaryWorker := cc.workers[1]

	// Write chunk to both workers (both replicas written, crash before CommitObject).
	require.NoError(t, primaryWorker.store.Write(ctx, chunkID, bytes.NewReader(data), int64(len(data))))
	require.NoError(t, secondaryWorker.store.Write(ctx, chunkID, bytes.NewReader(data), int64(len(data))))

	// Inject stale STAGED manifest.
	staleObjectID := "stale-upload-durable-" + chunkID[:8]
	require.NoError(t, cc.meta.SaveObject(ctx, &models.ObjectManifest{
		ID:         staleObjectID,
		Name:       "orphan-durable.bin",
		Status:     models.StatusStaged,
		ChunkCount: 1,
		SizeBytes:  int64(len(data)),
		CreatedAt:  time.Now().Add(-15 * time.Minute),
		ModifiedAt: time.Now().Add(-15 * time.Minute),
	}))

	_, err := cc.meta.AddChunkRef(ctx, staleObjectID, models.ChunkRef{
		ChunkID:     chunkID,
		Index:       0,
		Offset:      0,
		Size:        len(data),
		PrimaryID:   primaryWorker.id,
		SecondaryID: secondaryWorker.id,
	})
	require.NoError(t, err)
	// Reset ModifiedAt: AddChunkRef updates it to now, which would prevent expiry.
	require.NoError(t, cc.meta.SaveObject(ctx, &models.ObjectManifest{
		ID:         staleObjectID,
		Name:       "orphan-durable.bin",
		Status:     models.StatusStaged,
		ChunkCount: 1,
		SizeBytes:  int64(len(data)),
		CreatedAt:  time.Now().Add(-15 * time.Minute),
		ModifiedAt: time.Now().Add(-15 * time.Minute),
	}))

	// Create PENDING then immediately mark DURABLE (crash happened between
	// MarkChunkDurable and CommitObject).
	_, err = cc.meta.PrepareChunk(ctx, &models.ChunkRecord{
		ChunkID:   chunkID,
		SizeBytes: len(data),
		State:     models.ChunkPending,
		Replicas: []models.Replica{
			{WorkerID: primaryWorker.id, WorkerAddr: strings.TrimPrefix(primaryWorker.srv.URL, "http://")},
			{WorkerID: secondaryWorker.id, WorkerAddr: strings.TrimPrefix(secondaryWorker.srv.URL, "http://")},
		},
		CreatedAt: time.Now().Add(-15 * time.Minute),
	}, false)
	require.NoError(t, err)
	require.NoError(t, cc.meta.MarkChunkDurable(ctx, chunkID))

	// Verify DURABLE state in bbolt before reconciler.
	rec, err := cc.meta.GetChunk(ctx, chunkID)
	require.NoError(t, err)
	assert.Equal(t, models.ChunkDurable, rec.State, "chunk must be DURABLE before reconciler")

	primaryPath := filepath.Join(primaryWorker.dataDir, "chunks", chunkID+".bin")
	secondaryPath := filepath.Join(secondaryWorker.dataDir, "chunks", chunkID+".bin")
	_, err = os.Stat(primaryPath)
	require.NoError(t, err, "primary chunk file must exist before reconciler")
	_, err = os.Stat(secondaryPath)
	require.NoError(t, err, "secondary chunk file must exist before reconciler")

	// Reconciler: expires manifest → chunk live=0 → GC deletes from both workers.
	report := cc.triggerRecon(t)
	assert.GreaterOrEqual(t, report.StaleUploadsFailed, 1,
		"reconciler must expire the stale STAGED manifest")

	require.Eventually(t, func() bool {
		_, e1 := os.Stat(primaryPath)
		_, e2 := os.Stat(secondaryPath)
		return os.IsNotExist(e1) && os.IsNotExist(e2)
	}, 5*time.Second, 10*time.Millisecond,
		"GC must delete the orphan DURABLE chunk from both workers")

	status, _ := cc.download(t, staleObjectID)
	assert.Equal(t, http.StatusNotFound, status,
		"stale STAGED manifest must not be downloadable after expiry")
}

// ── §1 Dedup + all refs deleted + GC → chunk file removed from workers ──────

// TestDedup_AllRefsDeleted_ChunkGCed
// Uploads the same content 3 times so the chunk is deduplicated (ref_count=3).
// Deletes all 3 objects concurrently.  After a reconciler pass the chunk has no
// live references and must be removed from every worker's disk.
func TestDedup_AllRefsDeleted_ChunkGCed(t *testing.T) {
	rc := newReconCluster(t, 4)
	ctx := context.Background()

	content := bytes.Repeat([]byte("dedup-all-refs-deleted-gc-test-unique-bb55"), 5)

	// Upload same content 3 times → deduplicated to one chunk.
	objectIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		up := rc.upload(t, fmt.Sprintf("dedup-gc-%d.bin", i), content)
		objectIDs[i] = up.ObjectID
	}

	// All three objects share one chunk — verify ref_count >= 3.
	refs, err := rc.meta.GetChunkRefs(ctx, objectIDs[0])
	require.NoError(t, err)
	require.NotEmpty(t, refs)
	chunkID := refs[0].ChunkID
	primaryID := refs[0].PrimaryID

	rec, err := rc.meta.GetChunk(ctx, chunkID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, rec.RefCount, 3,
		"deduplicated chunk must have ref_count >= 3")

	// Find the primary worker's chunk file path.
	var primaryNode *workerNode
	for _, w := range rc.workers {
		if w.id == primaryID {
			primaryNode = w
			break
		}
	}
	require.NotNil(t, primaryNode, "primary worker not found")
	chunkPath := filepath.Join(primaryNode.dataDir, "chunks", chunkID+".bin")
	_, err = os.Stat(chunkPath)
	require.NoError(t, err, "chunk file must exist before any deletes")

	// Delete all 3 objects concurrently.
	var wg sync.WaitGroup
	for _, oid := range objectIDs {
		oid := oid
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, reqErr := http.NewRequest(http.MethodDelete,
				rc.controller.URL+"/api/v1/objects/"+oid, nil)
			require.NoError(t, reqErr)
			resp, doErr := http.DefaultClient.Do(req)
			if doErr == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// Run reconciler: all refs are from DELETED objects → chunk live=0 → GC deletes it.
	report := rc.triggerRecon(t)
	t.Logf("GC report: chunks_deleted=%d errors=%d lost=%d",
		report.ChunksDeleted, report.Errors, report.LostChunks)

	require.Eventually(t, func() bool {
		_, err := os.Stat(chunkPath)
		return os.IsNotExist(err)
	}, 10*time.Second, 20*time.Millisecond,
		"GC must delete the deduplicated chunk after all object refs are removed")

	assert.Zero(t, report.LostChunks, "GC must not report any lost chunks")
}
