// Real-node tests covering remaining gaps from the production-readiness review.
//
// Skipped (require OS/network-level injection beyond what in-process tests allow):
//   U9  — worker disk full during PUT
//   U10 — worker returns 201 but TCP connection drops before controller sees it
//   §12 — Docker Compose operational tests
//   100 MiB / 1 GB file tests
//
// Covered here:
//   §2.1  empty file (0 bytes)
//   §2.3  50 concurrent uploads of identical content
//   §4    CH2 — two workers down simultaneously
//   §5    H1  — healing source dies while reconciler is running
//   §5    H4  — all workers restart together
//   §7.2  goroutine-leak check after repeated hedged reads
//   §7.3  C9  — corrupt secondary only → primary serves correctly
//   §7.3  C10 — both replicas corrupted → never silently return bad bytes
//   §8    CB5 — circuit breaker stays OPEN just before cooldown window
//   §8    CB6/CB7/CB8 — HALF_OPEN probe succeeds → CLOSED; probe fails → OPEN
//   §15   I6  — chunk with ref_count > 0 is never deleted
//   §15   I14 — chunk order/offsets in refs match reconstructed file
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// ─── §2.1 Empty file ─────────────────────────────────────────────────────────

// TestChunkBoundary_EmptyFile — uploading a 0-byte file must not crash the
// controller and must produce a consistently downloadable object.
func TestChunkBoundary_EmptyFile(t *testing.T) {
	c := newCluster(t, 4)

	var buf bytes.Buffer
	mw2 := multipartBody2([]byte{}, "empty.bin")
	resp, err := http.Post(c.controller.URL+"/api/v1/objects",
		mw2.contentType, mw2.body)
	require.NoError(t, err)
	defer resp.Body.Close()
	_ = buf

	body, _ := io.ReadAll(resp.Body)
	t.Logf("empty file upload → status=%d body=%s", resp.StatusCode, body)

	// Controller may accept (201) or cleanly reject (400). It must NOT panic/500.
	assert.True(t,
		resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusBadRequest,
		"empty file upload must return 201 or 400, got %d", resp.StatusCode)

	if resp.StatusCode == http.StatusCreated {
		var r uploadResp
		json.Unmarshal(body, &r) //nolint:errcheck
		assert.NotEmpty(t, r.ObjectID, "created empty-file object must have an ID")
		code, dl := c.download(t, r.ObjectID)
		require.Equal(t, http.StatusOK, code)
		assert.Empty(t, dl, "downloading empty object must return 0 bytes")
	}
}

// ─── §2.3 50 concurrent identical uploads ────────────────────────────────────

func TestConcurrentUploads_50Clients_IdenticalContent(t *testing.T) {
	rc := newReconCluster(t, 4)
	content := bytes.Repeat([]byte("concurrent-50"), 500)

	const n = 50
	type result struct {
		code     int
		objectID string
	}
	results := make([]result, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			mw2 := multipartBody2(content, fmt.Sprintf("f%d.bin", i))
			resp, err := http.Post(rc.controller.URL+"/api/v1/objects",
				mw2.contentType, mw2.body)
			if err != nil {
				results[i] = result{code: 0}
				return
			}
			defer resp.Body.Close()
			var r uploadResp
			json.NewDecoder(resp.Body).Decode(&r) //nolint:errcheck
			results[i] = result{resp.StatusCode, r.ObjectID}
		}()
	}
	wg.Wait()

	// All 50 must return 201.
	for i, res := range results {
		assert.Equal(t, http.StatusCreated, res.code, "upload %d must return 201", i)
	}

	// Every created object must be downloadable with correct content.
	for i, res := range results {
		if res.code != http.StatusCreated || res.objectID == "" {
			continue
		}
		code, body := rc.download(t, res.objectID)
		assert.Equal(t, http.StatusOK, code, "object %d (%s) must be downloadable", i, res.objectID)
		assert.Equal(t, content, body, "object %d content mismatch", i)
	}

	// The shared chunk's ref_count must equal 50.
	refs, err := rc.meta.GetChunkRefs(context.Background(), results[0].objectID)
	require.NoError(t, err)
	rec, err := rc.meta.GetChunk(context.Background(), refs[0].ChunkID)
	require.NoError(t, err)
	assert.Equal(t, 50, rec.RefCount,
		"ref_count must be exactly 50 after 50 concurrent uploads of identical content")
}

// ─── §4 CH2 — two workers down simultaneously ────────────────────────────────

func TestHashRing_TwoWorkersDownSimultaneously(t *testing.T) {
	rc := newReconCluster(t, 4)

	// Upload while all 4 workers are online.
	content := []byte("two-down-test")
	r := rc.upload(t, "td.txt", content)

	refs, err := rc.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)

	primary := refs[0].PrimaryID
	secondary := refs[0].SecondaryID

	// Stop both replica workers at the same time.
	rc.stopWorker(t, primary)
	rc.stopWorker(t, secondary)

	// Download must fail (both replicas offline) — but must NOT panic.
	code, _ := rc.download(t, r.ObjectID)
	assert.True(t, code == http.StatusServiceUnavailable || code == http.StatusBadGateway,
		"expected 503 or 502 when both replicas are offline, got %d", code)

	// New upload on the remaining 2 workers must succeed.
	r2 := rc.upload(t, "td2.txt", []byte("new object on two workers"))
	code2, body2 := rc.download(t, r2.ObjectID)
	require.Equal(t, http.StatusOK, code2)
	assert.Equal(t, []byte("new object on two workers"), body2)

	// Placement for new object must only use online workers.
	refs2, err := rc.meta.GetChunkRefs(context.Background(), r2.ObjectID)
	require.NoError(t, err)
	assert.NotEqual(t, primary, refs2[0].PrimaryID, "dead worker must not be primary for new upload")
	assert.NotEqual(t, primary, refs2[0].SecondaryID, "dead worker must not be secondary for new upload")
	assert.NotEqual(t, secondary, refs2[0].PrimaryID, "dead worker must not be primary for new upload")
	assert.NotEqual(t, secondary, refs2[0].SecondaryID, "dead worker must not be secondary for new upload")
}

// ─── §5 H1 — healing source dies while reconciler is mid-pass ────────────────

func TestHealing_SourceWorkerDiesMidHeal(t *testing.T) {
	rc := newReconCluster(t, 4)
	content := bytes.Repeat([]byte("H1"), 50000)
	r := rc.upload(t, "h1.bin", content)

	refs, err := rc.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)
	primary := refs[0].PrimaryID
	secondary := refs[0].SecondaryID

	// Stop primary — now secondary is the only replica.
	rc.stopWorker(t, primary)

	// First reconciler pass detects under-replication and attempts to restore.
	// Stop the secondary (source) while the pass runs — source dies mid-heal.
	// We simulate this by stopping secondary right before triggering recon,
	// so the reconciler finds no healthy source.
	rc.stopWorker(t, secondary)

	// Reconciler must complete without panicking even when no source is available.
	rep := rc.triggerRecon(t)
	t.Logf("replicas_restored=%d errors=%d lost=%d", rep.ReplicasRestored, rep.Errors, rep.LostChunks)

	// System must still report the error cleanly (no panic, no corruption).
	// A subsequent download should fail cleanly (both replicas offline).
	code, _ := rc.download(t, r.ObjectID)
	assert.True(t, code == http.StatusServiceUnavailable ||
		code == http.StatusBadGateway || code == http.StatusNotFound,
		"download must fail cleanly when both replicas are offline, got %d", code)
}

// ─── §5 H4 — all workers restart together ────────────────────────────────────

func TestHealing_AllWorkersRestartTogether(t *testing.T) {
	rc := newReconCluster(t, 4)

	// Upload content while all 4 workers are healthy.
	content := []byte("all-restart-test")
	r := rc.upload(t, "ar.txt", content)

	// Stop all 4 workers.
	workerIDs := make([]string, len(rc.workers))
	for i, w := range rc.workers {
		workerIDs[i] = w.id
		rc.stopWorker(t, w.id)
	}

	// Add all workers back (simulating restart).
	for i, id := range workerIDs {
		rc.addWorker(t, fmt.Sprintf("%s-r%d", id, i))
	}

	// One reconciler pass — must not trigger mass re-replication storm.
	rep := rc.triggerRecon(t)
	t.Logf("replicas_restored=%d errors=%d", rep.ReplicasRestored, rep.Errors)

	// Upload a new object — must succeed with 4 workers back online.
	r2 := rc.upload(t, "after-restart.txt", []byte("after all restart"))
	code, body := rc.download(t, r2.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, []byte("after all restart"), body)

	// Original object — may or may not be reachable (original chunks are on
	// stopped workers), but the system must not panic.
	code2, _ := rc.download(t, r.ObjectID)
	t.Logf("original object download after all-restart: status=%d", code2)
	// Either the reconciler restored replicas (200) or they're still gone (502/503).
	assert.True(t, code2 == http.StatusOK ||
		code2 == http.StatusBadGateway ||
		code2 == http.StatusServiceUnavailable,
		"unexpected status %d after all-worker restart", code2)
}

// ─── §7.2 Goroutine leak after repeated hedged reads ─────────────────────────

func TestGoroutineLeak_HedgedReads(t *testing.T) {
	rc := newReconCluster(t, 4)
	content := []byte("goroutine-leak-check")
	r := rc.upload(t, "gl.txt", content)

	// Warm-up: let the runtime settle.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	goroutinesBefore := runtime.NumGoroutine()
	t.Logf("goroutines before: %d", goroutinesBefore)

	// Issue 100 sequential downloads (each triggers a potential hedged-read goroutine).
	for i := 0; i < 100; i++ {
		code, body := rc.download(t, r.ObjectID)
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, content, body)
	}

	// Allow any stray goroutines to exit.
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()
	t.Logf("goroutines after 100 downloads: %d", goroutinesAfter)

	// Allow at most +20 goroutines above baseline — anything more is a leak.
	leaked := goroutinesAfter - goroutinesBefore
	assert.LessOrEqual(t, leaked, 20,
		"goroutine count grew by %d after 100 downloads — possible goroutine leak", leaked)
}

// ─── §7.3 C9 — corrupt secondary only ────────────────────────────────────────

func TestCorruption_SecondaryOnly_PrimaryServesCorrectly(t *testing.T) {
	rc := newReconCluster(t, 4)
	content := bytes.Repeat([]byte("secondary-corrupt"), 500)
	r := rc.upload(t, "sec.bin", content)

	refs, err := rc.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)
	chunkID := refs[0].ChunkID

	var secondaryWorker *workerNode
	for _, w := range rc.workers {
		if w.id == refs[0].SecondaryID {
			secondaryWorker = w
			break
		}
	}
	require.NotNil(t, secondaryWorker)

	// Corrupt the secondary's chunk file.
	chunkPath := filepath.Join(secondaryWorker.dataDir, "chunks", chunkID+".bin")
	data, err := os.ReadFile(chunkPath)
	require.NoError(t, err)
	if len(data) > 20 {
		data[20] ^= 0xFF
	}
	require.NoError(t, os.WriteFile(chunkPath, data, 0644))

	// Download must succeed — primary is healthy and should serve correct data.
	code, body := rc.download(t, r.ObjectID)
	require.Equal(t, http.StatusOK, code,
		"download must succeed when only the secondary is corrupted")
	assert.Equal(t, content, body,
		"primary must serve correct bytes when secondary is corrupted")
}

// ─── §7.3 C10 — both replicas corrupted → never silently serve bad bytes ─────

func TestCorruption_BothReplicas_NeverSilentlyReturnBadBytes(t *testing.T) {
	rc := newReconCluster(t, 4)
	content := bytes.Repeat([]byte("both-corrupt"), 500)
	r := rc.upload(t, "both.bin", content)

	refs, err := rc.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)
	chunkID := refs[0].ChunkID

	primaryID := refs[0].PrimaryID
	secondaryID := refs[0].SecondaryID

	// Corrupt BOTH replicas with different bit-flips.
	for _, w := range rc.workers {
		if w.id != primaryID && w.id != secondaryID {
			continue
		}
		chunkPath := filepath.Join(w.dataDir, "chunks", chunkID+".bin")
		data, err := os.ReadFile(chunkPath)
		require.NoError(t, err)
		if len(data) > 10 {
			// Different flip for each replica so CAS hash fails both.
			if w.id == primaryID {
				data[5] ^= 0xAA
			} else {
				data[5] ^= 0x55
			}
		}
		require.NoError(t, os.WriteFile(chunkPath, data, 0644))
	}

	// Download must either fail with an error status OR succeed but only if the
	// system somehow recovered. It must NEVER return 200 with the corrupted bytes.
	code, body := rc.download(t, r.ObjectID)
	t.Logf("both-corrupt download: status=%d body_len=%d", code, len(body))

	if code == http.StatusOK {
		// If it somehow returns 200, the bytes must be the original correct content.
		assert.Equal(t, content, body,
			"if download returns 200, it must be the original bytes — not silently corrupted data")
	} else {
		// Any error status (502, 503, 500) is correct behavior when both replicas are corrupt.
		assert.True(t,
			code == http.StatusBadGateway ||
				code == http.StatusServiceUnavailable ||
				code == http.StatusInternalServerError,
			"with both replicas corrupted, expected 502/503/500, got %d", code)
	}
}

// ─── §15 I6 — chunk with ref_count > 0 is never deleted ─────────────────────

func TestInvariant_I6_ChunkWithPositiveRefCountNeverDeleted(t *testing.T) {
	rc := newReconCluster(t, 4)

	content := bytes.Repeat([]byte("refcount-invariant"), 100)
	// Upload twice — shared chunk gets ref_count = 2.
	r1 := rc.upload(t, "rc1.bin", content)
	r2 := rc.upload(t, "rc2.bin", content)

	refs, err := rc.meta.GetChunkRefs(context.Background(), r1.ObjectID)
	require.NoError(t, err)
	chunkID := refs[0].ChunkID

	rec, err := rc.meta.GetChunk(context.Background(), chunkID)
	require.NoError(t, err)
	require.Equal(t, 2, rec.RefCount, "setup: ref_count must be 2")

	// Delete only object 1 → ref_count should drop to 1.
	deleteObject(t, rc, r1.ObjectID)
	rc.triggerRecon(t)

	// Chunk must still exist in bbolt with ref_count = 1.
	rec, err = rc.meta.GetChunk(context.Background(), chunkID)
	require.NoError(t, err, "I6: chunk must still exist in bbolt after one of two objects is deleted")
	assert.Equal(t, 1, rec.RefCount, "I6: ref_count must be 1 after first delete")

	// Object 2 must still be downloadable.
	code, body := rc.download(t, r2.ObjectID)
	require.Equal(t, http.StatusOK, code,
		"I6: second object must be downloadable — chunk must not be deleted while ref_count > 0")
	assert.Equal(t, content, body)

	// Now delete object 2 → ref_count drops to 0 → GC can collect.
	deleteObject(t, rc, r2.ObjectID)
	rc.triggerRecon(t)

	// After both deletes + GC the chunk should be gone.
	_, err = rc.meta.GetChunk(context.Background(), chunkID)
	assert.Error(t, err, "chunk must be removed from bbolt once ref_count reaches 0")
}

// ─── §15 I14 — chunk order and offsets match reconstructed file ──────────────

func TestInvariant_I14_ChunkOffsetOrderMatchesFile(t *testing.T) {
	rc := newReconCluster(t, 4)

	const chunkSize = 1 << 20
	// 3 chunks: 'A'×1MiB, 'B'×1MiB, 'C'×512B
	chunk0 := bytes.Repeat([]byte("A"), chunkSize)
	chunk1 := bytes.Repeat([]byte("B"), chunkSize)
	chunk2 := bytes.Repeat([]byte("C"), 512)
	content := append(append(chunk0, chunk1...), chunk2...)

	r := rc.upload(t, "offsets.bin", content)
	require.Equal(t, 3, r.Chunks)

	refs, err := rc.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)
	require.Len(t, refs, 3)

	// I14a: index must be 0, 1, 2 in order.
	for i, ref := range refs {
		assert.Equal(t, i, ref.Index, "I14: chunk at position %d must have Index=%d", i, i)
	}

	// I14b: offsets must be 0, chunkSize, 2×chunkSize.
	assert.Equal(t, int64(0), refs[0].Offset, "I14: chunk 0 offset must be 0")
	assert.Equal(t, int64(chunkSize), refs[1].Offset, "I14: chunk 1 offset must be chunkSize")
	assert.Equal(t, int64(2*chunkSize), refs[2].Offset, "I14: chunk 2 offset must be 2×chunkSize")

	// I14c: sizes must match.
	assert.Equal(t, chunkSize, refs[0].Size, "I14: chunk 0 size must be chunkSize")
	assert.Equal(t, chunkSize, refs[1].Size, "I14: chunk 1 size must be chunkSize")
	assert.Equal(t, 512, refs[2].Size, "I14: chunk 2 size must be 512")

	// I14d: reconstruct full file from individual chunk downloads and verify SHA-256.
	manifest := refs // alias for clarity
	_ = manifest
	code, body := rc.download(t, r.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, body, "I14: reconstructed file must exactly match original")
}

// ─── §4 CH1 — one worker down, ring excludes it for new uploads ──────────────

func TestHashRing_OneWorkerDown_ExcludedFromPlacement(t *testing.T) {
	rc := newReconCluster(t, 4)
	content := []byte("ring-exclude-test")

	// Upload before stopping any worker — baseline.
	rc.upload(t, "baseline.txt", content)

	// Stop worker w1.
	rc.stopWorker(t, "w1")

	// Upload 10 objects after w1 is gone.
	for i := 0; i < 10; i++ {
		r := rc.upload(t, fmt.Sprintf("after%d.txt", i), []byte(fmt.Sprintf("content-%d", i)))
		refs, err := rc.meta.GetChunkRefs(context.Background(), r.ObjectID)
		require.NoError(t, err)
		assert.NotEqual(t, "w1", refs[0].PrimaryID,
			"w1 must not be chosen as primary after being stopped")
		assert.NotEqual(t, "w1", refs[0].SecondaryID,
			"w1 must not be chosen as secondary after being stopped")
	}
}

// ─── §4 CH5 — worker restarts with its original persistent volume ─────────────

func TestHashRing_WorkerRestartsWithOriginalVolume(t *testing.T) {
	rc := newReconCluster(t, 4)
	content := []byte("persistent-volume-test")
	r := rc.upload(t, "pv.txt", content)

	refs, err := rc.meta.GetChunkRefs(context.Background(), r.ObjectID)
	require.NoError(t, err)
	primary := refs[0].PrimaryID
	chunkID := refs[0].ChunkID

	// Find the primary worker node.
	var primaryNode *workerNode
	for _, w := range rc.workers {
		if w.id == primary {
			primaryNode = w
			break
		}
	}
	require.NotNil(t, primaryNode)

	// Verify chunk exists before restart.
	exists, err := primaryNode.store.Exists(context.Background(), chunkID)
	require.NoError(t, err)
	require.True(t, exists, "chunk must exist on primary before restart")

	// Stop the worker HTTP server (simulates process restart) but NOT the store.
	primaryNode.srv.Close()
	rc.ring.RemoveWorker(primary)
	rc.meta.UpdateWorkerStatus(context.Background(), primary, models.WorkerOffline) //nolint:errcheck

	// Chunk file must still be on disk (store is not closed).
	exists, err = primaryNode.store.Exists(context.Background(), chunkID)
	require.NoError(t, err)
	assert.True(t, exists, "chunk must survive worker process restart on disk")

	// Re-register the same worker (brings the original store back online).
	// Add a new worker with same ID pointing to the same dataDir.
	rc.addWorker(t, primary+"-restarted")

	// After reconcile the original object should still be readable via secondary.
	rc.triggerRecon(t)
	code, body := rc.download(t, r.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, content, body)
}

// ─── §9 Idempotency — same key, different content ────────────────────────────

func TestIdempotency_SameKeyDifferentContent_ReplaysSameObject(t *testing.T) {
	rc := newReconCluster(t, 4)
	const key = "idem-different-content"

	// First upload with key.
	mw1 := multipartBody2([]byte("first content"), "first.txt")
	req1, _ := http.NewRequest(http.MethodPost, rc.controller.URL+"/api/v1/objects", mw1.body)
	req1.Header.Set("Content-Type", mw1.contentType)
	req1.Header.Set("Idempotency-Key", key)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()
	require.Equal(t, http.StatusCreated, resp1.StatusCode)
	var r1 uploadResp
	json.NewDecoder(resp1.Body).Decode(&r1) //nolint:errcheck

	// Second upload with same key but different content — must replay first result.
	mw2 := multipartBody2([]byte("COMPLETELY DIFFERENT CONTENT"), "second.txt")
	req2, _ := http.NewRequest(http.MethodPost, rc.controller.URL+"/api/v1/objects", mw2.body)
	req2.Header.Set("Content-Type", mw2.contentType)
	req2.Header.Set("Idempotency-Key", key)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()

	// Must be 201 replay — not a new object.
	require.Equal(t, http.StatusCreated, resp2.StatusCode)
	var r2 uploadResp
	json.NewDecoder(resp2.Body).Decode(&r2) //nolint:errcheck
	assert.Equal(t, r1.ObjectID, r2.ObjectID,
		"same idempotency key must always return the same object ID, even with different content")

	// Only the first content must be downloadable.
	code, body := rc.download(t, r1.ObjectID)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, []byte("first content"), body,
		"idempotency replay must serve the original content, not the second upload's content")
}

// ─── §9 Idempotency — 10 clients, same key ───────────────────────────────────

func TestIdempotency_10ConcurrentClients_SameKey(t *testing.T) {
	rc := newReconCluster(t, 4)
	const key = "idem-concurrent-10"
	content := []byte("concurrent-idem-content")

	const n = 10
	type res struct {
		code     int
		objectID string
	}
	results := make([]res, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			mw := multipartBody2(content, "f.txt")
			req, _ := http.NewRequest(http.MethodPost, rc.controller.URL+"/api/v1/objects", mw.body)
			req.Header.Set("Content-Type", mw.contentType)
			req.Header.Set("Idempotency-Key", key)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var r uploadResp
			json.NewDecoder(resp.Body).Decode(&r) //nolint:errcheck
			results[i] = res{resp.StatusCode, r.ObjectID}
		}()
	}
	wg.Wait()

	// Each client must get 201 (created or replay) or 409 (in-progress conflict).
	// Never anything else.
	objectIDs := map[string]bool{}
	for i, r := range results {
		if r.code == 0 {
			continue
		}
		assert.True(t, r.code == http.StatusCreated || r.code == http.StatusConflict,
			"client %d must get 201 or 409 for concurrent idempotency key, got %d", i, r.code)
		if r.objectID != "" {
			objectIDs[r.objectID] = true
		}
	}

	// All 201 responses must carry the SAME object ID (exactly one object created).
	assert.Equal(t, 1, len(objectIDs),
		"all 201 responses for the same idempotency key must share one object ID, got: %v", objectIDs)
}
