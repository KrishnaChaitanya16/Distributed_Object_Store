package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/workerclient"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/api"
	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/worker/storage"
)

// These tests drive the controller's real WorkerClientPool against the real worker server so the two sides cannot drift apart.

func startWorker(t *testing.T, id string) (string, *storage.Store) {
	t.Helper()
	st, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(nil)
	srv.Config.Handler = api.New(st, id, srv.Listener.Addr().String(), srv.Client()).Handler()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), st
}

func randomChunk(t *testing.T, n int) ([]byte, string) {
	t.Helper()
	b := make([]byte, n)
	rand.Read(b)
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

func TestPoolRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool := workerclient.New()
	addr, _ := startWorker(t, "w1")
	for _, n := range []int{0, 1, 4096, storage.MaxChunkSize} {
		data, id := randomChunk(t, n)
		if err := pool.PutChunk(ctx, addr, id, bytes.NewReader(data), int64(n)); err != nil {
			t.Fatalf("PutChunk %d bytes: %v", n, err)
		}
		rc, err := pool.GetChunk(ctx, addr, id)
		if err != nil {
			t.Fatalf("GetChunk %d bytes: %v", n, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, data) {
			t.Fatalf("%d-byte chunk differs after round trip", n)
		}
	}
}

func TestPoolListChunks(t *testing.T) {
	ctx := context.Background()
	pool := workerclient.New()
	addr, _ := startWorker(t, "w1")
	ids, err := pool.ListChunks(ctx, addr, "w1")
	if err != nil || len(ids) != 0 {
		t.Fatalf("empty worker: ids=%v err=%v", ids, err)
	}
	data, id := randomChunk(t, 100)
	pool.PutChunk(ctx, addr, id, bytes.NewReader(data), 100)
	ids, err = pool.ListChunks(ctx, addr, "w1")
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
}

func TestPoolReplicatePeer(t *testing.T) {
	ctx := context.Background()
	pool := workerclient.New()
	src, _ := startWorker(t, "w1")
	dst, dstStore := startWorker(t, "w2")
	data, id := randomChunk(t, storage.MaxChunkSize)
	pool.PutChunk(ctx, src, id, bytes.NewReader(data), int64(len(data)))
	if err := pool.ReplicatePeer(ctx, src, id, dst); err != nil {
		t.Fatalf("ReplicatePeer reported failure on success: %v", err)
	}
	if ok, err := dstStore.VerifyCRC(ctx, id); !ok || err != nil {
		t.Fatalf("target copy ok=%v err=%v", ok, err)
	}
}

func TestPoolDeleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := workerclient.New()
	addr, _ := startWorker(t, "w1")
	data, id := randomChunk(t, 100)
	pool.PutChunk(ctx, addr, id, bytes.NewReader(data), 100)
	for i := 0; i < 3; i++ {
		if err := pool.DeleteChunk(ctx, addr, id); err != nil {
			t.Fatalf("delete #%d: %v", i+1, err)
		}
	}
	if _, err := pool.GetChunk(ctx, addr, id); err == nil {
		t.Fatal("chunk still readable after delete")
	}
}

func TestPoolSurfacesWorkerRejections(t *testing.T) {
	ctx := context.Background()
	data, _ := randomChunk(t, 100)
	_, wrongID := randomChunk(t, 100)
	_, missing := randomChunk(t, 100)

	addr, _ := startWorker(t, "w1")
	if err := workerclient.New().PutChunk(ctx, addr, wrongID, bytes.NewReader(data), 100); err == nil {
		t.Error("PutChunk with a mismatched hash should fail")
	}
	if _, err := workerclient.New().GetChunk(ctx, addr, missing); err == nil {
		t.Error("GetChunk of a missing chunk should fail")
	}
	dst, _ := startWorker(t, "w2")
	if err := workerclient.New().ReplicatePeer(ctx, addr, missing, dst); err == nil {
		t.Error("ReplicatePeer of a chunk the source lacks should fail")
	}
}
