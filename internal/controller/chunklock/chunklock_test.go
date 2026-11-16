package chunklock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSameChunkIsSerialised(t *testing.T) {
	var inside, maxInside atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := Lock("chunk-a")
			defer unlock()
			n := inside.Add(1)
			for {
				m := maxInside.Load()
				if n <= m || maxInside.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inside.Add(-1)
		}()
	}
	wg.Wait()
	if maxInside.Load() != 1 {
		t.Fatalf("%d holders at once", maxInside.Load())
	}
}

func TestDifferentStripesDoNotBlock(t *testing.T) {
	unlock := Lock("chunk-a")
	defer unlock()
	done := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			id := "other-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
			if stripe(id) != stripe("chunk-a") {
				Lock(id)()
				close(done)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("an unrelated chunk was blocked")
	}
}

func TestUnlockReleases(t *testing.T) {
	Lock("x")()
	done := make(chan struct{})
	go func() { Lock("x")(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lock was not released")
	}
}
