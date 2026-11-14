package health

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// --- LeaseTable ---

func TestRecordHeartbeat_SetsOnline(t *testing.T) {
	lt := NewLeaseTable()
	lt.RecordHeartbeat("w1")
	assert.Equal(t, models.WorkerOnline, lt.GetWorkerStatus("w1"))
}

func TestGetWorkerStatus_UnknownWorker(t *testing.T) {
	lt := NewLeaseTable()
	assert.Equal(t, models.WorkerUnknown, lt.GetWorkerStatus("w-never-seen"))
}

func TestListOnlineWorkers(t *testing.T) {
	lt := NewLeaseTable()
	lt.RecordHeartbeat("w1")
	lt.RecordHeartbeat("w2")
	lt.RecordHeartbeat("w3")

	online := lt.ListOnlineWorkers()
	assert.Len(t, online, 3)
}

func TestListOnlineWorkers_ExcludesOffline(t *testing.T) {
	lt := NewLeaseTable()
	lt.RecordHeartbeat("w1")
	lt.RecordHeartbeat("w2")

	lt.mu.Lock()
	lt.markOffline("w2")
	lt.mu.Unlock()

	online := lt.ListOnlineWorkers()
	assert.Len(t, online, 1)
	assert.Equal(t, "w1", online[0])
}

func TestListOnlineWorkers_Empty(t *testing.T) {
	lt := NewLeaseTable()
	online := lt.ListOnlineWorkers()
	assert.Empty(t, online)
}

// --- Subscribe / fan-out ---

func TestSubscribe_ReceivesOfflineEvent(t *testing.T) {
	lt := NewLeaseTable()
	ch := lt.Subscribe()

	lt.RecordHeartbeat("w1")

	lt.mu.Lock()
	lt.markOffline("w1")
	lt.mu.Unlock()

	select {
	case event := <-ch:
		assert.Equal(t, "w1", event.WorkerID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OFFLINE event")
	}
}

func TestSubscribe_FanOut_MultipleSubscribers(t *testing.T) {
	lt := NewLeaseTable()
	ch1 := lt.Subscribe()
	ch2 := lt.Subscribe()

	lt.RecordHeartbeat("w1")
	lt.mu.Lock()
	lt.markOffline("w1")
	lt.mu.Unlock()

	for _, ch := range []<-chan models.WorkerOfflineEvent{ch1, ch2} {
		select {
		case event := <-ch:
			assert.Equal(t, "w1", event.WorkerID)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for OFFLINE event on subscriber")
		}
	}
}

// --- Monitor goroutine ---

func TestMonitor_DeclaresOfflineAfterTimeout(t *testing.T) {
	lt := NewLeaseTable()

	// Use a very short timeout for testing
	origTimeout := LeaseTimeout
	_ = origTimeout // keep reference — we override via monitor interval trick below

	mon := &Monitor{table: lt, interval: 50 * time.Millisecond}
	ch := lt.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mon.Start(ctx)

	// Record heartbeat then stop — simulate worker going silent
	lt.RecordHeartbeat("w1")

	// Manually backdate lastSeen so the lease appears expired
	lt.mu.Lock()
	lt.lastSeen["w1"] = time.Now().Add(-LeaseTimeout - time.Second)
	lt.mu.Unlock()

	select {
	case event := <-ch:
		assert.Equal(t, "w1", event.WorkerID)
		assert.Equal(t, models.WorkerOffline, lt.GetWorkerStatus("w1"))
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not declare worker OFFLINE in time")
	}
}

func TestMonitor_RecordHeartbeat_ResetsLease(t *testing.T) {
	lt := NewLeaseTable()
	mon := &Monitor{table: lt, interval: 50 * time.Millisecond}
	ch := lt.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mon.Start(ctx)

	lt.RecordHeartbeat("w1")

	// Keep sending heartbeats — worker should stay ONLINE
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			time.Sleep(50 * time.Millisecond)
			lt.RecordHeartbeat("w1")
		}
	}()
	<-done

	// No OFFLINE event should have been sent
	select {
	case event := <-ch:
		t.Fatalf("unexpected OFFLINE event for healthy worker: %s", event.WorkerID)
	default:
		assert.Equal(t, models.WorkerOnline, lt.GetWorkerStatus("w1"))
	}
}

func TestMonitor_DoesNotReFireOfflineForAlreadyOfflineWorker(t *testing.T) {
	lt := NewLeaseTable()
	mon := &Monitor{table: lt, interval: 50 * time.Millisecond}
	ch := lt.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mon.Start(ctx)

	lt.RecordHeartbeat("w1")
	lt.mu.Lock()
	lt.lastSeen["w1"] = time.Now().Add(-LeaseTimeout - time.Second)
	lt.mu.Unlock()

	// Drain the first event
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive first OFFLINE event")
	}

	// Wait another cycle — should not fire again
	time.Sleep(200 * time.Millisecond)
	select {
	case event := <-ch:
		t.Fatalf("duplicate OFFLINE event fired for worker: %s", event.WorkerID)
	default:
		// correct — no second event
	}
}
