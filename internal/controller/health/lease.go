package health

import (
	"sync"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

const (
	HeartbeatInterval = 2 * time.Second
	LeaseTimeout      = 6 * time.Second // 3 missed heartbeats
)

// LeaseTable tracks the last-seen time and status of every worker.
// All methods are goroutine-safe.
type LeaseTable struct {
	mu          sync.RWMutex
	lastSeen    map[string]time.Time
	status      map[string]models.WorkerStatus
	subscribers []chan models.WorkerOfflineEvent
}

func NewLeaseTable() *LeaseTable {
	return &LeaseTable{
		lastSeen: make(map[string]time.Time),
		status:   make(map[string]models.WorkerStatus),
	}
}

// RecordHeartbeat updates the worker's last-seen time and marks it ONLINE.
// Called on every heartbeat and on worker registration.
func (lt *LeaseTable) RecordHeartbeat(workerID string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.lastSeen[workerID] = time.Now()
	lt.status[workerID] = models.WorkerOnline
}

// GetWorkerStatus returns the current status of a worker.
// Returns UNKNOWN if the worker has never been seen.
func (lt *LeaseTable) GetWorkerStatus(workerID string) models.WorkerStatus {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	if s, ok := lt.status[workerID]; ok {
		return s
	}
	return models.WorkerUnknown
}

// ListOnlineWorkers returns the IDs of all workers currently ONLINE.
func (lt *LeaseTable) ListOnlineWorkers() []string {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	var online []string
	for id, s := range lt.status {
		if s == models.WorkerOnline {
			online = append(online, id)
		}
	}
	return online
}

// WorkerLease is one worker's health as the lease table sees it.
type WorkerLease struct {
	Status   models.WorkerStatus `json:"status"`
	LastSeen time.Time           `json:"last_seen"`
}

// Snapshot returns the status and last heartbeat of every worker the table has seen.
func (lt *LeaseTable) Snapshot() map[string]WorkerLease {
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	out := make(map[string]WorkerLease, len(lt.status))
	for id, st := range lt.status {
		out[id] = WorkerLease{Status: st, LastSeen: lt.lastSeen[id]}
	}
	return out
}

// Subscribe registers a new subscriber and returns a read-only channel.
// Each OFFLINE event is sent to every subscriber (fan-out).
// The caller must drain the channel — sends are non-blocking and events are
// dropped if the channel is full to avoid blocking the monitor goroutine.
func (lt *LeaseTable) Subscribe() <-chan models.WorkerOfflineEvent {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	ch := make(chan models.WorkerOfflineEvent, 16)
	lt.subscribers = append(lt.subscribers, ch)
	return ch
}

// markOffline sets a worker OFFLINE and broadcasts to all subscribers.
// Must be called with lt.mu held for write.
func (lt *LeaseTable) markOffline(workerID string) {
	lt.status[workerID] = models.WorkerOffline
	event := models.WorkerOfflineEvent{
		WorkerID:  workerID,
		Timestamp: time.Now(),
	}
	for _, ch := range lt.subscribers {
		select {
		case ch <- event:
		default: // subscriber not draining — drop event rather than block
		}
	}
}
