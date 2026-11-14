package health

import (
	"context"
	"log/slog"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// Monitor runs a background ticker that declares workers OFFLINE
// when their lease expires (no heartbeat for LeaseTimeout).
type Monitor struct {
	table    *LeaseTable
	interval time.Duration
}

func NewMonitor(table *LeaseTable) *Monitor {
	return &Monitor{
		table:    table,
		interval: time.Second, // check every second, timeout is 6s
	}
}

// Start runs the lease checker in a goroutine until ctx is cancelled.
// Call once from main().
func (m *Monitor) Start(ctx context.Context) {
	go m.run(ctx)
}

func (m *Monitor) run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkLeases()
		}
	}
}

func (m *Monitor) checkLeases() {
	now := time.Now()

	m.table.mu.Lock()
	defer m.table.mu.Unlock()

	for workerID, last := range m.table.lastSeen {
		if m.table.status[workerID] == models.WorkerOnline && now.Sub(last) > LeaseTimeout {
			slog.Warn("worker lease expired — marking OFFLINE",
				"worker_id", workerID,
				"last_seen_secs", now.Sub(last).Seconds(),
			)
			m.table.markOffline(workerID)
		}
	}
}
