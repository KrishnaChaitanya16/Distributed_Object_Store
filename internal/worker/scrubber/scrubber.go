package scrubber

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/pkg/models"
)

// maxRecent bounds the corruption history kept for the status endpoint.
const maxRecent = 20

var ErrAlreadyRunning = errors.New("scrubber is already running")

// Store is the slice of the chunk store the scrubber needs; storage.Store satisfies it.
type Store interface {
	List(ctx context.Context) ([]string, error)
	VerifyCRC(ctx context.Context, chunkID string) (bool, error)
	Quarantine(ctx context.Context, chunkID string) (bool, error)
}

type Config struct {
	// Interval is the gap between the end of one pass and the start of the next.
	Interval time.Duration
	// Pause is slept between chunks so a pass never saturates the disk.
	Pause time.Duration
	// OnCorrupt runs after a corrupt chunk has been quarantined; it may be nil.
	OnCorrupt func(chunkID string)
	// Logf defaults to log.Printf.
	Logf func(format string, args ...any)
}

type Event struct {
	ChunkID string    `json:"chunk_id"`
	At      time.Time `json:"at"`
}

// Status is a point-in-time snapshot; counters cover the lifetime of the process.
type Status struct {
	Running            bool       `json:"running"`
	IntervalSeconds    float64    `json:"interval_seconds"`
	Passes             int64      `json:"passes"`
	ChunksScanned      int64      `json:"chunks_scanned"`
	CorruptQuarantined int64      `json:"corrupt_quarantined"`
	Errors             int64      `json:"errors"`
	LastPassStarted    *time.Time `json:"last_pass_started,omitempty"`
	LastPassDurationMs int64      `json:"last_pass_duration_ms"`
	LastPassScanned    int        `json:"last_pass_scanned"`
	LastPassCorrupt    int        `json:"last_pass_corrupt"`
	LastError          string     `json:"last_error,omitempty"`
	Recent             []Event    `json:"recent_corruption"`
}

// Scrubber periodically re-checks every chunk's CRC32C footer and quarantines chunks that no longer match.
type Scrubber struct {
	store   Store
	cfg     Config
	trigger chan struct{}
	started atomic.Bool

	mu sync.Mutex
	st Status
}

func New(store Store, cfg Config) (*Scrubber, error) {
	if store == nil {
		return nil, errors.New("scrubber needs a store")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("scrub interval must be positive, got %s", cfg.Interval)
	}
	if cfg.Pause < 0 {
		return nil, fmt.Errorf("scrub pause must not be negative, got %s", cfg.Pause)
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	return &Scrubber{
		store:   store,
		cfg:     cfg,
		trigger: make(chan struct{}, 1),
		st:      Status{IntervalSeconds: cfg.Interval.Seconds(), Recent: []Event{}},
	}, nil
}

// Run scrubs every Interval, or sooner when triggered, until ctx is cancelled; passes never overlap.
func (s *Scrubber) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	timer := time.NewTimer(s.cfg.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		case <-s.trigger:
			timer.Stop()
		}
		s.pass(ctx)
		if ctx.Err() != nil {
			return nil
		}
		timer.Reset(s.cfg.Interval)
	}
}

// Trigger asks for a pass now; it returns false if one is already queued, since a single queued pass covers every request.
func (s *Scrubber) Trigger() bool {
	select {
	case s.trigger <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Scrubber) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.st
	st.Recent = make([]Event, len(s.st.Recent))
	copy(st.Recent, s.st.Recent)
	if s.st.LastPassStarted != nil {
		t := *s.st.LastPassStarted
		st.LastPassStarted = &t
	}
	return st
}

func (s *Scrubber) pass(ctx context.Context) {
	start := time.Now()
	s.update(func(st *Status) {
		st.Running = true
		st.LastPassStarted = &start
		st.LastPassScanned, st.LastPassCorrupt = 0, 0
	})
	completed := s.scan(ctx)
	s.update(func(st *Status) {
		st.Running = false
		st.LastPassDurationMs = time.Since(start).Milliseconds()
		if completed {
			st.Passes++
		}
	})
}

// scan reports whether it reached the end of the chunk list without being cancelled.
func (s *Scrubber) scan(ctx context.Context) bool {
	ids, err := s.store.List(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		s.fail("list chunks", err)
		return true
	}
	for i, id := range ids {
		if i > 0 && !s.sleep(ctx) {
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		ok, err := s.store.VerifyCRC(ctx, id)
		switch {
		case errors.Is(err, models.ErrNotFound):
			continue
		case err != nil && ctx.Err() != nil:
			return false
		case err != nil:
			s.fail("verify "+id, err)
			continue
		}
		s.update(func(st *Status) { st.ChunksScanned++; st.LastPassScanned++ })
		if !ok {
			if !s.quarantine(ctx, id) && ctx.Err() != nil {
				return false
			}
		}
	}
	return ctx.Err() == nil
}

// quarantine reports whether the chunk was moved aside.
func (s *Scrubber) quarantine(ctx context.Context, id string) bool {
	moved, err := s.store.Quarantine(ctx, id)
	switch {
	case errors.Is(err, models.ErrNotFound):
		s.cfg.Logf("scrubber: chunk %s failed its CRC check but was deleted before quarantine", id)
		return false
	case err != nil && ctx.Err() != nil:
		return false
	case err != nil && !moved:
		s.fail("quarantine "+id, err)
		return false
	case !moved:
		s.cfg.Logf("scrubber: chunk %s failed its CRC check but was replaced by a good copy before quarantine", id)
		return false
	}
	if err != nil {
		s.fail("quarantine "+id, err)
	}
	now := time.Now()
	s.update(func(st *Status) {
		st.CorruptQuarantined++
		st.LastPassCorrupt++
		st.Recent = append([]Event{{ChunkID: id, At: now}}, st.Recent...)
		if len(st.Recent) > maxRecent {
			st.Recent = st.Recent[:maxRecent]
		}
	})
	s.cfg.Logf("scrubber: chunk %s is corrupt and was quarantined", id)
	if s.cfg.OnCorrupt != nil {
		s.cfg.OnCorrupt(id)
	}
	return true
}

// sleep waits Pause and reports false if ctx was cancelled first.
func (s *Scrubber) sleep(ctx context.Context) bool {
	if s.cfg.Pause <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(s.cfg.Pause)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (s *Scrubber) fail(op string, err error) {
	s.update(func(st *Status) { st.Errors++; st.LastError = op + ": " + err.Error() })
	s.cfg.Logf("scrubber: %s: %v", op, err)
}

func (s *Scrubber) update(f func(*Status)) {
	s.mu.Lock()
	f(&s.st)
	s.mu.Unlock()
}
