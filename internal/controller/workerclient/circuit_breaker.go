package workerclient

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when a worker's circuit breaker is OPEN.
var ErrCircuitOpen = errors.New("circuit breaker open — worker unavailable")

const (
	failureThreshold = 5                // consecutive failures before opening
	halfOpenTimeout  = 30 * time.Second // time in OPEN before trying HALF_OPEN
)

type cbState int

const (
	stateClosed cbState = iota
	stateOpen
	stateHalfOpen
)

// circuitBreaker is a per-worker GoF State machine: CLOSED → OPEN → HALF_OPEN → CLOSED.
type circuitBreaker struct {
	mu            sync.Mutex
	state         cbState
	failures      int
	lastFailureAt time.Time
}

func newCircuitBreaker() *circuitBreaker {
	return &circuitBreaker{state: stateClosed}
}

// Allow returns nil if the call should proceed, ErrCircuitOpen if it should be rejected.
func (cb *circuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		return nil
	case stateOpen:
		if time.Since(cb.lastFailureAt) >= halfOpenTimeout {
			cb.state = stateHalfOpen
			return nil
		}
		return ErrCircuitOpen
	case stateHalfOpen:
		return nil
	}
	return nil
}

// RecordSuccess resets the breaker to CLOSED.
func (cb *circuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = stateClosed
}

// RecordFailure increments the failure counter and opens the breaker when the threshold is hit.
func (cb *circuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailureAt = time.Now()
	if cb.failures >= failureThreshold {
		cb.state = stateOpen
	}
}

// State returns the current state string — used for logging/metrics.
func (cb *circuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case stateClosed:
		return "CLOSED"
	case stateOpen:
		return "OPEN"
	case stateHalfOpen:
		return "HALF_OPEN"
	}
	return "UNKNOWN"
}
