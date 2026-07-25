// Package circuit implements a per-<rail>:<endpoint> circuit breaker with
// closed / open / half-open states.
package circuit

import (
	"sync"
	"time"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/metrics"
)

// State is the circuit breaker state.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// Breaker is a concurrency-safe circuit breaker scoped to a single
// <rail>:<endpoint> key. After maxFailures consecutive failures the breaker
// opens for the cool-down duration; the next call after cool-down enters
// half-open and probes the downstream.
type Breaker struct {
	name        string
	maxFailures int
	coolDown    time.Duration

	mu          sync.Mutex
	state       State
	failures    int
	openedAt    time.Time
	openCounter *metrics.Counter
}

// New constructs a Breaker. openMetricName is the name of the
// rail_circuit_open counter incremented each time the breaker opens; pass an
// empty string to skip metric registration.
func New(name string, maxFailures int, coolDown time.Duration, openMetricName string) *Breaker {
	b := &Breaker{
		name:        name,
		maxFailures: maxFailures,
		coolDown:    coolDown,
		state:       StateClosed,
	}
	if openMetricName != "" {
		b.openCounter = metrics.Default.RegisterCounter(openMetricName, "rail circuit opened events")
	}
	return b
}

// Name returns the breaker scope name.
func (b *Breaker) Name() string { return b.name }

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether a call is permitted given the current state.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(b.openedAt) >= b.coolDown {
			b.state = StateHalfOpen
			return true
		}
		return false
	case StateHalfOpen:
		return true
	}
	return false
}

// RecordSuccess resets the failure count and closes the breaker.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = StateClosed
}

// RecordFailure increments the failure count and possibly opens the breaker.
// Returns the resulting state.
func (b *Breaker) RecordFailure() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == StateHalfOpen {
		b.open()
		return b.state
	}
	if b.failures >= b.maxFailures {
		b.open()
	}
	return b.state
}

func (b *Breaker) open() {
	b.state = StateOpen
	b.openedAt = time.Now()
	if b.openCounter != nil {
		b.openCounter.Inc()
	}
}
