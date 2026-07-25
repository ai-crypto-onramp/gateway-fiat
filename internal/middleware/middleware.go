// Package middleware wraps outbound rail calls in a per-<rail>:<endpoint>
// circuit breaker and retry-with-jitter middleware. When the circuit is
// open it returns a normalized rail.RAIL_UNAVAILABLE error without invoking
// the downstream.
package middleware

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail/circuit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail/retry"
)

// DefaultMaxFailures is read from CIRCUIT_MAX_FAILURES at init time.
var DefaultMaxFailures = envInt("CIRCUIT_MAX_FAILURES", 5)

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return def
	}
	return n
}

// Middleware wraps outbound rail calls with a circuit breaker + retry.
type Middleware struct {
	mu       sync.Mutex
	breakers map[string]*circuit.Breaker
	maxFail  int
	coolDown time.Duration
}

// New constructs a Middleware with the given cool-down duration.
func New(coolDown time.Duration) *Middleware {
	return &Middleware{
		breakers: map[string]*circuit.Breaker{},
		maxFail:  DefaultMaxFailures,
		coolDown: coolDown,
	}
}

// breaker returns the breaker for the given scope, creating one if needed.
func (m *Middleware) breaker(scope string) *circuit.Breaker {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.breakers[scope]
	if !ok {
		b = circuit.New(scope, m.maxFail, m.coolDown, "rail_circuit_open")
		m.breakers[scope] = b
	}
	return b
}

// Call wraps fn in the circuit breaker + retry middleware. scope is the
// "<rail>:<endpoint>" key; paymentID / operation are used for idempotency key
// derivation. On a non-retryable error the breaker records a failure; on
// success it resets. When the breaker is open the call returns a normalized
// rail.RAIL_UNAVAILABLE error immediately without invoking fn.
func (m *Middleware) Call(ctx context.Context, scope, paymentID, operation string, fn func(ctx context.Context, idemKey string, attempt int) error) error {
	b := m.breaker(scope)
	if !b.Allow() {
		return rail.NewError(rail.CodeRailUnavailable, "circuit open for "+scope)
	}
	opts := retry.Default()
	err := retry.Do(ctx, paymentID, operation, opts, func(ctx context.Context, idemKey string, attempt int) error {
		if !b.Allow() {
			return rail.NewError(rail.CodeRailUnavailable, "circuit open for "+scope)
		}
		err := fn(ctx, idemKey, attempt)
		if err == nil {
			b.RecordSuccess()
			return nil
		}
		if isRetryableErr(err) {
			b.RecordFailure()
		}
		return err
	})
	if err != nil {
		if _, ok := err.(*rail.Error); !ok {
			err = rail.AsError(err)
		}
	}
	return err
}

func isRetryableErr(err error) bool {
	if err == nil {
		return false
	}
	if re, ok := err.(*rail.Error); ok {
		return re.Code == rail.CodeRailUnavailable
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "timeout") ||
		strings.Contains(s, "no such host") ||
		strings.Contains(s, "dial")
}

// Breaker exposes the circuit breaker for a scope (for tests / inspection).
func (m *Middleware) Breaker(scope string) *circuit.Breaker {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.breakers[scope]
}
