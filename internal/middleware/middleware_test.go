package middleware

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail/circuit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail/retry"
)

func TestCallSucceeds(t *testing.T) {
	t.Parallel()
	m := New(10 * time.Millisecond)
	var calls int32
	err := m.Call(context.Background(), "card:authorize", "p1", "authorize", func(ctx context.Context, idem string, attempt int) error {
		atomic.AddInt32(&calls, 1)
		if idem != "p1:authorize:1" {
			t.Fatalf("idem = %q", idem)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
	if m.Breaker("card:authorize").State() != circuit.StateClosed {
		t.Fatal("breaker should be closed")
	}
}

func TestCallRetriesTransient(t *testing.T) {
	t.Parallel()
	m := New(10 * time.Millisecond)
	var calls int32
	err := m.Call(context.Background(), "card:authorize", "p2", "authorize", func(ctx context.Context, idem string, attempt int) error {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return rail.NewError(rail.CodeRailUnavailable, "transient")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls < 2 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestCallTripsBreaker(t *testing.T) {
	t.Parallel()
	m := New(10 * time.Millisecond)
	var calls int32
	for i := 0; i < DefaultMaxFailures*retry.Default().MaxAttempts; i++ {
		_ = m.Call(context.Background(), "card:capture", "p3", "capture", func(ctx context.Context, idem string, attempt int) error {
			atomic.AddInt32(&calls, 1)
			return rail.NewError(rail.CodeRailUnavailable, "down")
		})
	}
	b := m.Breaker("card:capture")
	if b.State() != circuit.StateOpen {
		t.Fatalf("expected open, got %v", b.State())
	}
	err := m.Call(context.Background(), "card:capture", "p3b", "capture", func(ctx context.Context, idem string, attempt int) error {
		t.Fatal("should not call fn when open")
		return nil
	})
	if err == nil {
		t.Fatal("expected RAIL_UNAVAILABLE")
	}
	re, ok := err.(*rail.Error)
	if !ok || re.Code != rail.CodeRailUnavailable {
		t.Fatalf("err = %v", err)
	}
}

func TestCallNonRetryableNoRetry(t *testing.T) {
	t.Parallel()
	m := New(10 * time.Millisecond)
	var calls int32
	nonRetry := rail.NewError(rail.CodeInsufficientFunds, "nope")
	err := m.Call(context.Background(), "card:refund", "p4", "refund", func(ctx context.Context, idem string, attempt int) error {
		atomic.AddInt32(&calls, 1)
		return nonRetry
	})
	if !errors.Is(err, nonRetry) {
		t.Fatalf("err = %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestBreakerResetsOnSuccess(t *testing.T) {
	t.Parallel()
	m := New(10 * time.Millisecond)
	var calls int32
	_ = m.Call(context.Background(), "ach:authorize", "p5", "authorize", func(ctx context.Context, idem string, attempt int) error {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return rail.NewError(rail.CodeRailUnavailable, "x")
		}
		return nil
	})
	if m.Breaker("ach:authorize").State() != circuit.StateClosed {
		t.Fatal("breaker should close after success")
	}
}

func TestEnvMaxFailures(t *testing.T) {
	t.Parallel()
	if DefaultMaxFailures < 1 {
		t.Fatal("DefaultMaxFailures too low")
	}
}
