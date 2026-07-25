package retry

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
)

func TestDoSucceedsOnFirst(t *testing.T) {
	t.Parallel()
	var calls int32
	err := Do(context.Background(), "p1", "authorize", Options{MaxAttempts: 3, Base: time.Millisecond, Cap: 10 * time.Millisecond}, func(ctx context.Context, idem string, attempt int) error {
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
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	var calls int32
	err := Do(context.Background(), "p2", "authorize", Options{MaxAttempts: 4, Base: time.Millisecond, Cap: 10 * time.Millisecond}, func(ctx context.Context, idem string, attempt int) error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return rail.NewError(rail.CodeRailUnavailable, "transient")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestDoSurfacesNonRetryable(t *testing.T) {
	t.Parallel()
	var calls int32
	nonRetry := rail.NewError(rail.CodeInsufficientFunds, "nope")
	err := Do(context.Background(), "p3", "authorize", Options{MaxAttempts: 5, Base: time.Millisecond, Cap: 10 * time.Millisecond}, func(ctx context.Context, idem string, attempt int) error {
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

func TestDoExhaustsAttempts(t *testing.T) {
	t.Parallel()
	var calls int32
	err := Do(context.Background(), "p4", "authorize", Options{MaxAttempts: 3, Base: time.Millisecond, Cap: 10 * time.Millisecond}, func(ctx context.Context, idem string, attempt int) error {
		atomic.AddInt32(&calls, 1)
		return rail.NewError(rail.CodeRailUnavailable, "still down")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 3 {
		t.Fatalf("calls = %d", calls)
	}
	re, ok := err.(*rail.Error)
	if !ok || re.Code != rail.CodeRailUnavailable {
		t.Fatalf("err = %v", err)
	}
}

func TestDoIdempotencyKeysPerAttempt(t *testing.T) {
	t.Parallel()
	keys := map[int]string{}
	err := Do(context.Background(), "p5", "capture", Options{MaxAttempts: 3, Base: time.Millisecond, Cap: 5 * time.Millisecond}, func(ctx context.Context, idem string, attempt int) error {
		keys[attempt] = idem
		if attempt < 2 {
			return rail.NewError(rail.CodeRailUnavailable, "x")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if keys[1] != "p5:capture:1" || keys[2] != "p5:capture:2" {
		t.Fatalf("keys = %+v", keys)
	}
}

func TestDoContextCancelStops(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Do(ctx, "p6", "authorize", Options{MaxAttempts: 5, Base: time.Millisecond, Cap: 5 * time.Millisecond}, func(ctx context.Context, idem string, attempt int) error {
		return rail.NewError(rail.CodeRailUnavailable, "x")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultMaxAttempts(t *testing.T) {
	t.Parallel()
	d := Default()
	if d.MaxAttempts != MaxAttempts {
		t.Fatalf("max = %d", d.MaxAttempts)
	}
	if d.IsRetryable == nil {
		t.Fatal("IsRetryable nil")
	}
	if d.IsRetryable(rail.NewError(rail.CodeRailUnavailable, "x")) != true {
		t.Fatal("should retry RAIL_UNAVAILABLE")
	}
	if d.IsRetryable(rail.NewError(rail.CodeInsufficientFunds, "x")) != false {
		t.Fatal("should not retry NSF")
	}
	if d.IsRetryable(nil) != false {
		t.Fatal("nil not retryable")
	}
}

func TestBackoffCaps(t *testing.T) {
	t.Parallel()
	d := backoff(time.Millisecond, 10*time.Millisecond, 20)
	if d > 10*time.Millisecond {
		t.Fatalf("backoff = %v", d)
	}
	d2 := backoff(time.Millisecond, 10*time.Millisecond, 1)
	if d2 > time.Millisecond {
		t.Fatalf("backoff = %v", d2)
	}
}
