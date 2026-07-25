// Package retry implements exponential-backoff-with-jitter retry of
// idempotent outbound calls, capped by RETRY_MAX_ATTEMPTS.
package retry

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
)

// MaxAttempts is the maximum number of retry attempts per outbound call. It
// is read from RETRY_MAX_ATTEMPTS at package init time and may also be
// overridden per-call via Options.MaxAttempts.
var MaxAttempts = envInt("RETRY_MAX_ATTEMPTS", 4)

// Base is the base backoff duration for the first retry.
var Base = 100 * time.Millisecond

// Cap is the maximum backoff duration.
var Cap = 2 * time.Second

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

// Options configures a single Do call.
type Options struct {
	MaxAttempts int
	Base        time.Duration
	Cap         time.Duration
	IsRetryable func(error) bool
}

// Default is the default retry options: up to MaxAttempts attempts, with
// exponential backoff capped at Cap, retrying only on RAIL_UNAVAILABLE.
func Default() Options {
	return Options{
		MaxAttempts: MaxAttempts,
		Base:        Base,
		Cap:         Cap,
		IsRetryable: func(err error) bool {
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
		},
	}
}

// Do runs fn with retry. The idempotency key passed to fn carries the
// current attempt number so the downstream can dedupe.
func Do(ctx context.Context, paymentID, operation string, opts Options, fn func(ctx context.Context, idemKey string, attempt int) error) error {
	max := opts.MaxAttempts
	if max < 1 {
		max = 1
	}
	base := opts.Base
	if base <= 0 {
		base = Base
	}
	capD := opts.Cap
	if capD <= 0 {
		capD = Cap
	}
	isRetry := opts.IsRetryable
	if isRetry == nil {
		isRetry = Default().IsRetryable
	}
	var lastErr error
	for attempt := 1; attempt <= max; attempt++ {
		idem := rail.IdempotencyKey(paymentID, operation, attempt)
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx, idem, attempt)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetry(err) {
			return err
		}
		if attempt == max {
			break
		}
		backoff := backoff(base, capD, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return lastErr
}

// backoff computes an exponential backoff with full jitter for the given
// attempt: base * 2^(attempt-1), capped at cap, with jitter in [0, backoff).
func backoff(base, cap time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d > cap {
			d = cap
			break
		}
	}
	if d <= 0 {
		d = base
	}
	jitter := time.Duration(rand.Int64N(int64(d)))
	return jitter
}
