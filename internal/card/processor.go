package card

import (
	"context"
	"errors"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
)

// Processor is the interface a card processor client implements. Both the
// Stripe and Adyen clients satisfy it.
type Processor interface {
	// Authorize reserves funds and returns the rail reference + status.
	Authorize(ctx context.Context, ref string, amountMinor int64, currency, idemKey string) (railRef, status string, err error)
	// Capture settles a previously authorized amount.
	Capture(ctx context.Context, railRef string, amountMinor int64, currency, idemKey string) (status string, err error)
	// Refund returns captured funds.
	Refund(ctx context.Context, railRef string, amountMinor int64, currency, idemKey string) (status string, err error)
	// Status queries the current payment status.
	Status(ctx context.Context, railRef string) (status string, err error)
	// DeclineCode extracts the processor-specific decline code from an error.
	DeclineCode(err error) string
}

// ErrProcessor is returned when the configured processor name is unknown.
var ErrProcessor = errors.New("unknown card processor")

// MapDecline maps a processor decline code to a normalized rail error using
// the per-processor mapping tables.
func MapDecline(processor, code string) *rail.Error {
	switch processor {
	case "stripe":
		return mapStripeDecline(code)
	case "adyen":
		return mapAdyenDecline(code)
	default:
		return rail.NewError(rail.CodeDoNotHonor, code)
	}
}
