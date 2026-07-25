// Package card implements the RailConnector for card payments by delegating
// Authorize/Capture/Refund/Status to a configurable card processor (Stripe or
// Adyen, selected via RAIL_CARD_PROCESSOR).
package card

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/audit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/card/adyen"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/card/stripe"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/metrics"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
)

// Default stripe API base URL.
const StripeBaseURL = "https://api.stripe.com"

// Default adyen API base URL.
const AdyenBaseURL = "https://checkout-test.adyen.com"

// Config configures the card adapter.
type Config struct {
	Processor string // "stripe" or "adyen"
	APIKey    string
	BaseURL   string // optional override (for tests)
	Merchant  string // adyen merchant account
}

// Connector implements rail.Connector for the card rail.
type Connector struct {
	cfg       Config
	processor Processor
	procName  string
	store     store.Store
	audit     audit.Sink
	log       *slog.Logger
	authLat   *metrics.Histogram
	capLat    *metrics.Histogram
}

var (
	authLatency = metrics.Default.RegisterHistogram(
		"rail_authorize_latency", "card authorize latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
	captureLatency = metrics.Default.RegisterHistogram(
		"rail_capture_latency", "card capture latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
)

// New constructs a card Connector. The processor is selected from
// cfg.Processor; when empty, RAIL_CARD_PROCESSOR is consulted.
func New(cfg Config, s store.Store, as audit.Sink) (*Connector, error) {
	proc := strings.ToLower(strings.TrimSpace(cfg.Processor))
	if proc == "" {
		proc = strings.ToLower(strings.TrimSpace(os.Getenv("RAIL_CARD_PROCESSOR")))
	}
	if proc == "" {
		return nil, ErrProcessor
	}
	baseURL := cfg.BaseURL
	apiKey := cfg.APIKey
	var p Processor
	switch proc {
	case "stripe":
		if baseURL == "" {
			baseURL = StripeBaseURL
		}
		p = wrapStripe(stripe.New(baseURL, apiKey))
	case "adyen":
		if baseURL == "" {
			baseURL = AdyenBaseURL
		}
		p = wrapAdyen(adyen.New(baseURL, apiKey, cfg.Merchant))
	default:
		return nil, ErrProcessor
	}
	if as == nil {
		as = audit.NewRecorder()
	}
	return &Connector{
		cfg:       cfg,
		processor: p,
		procName:  proc,
		store:     s,
		audit:     as,
		log:       slog.Default(),
		authLat:   authLatency,
		capLat:    captureLatency,
	}, nil
}

// Authorize reserves funds on the card rail.
func (c *Connector) Authorize(ctx context.Context, in rail.Context) (rail.Response, error) {
	if in.PaymentID == "" {
		return failResp(rail.CodeInvalidRequest, "missing payment_id"), nil
	}
	start := time.Now()
	defer func() { c.authLat.Observe(time.Since(start).Seconds()) }()
	idem := in.IdempotencyKey
	if idem == "" {
		idem = rail.IdempotencyKey(in.PaymentID, "authorize", max1(in.Attempt))
	}
	railRef, status, err := c.processor.Authorize(ctx, in.PaymentID, toMinor(in.Amount), in.Currency, idem)
	c.log.Info("card authorize",
		"tx_id", in.PaymentID, "rail", "card", "rail_request_id", idem,
		"status", status, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "authorize", rail.StatusFailed, idem, railRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := mapStripeStatus(status)
	c.store.Upsert(store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "card",
		Operation:      "authorize",
		Amount:         in.Amount,
		Currency:       in.Currency,
		Status:         st,
		IdempotencyKey: idem,
		RailRef:        railRef,
	})
	c.persist(in, "authorize", st, idem, railRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.authorize", PaymentID: in.PaymentID, Rail: "card", Operation: "authorize", Status: string(st), Amount: in.Amount})
	return rail.Response{Status: st, RailRef: railRef}, nil
}

// Capture settles a previously authorized amount.
func (c *Connector) Capture(ctx context.Context, in rail.Context, amount decimal.Decimal) (rail.Response, error) {
	if in.PaymentID == "" {
		return failResp(rail.CodeInvalidRequest, "missing payment_id"), nil
	}
	start := time.Now()
	defer func() { c.capLat.Observe(time.Since(start).Seconds()) }()
	rec, ok := c.store.Get(in.PaymentID)
	if !ok {
		return failResp(rail.CodeInvalidRequest, "unknown payment_id"), nil
	}
	if rec.RailRef == "" {
		return failResp(rail.CodeInvalidRequest, "no rail_ref for payment"), nil
	}
	idem := in.IdempotencyKey
	if idem == "" {
		idem = rail.IdempotencyKey(in.PaymentID, "capture", 1)
	}
	status, err := c.processor.Capture(ctx, rec.RailRef, toMinor(amount), in.Currency, idem)
	c.log.Info("card capture",
		"tx_id", in.PaymentID, "rail", "card", "rail_request_id", idem,
		"status", status, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := mapStripeStatus(status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "capture", st, idem, rec.RailRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.capture", PaymentID: in.PaymentID, Rail: "card", Operation: "capture", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: rec.RailRef}, nil
}

// Refund returns captured funds.
func (c *Connector) Refund(ctx context.Context, in rail.Context, amount decimal.Decimal) (rail.Response, error) {
	if in.PaymentID == "" {
		return failResp(rail.CodeInvalidRequest, "missing payment_id"), nil
	}
	rec, ok := c.store.Get(in.PaymentID)
	if !ok {
		return failResp(rail.CodeInvalidRequest, "unknown payment_id"), nil
	}
	idem := in.IdempotencyKey
	if idem == "" {
		idem = rail.IdempotencyKey(in.PaymentID, "refund", 1)
	}
	status, err := c.processor.Refund(ctx, rec.RailRef, toMinor(amount), in.Currency, idem)
	c.log.Info("card refund",
		"tx_id", in.PaymentID, "rail", "card", "rail_request_id", idem,
		"status", status, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "refund", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := mapRefundStatus(status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "refund", st, idem, rec.RailRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.refund", PaymentID: in.PaymentID, Rail: "card", Operation: "refund", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: rec.RailRef}, nil
}

// GetStatus queries the current payment status on the rail.
func (c *Connector) GetStatus(ctx context.Context, in rail.Context) (rail.Status, error) {
	if in.PaymentID == "" {
		return rail.StatusUnknown, rail.NewError(rail.CodeInvalidRequest, "missing payment_id")
	}
	rec, ok := c.store.Get(in.PaymentID)
	if !ok {
		return rail.StatusUnknown, rail.NewError(rail.CodeInvalidRequest, "unknown payment_id")
	}
	if rec.RailRef == "" {
		return rec.Status, nil
	}
	status, err := c.processor.Status(ctx, rec.RailRef)
	if err != nil {
		re := c.mapErr(err)
		return rail.StatusUnknown, re
	}
	st := mapStripeStatus(status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	return st, nil
}

// mapErr maps a processor error to the normalized rail error taxonomy.
func (c *Connector) mapErr(err error) *rail.Error {
	if err == nil {
		return nil
	}
	// Network / 5xx -> RAIL_UNAVAILABLE
	if ae, ok := err.(*stripe.APIError); ok && ae.Status >= 500 {
		return rail.NewError(rail.CodeRailUnavailable, err.Error())
	}
	if ae, ok := err.(*adyen.APIError); ok && ae.Status >= 500 {
		return rail.NewError(rail.CodeRailUnavailable, err.Error())
	}
	if isTransport(err) {
		return rail.NewError(rail.CodeRailUnavailable, err.Error())
	}
	code := c.processor.DeclineCode(err)
	if code == "" {
		return rail.AsError(err)
	}
	return MapDecline(c.procName, code)
}

func isTransport(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "no such host") ||
		strings.Contains(s, "timeout") ||
		strings.Contains(s, "i/o timeout") ||
		strings.Contains(s, "dial")
}

func (c *Connector) persist(in rail.Context, op string, st rail.Status, idem, ref string, re *rail.Error) {
	if c.store == nil {
		return
	}
	rec := store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "card",
		Operation:      op,
		Amount:         in.Amount,
		Currency:       in.Currency,
		Status:         st,
		IdempotencyKey: idem,
		RailRef:        ref,
	}
	if re != nil {
		rec.ErrorCode = re.Code
		rec.ErrorMessage = re.Reason
	}
	c.store.Upsert(rec)
}

// mapStripeStatus translates processor status strings to rail.Status.
// Handles both Stripe ("succeeded", "requires_capture", "canceled") and
// Adyen ("Authorised", "Settled", "Refused", "Refunded") status values.
func mapStripeStatus(s string) rail.Status {
	switch strings.ToLower(s) {
	case "succeeded", "settled", "captured":
		return rail.StatusCaptured
	case "requires_capture", "authorized", "authorised":
		return rail.StatusAuthorized
	case "refunded", "refund":
		return rail.StatusRefunded
	case "canceled", "cancelled", "refused", "failed":
		return rail.StatusFailed
	case "pending":
		return rail.StatusPending
	default:
		if s == "" {
			return rail.StatusUnknown
		}
		return rail.StatusPending
	}
}

// mapRefundStatus maps processor refund responses. Stripe refunds return
// "succeeded" for a completed refund; we translate that to StatusRefunded
// rather than StatusCaptured.
func mapRefundStatus(s string) rail.Status {
	switch strings.ToLower(s) {
	case "succeeded", "received", "refunded", "refund":
		return rail.StatusRefunded
	case "pending":
		return rail.StatusPending
	case "failed", "canceled", "cancelled":
		return rail.StatusFailed
	default:
		return mapStripeStatus(s)
	}
}

func toMinor(amount decimal.Decimal) int64 {
	// truncate to cents, then round to nearest cent
	cents := amount.Mul(decimal.NewFromInt(100))
	f, _ := cents.Float64()
	if f >= 0 {
		return int64(f + 0.5)
	}
	return int64(f - 0.5)
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func failResp(code, msg string) rail.Response {
	return rail.Response{Status: rail.StatusFailed, ErrorCode: code, ErrorMessage: msg}
}

// wrapStripe adapts the stripe client to the Processor interface.
func wrapStripe(c *stripe.Client) Processor { return &stripeProc{c} }

type stripeProc struct{ c *stripe.Client }

func (p *stripeProc) Authorize(ctx context.Context, ref string, amountMinor int64, currency, idemKey string) (string, string, error) {
	return p.c.Authorize(ctx, ref, amountMinor, currency, idemKey)
}
func (p *stripeProc) Capture(ctx context.Context, railRef string, amountMinor int64, currency, idemKey string) (string, error) {
	return p.c.Capture(ctx, railRef, amountMinor, currency, idemKey)
}
func (p *stripeProc) Refund(ctx context.Context, railRef string, amountMinor int64, currency, idemKey string) (string, error) {
	return p.c.Refund(ctx, railRef, amountMinor, currency, idemKey)
}
func (p *stripeProc) Status(ctx context.Context, railRef string) (string, error) {
	return p.c.Status(ctx, railRef)
}
func (p *stripeProc) DeclineCode(err error) string { return stripe.DeclineCode(err) }

// wrapAdyen adapts the adyen client to the Processor interface.
func wrapAdyen(c *adyen.Client) Processor { return &adyenProc{c} }

type adyenProc struct{ c *adyen.Client }

func (p *adyenProc) Authorize(ctx context.Context, ref string, amountMinor int64, currency, idemKey string) (string, string, error) {
	return p.c.Authorize(ctx, ref, amountMinor, currency, idemKey)
}
func (p *adyenProc) Capture(ctx context.Context, railRef string, amountMinor int64, currency, idemKey string) (string, error) {
	return p.c.Capture(ctx, railRef, amountMinor, currency, idemKey)
}
func (p *adyenProc) Refund(ctx context.Context, railRef string, amountMinor int64, currency, idemKey string) (string, error) {
	return p.c.Refund(ctx, railRef, amountMinor, currency, idemKey)
}
func (p *adyenProc) Status(ctx context.Context, railRef string) (string, error) {
	return p.c.Status(ctx, railRef)
}
func (p *adyenProc) DeclineCode(err error) string { return adyen.DeclineCode(err) }

func init() {} // no-op; card adapter is tested via New, not the registry
