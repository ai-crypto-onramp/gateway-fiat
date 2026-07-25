// Package dummy implements a single in-process RailConnector used in tests
// and local environments. In the simplified implementation all rail families
// (card, ach, sepa, pix, upi) resolve to this connector.
package dummy

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/settlement"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
	"github.com/google/uuid"
)

// Config configures the DummyRailConnector.
type Config struct {
	Fail      bool // force every operation to fail with RAIL_UNAVAILABLE
	Rail      string
	AuditSink audit.Sink
}

// Connector is the in-memory dummy rail connector.
type Connector struct {
	cfg      Config
	store    store.Store
	tracker  *settlement.Tracker
	audit    audit.Sink
	mu       sync.Mutex
	failFlag bool
}

// New constructs a DummyRailConnector backed by s and tracked by t.
func New(s store.Store, t *settlement.Tracker, cfg Config) *Connector {
	if cfg.Rail == "" {
		cfg.Rail = "dummy"
	}
	if cfg.AuditSink == nil {
		cfg.AuditSink = audit.NewRecorder()
	}
	fail := cfg.Fail || envBool("RAIL_DUMMY_FAIL")
	return &Connector{cfg: cfg, store: s, tracker: t, audit: cfg.AuditSink, failFlag: fail}
}

func envBool(key string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes"
}

// SetFail lets tests flip the failure flag at runtime.
func (c *Connector) SetFail(b bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failFlag = b
}

func (c *Connector) failOn() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failFlag
}

// Authorize reserves funds on the dummy rail and records a pending request.
func (c *Connector) Authorize(ctx context.Context, in rail.Context) (rail.Response, error) {
	if in.PaymentID == "" {
		return rail.Response{Status: rail.StatusFailed, ErrorCode: rail.CodeInvalidRequest, ErrorMessage: "missing payment_id"}, nil
	}
	if c.failOn() {
		c.emit(in, "authorize", rail.StatusFailed, in.Amount)
		return rail.Response{
			Status:       rail.StatusFailed,
			ErrorCode:    rail.CodeRailUnavailable,
			ErrorMessage: "dummy rail forced failure",
		}, nil
	}
	ref := "r-" + uuid.NewString()
	c.store.Upsert(store.Record{
		PaymentID:      in.PaymentID,
		Rail:           c.cfg.Rail,
		Operation:      "authorize",
		Amount:         in.Amount,
		Currency:       in.Currency,
		Status:         rail.StatusAuthorized,
		IdempotencyKey: in.IdempotencyKey,
		RailRef:        ref,
	})
	c.emit(in, "authorize", rail.StatusAuthorized, in.Amount)
	return rail.Response{Status: rail.StatusAuthorized, RailRef: ref, RawResponse: []byte(`{"dummy":true}`)}, nil
}

// Capture settles a previously authorized amount.
func (c *Connector) Capture(ctx context.Context, in rail.Context, amount decimal.Decimal) (rail.Response, error) {
	if in.PaymentID == "" {
		return rail.Response{Status: rail.StatusFailed, ErrorCode: rail.CodeInvalidRequest, ErrorMessage: "missing payment_id"}, nil
	}
	if c.failOn() {
		c.emit(in, "capture", rail.StatusFailed, amount)
		return rail.Response{
			Status:       rail.StatusFailed,
			ErrorCode:    rail.CodeRailUnavailable,
			ErrorMessage: "dummy rail forced failure",
		}, nil
	}
	rec, ok := c.store.Get(in.PaymentID)
	if !ok {
		return rail.Response{Status: rail.StatusFailed, ErrorCode: rail.CodeInvalidRequest, ErrorMessage: "unknown payment_id"}, nil
	}
	if rec.Status != rail.StatusAuthorized {
		return rail.Response{
			Status:       rail.StatusFailed,
			ErrorCode:    rail.CodeInvalidRequest,
			ErrorMessage: fmt.Sprintf("cannot capture from status %q", rec.Status),
		}, nil
	}
	c.store.SetStatus(in.PaymentID, rail.StatusCaptured, "", "")
	if c.tracker != nil {
		c.tracker.RecordCapture(c.cfg.Rail, amount)
	}
	c.emit(in, "capture", rail.StatusCaptured, amount)
	return rail.Response{Status: rail.StatusCaptured, RailRef: rec.RailRef}, nil
}

// Refund returns a captured/settled amount.
func (c *Connector) Refund(ctx context.Context, in rail.Context, amount decimal.Decimal) (rail.Response, error) {
	if in.PaymentID == "" {
		return rail.Response{Status: rail.StatusFailed, ErrorCode: rail.CodeInvalidRequest, ErrorMessage: "missing payment_id"}, nil
	}
	if c.failOn() {
		c.emit(in, "refund", rail.StatusFailed, amount)
		return rail.Response{
			Status:       rail.StatusFailed,
			ErrorCode:    rail.CodeRailUnavailable,
			ErrorMessage: "dummy rail forced failure",
		}, nil
	}
	rec, ok := c.store.Get(in.PaymentID)
	if !ok {
		return rail.Response{Status: rail.StatusFailed, ErrorCode: rail.CodeInvalidRequest, ErrorMessage: "unknown payment_id"}, nil
	}
	if rec.Status != rail.StatusCaptured && rec.Status != rail.StatusSettled {
		return rail.Response{
			Status:       rail.StatusFailed,
			ErrorCode:    rail.CodeInvalidRequest,
			ErrorMessage: fmt.Sprintf("cannot refund from status %q", rec.Status),
		}, nil
	}
	c.store.SetStatus(in.PaymentID, rail.StatusRefunded, "", "")
	if c.tracker != nil {
		c.tracker.RecordRefund(c.cfg.Rail, amount)
	}
	c.emit(in, "refund", rail.StatusRefunded, amount)
	return rail.Response{Status: rail.StatusRefunded, RailRef: rec.RailRef}, nil
}

// GetStatus returns the current status from the in-memory store.
func (c *Connector) GetStatus(ctx context.Context, in rail.Context) (rail.Status, error) {
	if in.PaymentID == "" {
		return rail.StatusUnknown, rail.NewError(rail.CodeInvalidRequest, "missing payment_id")
	}
	rec, ok := c.store.Get(in.PaymentID)
	if !ok {
		return rail.StatusUnknown, rail.NewError(rail.CodeInvalidRequest, "unknown payment_id")
	}
	return rec.Status, nil
}

func (c *Connector) emit(in rail.Context, op string, status rail.Status, amount decimal.Decimal) {
	if c.audit == nil {
		return
	}
	c.audit.Emit(audit.Event{
		Type:      "rail." + op,
		PaymentID: in.PaymentID,
		Rail:      c.cfg.Rail,
		Operation: op,
		Status:    string(status),
		Amount:    amount,
		Payload: map[string]any{
			"currency": in.Currency,
			"payer":    in.PayerRef,
		},
	})
}

// FormatAmount is a small helper used by tests to round-trip an amount.
func FormatAmount(amount decimal.Decimal) string {
	return amount.String()
}
