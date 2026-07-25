// Package pix implements the RailConnector for PIX instant payments against
// the Banco Central do Brasil SPI, including DICT key resolution.
package pix

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/metrics"
	"github.com/ai-crypto-onramp/rail-connector/internal/pix/spi"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
)

// Default SPI base URL.
const DefaultBaseURL = "http://localhost:8080"

// Config configures the PIX adapter.
type Config struct {
	BaseURL string
	APIKey  string
}

// Connector implements rail.Connector for the PIX rail.
type Connector struct {
	cfg     Config
	client  *spi.Client
	store   store.Store
	audit   audit.Sink
	log     *slog.Logger
	authLat *metrics.Histogram
	capLat  *metrics.Histogram
}

var (
	authLatency = metrics.Default.RegisterHistogram(
		"rail_authorize_latency", "pix authorize latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
	captureLatency = metrics.Default.RegisterHistogram(
		"rail_capture_latency", "pix capture latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
)

// New constructs a PIX Connector.
func New(cfg Config, s store.Store, as audit.Sink) (*Connector, error) {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = os.Getenv("RAIL_PIX_API_URL")
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("RAIL_PIX_API_KEY")
	}
	if as == nil {
		as = audit.NewRecorder()
	}
	return &Connector{cfg: cfg, client: spi.New(baseURL, apiKey), store: s, audit: as, log: slog.Default(), authLat: authLatency, capLat: captureLatency}, nil
}

// Authorize initiates a PIX instant payment. Authorize+Capture are a single
// instant payment; Authorize resolves the DICT key and submits the payment,
// treating CONFIRMED as Authorized.
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
	pixKey := in.RailSpecific["pix_key"]
	if pixKey == "" {
		c.persist(in, "authorize", rail.StatusFailed, idem, "", rail.NewError(rail.CodeInvalidRequest, "missing pix_key"))
		return failResp(rail.CodeInvalidRequest, "missing pix_key"), nil
	}
	dict, err := c.client.ResolveDICT(ctx, pixKey)
	c.log.Info("pix dict resolve", "tx_id", in.PaymentID, "rail", "pix", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "authorize", rail.StatusFailed, idem, "", re)
		return failResp(re.Code, re.Reason), nil
	}
	pr, err := c.client.InitiatePayment(ctx, idem, in.PaymentID, in.PayerRef, dict.Account, dict.BankCode, in.Amount, in.Currency)
	c.log.Info("pix payment initiate", "tx_id", in.PaymentID, "rail", "pix", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "authorize", rail.StatusFailed, idem, "", re)
		return failResp(re.Code, re.Reason), nil
	}
	if pr.ReturnCode != "" && pr.Status != "CONFIRMED" {
		re := mapReasonCode(pr.ReturnCode)
		c.persist(in, "authorize", rail.StatusFailed, idem, pr.PaymentID, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := mapStatus(pr.Status)
	c.store.Upsert(store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "pix",
		Operation:      "authorize",
		Amount:         in.Amount,
		Currency:       in.Currency,
		Status:         st,
		IdempotencyKey: idem,
		RailRef:        pr.PaymentID,
	})
	c.persist(in, "authorize", st, idem, pr.PaymentID, nil)
	c.audit.Emit(audit.Event{Type: "rail.authorize", PaymentID: in.PaymentID, Rail: "pix", Operation: "authorize", Status: string(st), Amount: in.Amount})
	return rail.Response{Status: st, RailRef: pr.PaymentID}, nil
}

// Capture is a no-op for PIX (instant payment is already settled at
// Authorize); we just mark the status Captured if currently Authorized.
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
	idem := in.IdempotencyKey
	if idem == "" {
		idem = rail.IdempotencyKey(in.PaymentID, "capture", 1)
	}
	// Poll SPI for the latest status.
	pr, err := c.client.GetPayment(ctx, rec.RailRef)
	c.log.Info("pix capture (poll)", "tx_id", in.PaymentID, "rail", "pix", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := mapStatus(pr.Status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "capture", st, idem, rec.RailRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.capture", PaymentID: in.PaymentID, Rail: "pix", Operation: "capture", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: rec.RailRef}, nil
}

// Refund initiates a PIX refund.
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
	pr, err := c.client.RefundPayment(ctx, idem, rec.RailRef, amount, in.Currency)
	c.log.Info("pix refund", "tx_id", in.PaymentID, "rail", "pix", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "refund", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := rail.StatusRefunded
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "refund", st, idem, pr.PaymentID, nil)
	c.audit.Emit(audit.Event{Type: "rail.refund", PaymentID: in.PaymentID, Rail: "pix", Operation: "refund", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: pr.PaymentID}, nil
}

// GetStatus polls the SPI for the current payment status.
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
	pr, err := c.client.GetPayment(ctx, rec.RailRef)
	if err != nil {
		re := c.mapErr(err)
		return rail.StatusUnknown, re
	}
	st := mapStatus(pr.Status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	return st, nil
}

func (c *Connector) mapErr(err error) *rail.Error {
	if err == nil {
		return nil
	}
	if ae, ok := err.(*spi.APIError); ok {
		if ae.Status >= 500 {
			return rail.NewError(rail.CodeRailUnavailable, err.Error())
		}
		if ae.Code != "" {
			return mapReasonCode(ae.Code)
		}
	}
	s := err.Error()
	if strings.Contains(s, "connection refused") || strings.Contains(s, "no such host") || strings.Contains(s, "timeout") || strings.Contains(s, "dial") {
		return rail.NewError(rail.CodeRailUnavailable, s)
	}
	return rail.AsError(err)
}

func (c *Connector) persist(in rail.Context, op string, st rail.Status, idem, ref string, re *rail.Error) {
	if c.store == nil {
		return
	}
	rec := store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "pix",
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

func mapStatus(s string) rail.Status {
	switch strings.ToUpper(s) {
	case "CONFIRMED", "CONCLUDED":
		return rail.StatusCaptured
	case "AUTHORIZED", "ACTIVE":
		return rail.StatusAuthorized
	case "REFUNDED":
		return rail.StatusRefunded
	case "REJECTED", "FAILED", "CANCELLED", "CANCELED":
		return rail.StatusFailed
	case "PROCESSING", "PENDING":
		return rail.StatusPending
	default:
		return rail.StatusUnknown
	}
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
