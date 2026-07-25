// Package upi implements the RailConnector for UPI Collect / Intent flows
// against the NPCI UPI APIs, including collect, status, refund, and dispute
// (chargeback) recording.
package upi

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/metrics"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
	"github.com/ai-crypto-onramp/rail-connector/internal/upi/npci"
)

// Default NPCI base URL.
const DefaultBaseURL = "http://localhost:8080"

// Config configures the UPI adapter.
type Config struct {
	BaseURL  string
	APIKey   string
	PayeeVPA string
}

// Connector implements rail.Connector for the UPI rail. It also exposes a
// UPI-specific Chargeback method.
type Connector struct {
	cfg        Config
	client     *npci.Client
	store      store.Store
	audit      audit.Sink
	log        *slog.Logger
	authLat    *metrics.Histogram
	capLat     *metrics.Histogram
	chargeRate *metrics.Counter
}

var (
	authLatency = metrics.Default.RegisterHistogram(
		"rail_authorize_latency", "upi authorize latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
	captureLatency = metrics.Default.RegisterHistogram(
		"rail_capture_latency", "upi capture latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
	chargebackRate = metrics.Default.RegisterCounter(
		"rail_chargeback_rate", "upi chargeback events received",
	)
)

// New constructs a UPI Connector.
func New(cfg Config, s store.Store, as audit.Sink) (*Connector, error) {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = os.Getenv("RAIL_UPI_API_URL")
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("RAIL_UPI_API_KEY")
	}
	if as == nil {
		as = audit.NewRecorder()
	}
	return &Connector{cfg: cfg, client: npci.New(baseURL, apiKey), store: s, audit: as, log: slog.Default(), authLat: authLatency, capLat: captureLatency, chargeRate: chargebackRate}, nil
}

// Authorize initiates a UPI Collect request.
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
	payerVPA := in.RailSpecific["payer_vpa"]
	if payerVPA == "" {
		c.persist(in, "authorize", rail.StatusFailed, idem, "", rail.NewError(rail.CodeInvalidRequest, "missing payer_vpa"))
		return failResp(rail.CodeInvalidRequest, "missing payer_vpa"), nil
	}
	cr, err := c.client.InitiateCollect(ctx, idem, payerVPA, c.cfg.PayeeVPA, in.Amount, in.Currency, in.RailSpecific["remark"])
	c.log.Info("upi collect initiate", "tx_id", in.PaymentID, "rail", "upi", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "authorize", rail.StatusFailed, idem, "", re)
		return failResp(re.Code, re.Reason), nil
	}
	if cr.ResponseCode != "" && cr.ResponseCode != "00" {
		re := mapResponseCode(cr.ResponseCode)
		if re == nil {
			re = rail.NewError(rail.CodeDoNotHonor, cr.ResponseCode)
		}
		c.persist(in, "authorize", rail.StatusFailed, idem, cr.CollectID, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := mapStatus(cr.Status)
	c.store.Upsert(store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "upi",
		Operation:      "authorize",
		Amount:         in.Amount,
		Currency:       in.Currency,
		Status:         st,
		IdempotencyKey: idem,
		RailRef:        cr.CollectID,
	})
	c.persist(in, "authorize", st, idem, cr.CollectID, nil)
	c.audit.Emit(audit.Event{Type: "rail.authorize", PaymentID: in.PaymentID, Rail: "upi", Operation: "authorize", Status: string(st), Amount: in.Amount})
	return rail.Response{Status: st, RailRef: cr.CollectID}, nil
}

// Capture confirms a UPI Collect by polling the collect status and treating
// CONFIRMED as captured.
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
	cr, err := c.client.GetCollectStatus(ctx, rec.RailRef)
	c.log.Info("upi capture (poll)", "tx_id", in.PaymentID, "rail", "upi", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	if cr.ResponseCode != "" && cr.ResponseCode != "00" {
		re := mapResponseCode(cr.ResponseCode)
		if re == nil {
			re = rail.NewError(rail.CodeDoNotHonor, cr.ResponseCode)
		}
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := mapStatus(cr.Status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "capture", st, idem, rec.RailRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.capture", PaymentID: in.PaymentID, Rail: "upi", Operation: "capture", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: rec.RailRef}, nil
}

// Refund initiates a UPI refund.
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
	rr, err := c.client.Refund(ctx, idem, rec.RailRef, amount, in.Currency)
	c.log.Info("upi refund", "tx_id", in.PaymentID, "rail", "upi", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "refund", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := rail.StatusRefunded
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "refund", st, idem, rr.RefundID, nil)
	c.audit.Emit(audit.Event{Type: "rail.refund", PaymentID: in.PaymentID, Rail: "upi", Operation: "refund", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: rr.RefundID}, nil
}

// GetStatus polls the NPCI for the collect status.
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
	cr, err := c.client.GetCollectStatus(ctx, rec.RailRef)
	if err != nil {
		re := c.mapErr(err)
		return rail.StatusUnknown, re
	}
	st := mapStatus(cr.Status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	return st, nil
}

// Chargeback records a dispute / chargeback from the rail and emits the
// rail.chargeback.received event. It inserts a rail_chargebacks row via the
// store and increments the rail_chargeback_rate metric.
func (c *Connector) Chargeback(ctx context.Context, in rail.Context, amount decimal.Decimal, reasonCode string) (rail.Response, error) {
	if in.PaymentID == "" {
		return failResp(rail.CodeInvalidRequest, "missing payment_id"), nil
	}
	rec, ok := c.store.Get(in.PaymentID)
	if !ok {
		return failResp(rail.CodeInvalidRequest, "unknown payment_id"), nil
	}
	idem := in.IdempotencyKey
	if idem == "" {
		idem = rail.IdempotencyKey(in.PaymentID, "chargeback", 1)
	}
	dr, err := c.client.RecordDispute(ctx, idem, rec.RailRef, reasonCode, amount, in.Currency)
	c.log.Info("upi chargeback (dispute record)", "tx_id", in.PaymentID, "rail", "upi", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "chargeback", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	entry := c.store.AddChargeback(store.ChargebackEntry{
		Rail:       "upi",
		PaymentID:  in.PaymentID,
		Amount:     amount,
		ReasonCode: reasonCode,
	})
	c.store.SetStatus(in.PaymentID, rail.StatusChargeback, "", "")
	c.persist(in, "chargeback", rail.StatusChargeback, idem, rec.RailRef, nil)
	c.chargeRate.Inc()
	c.audit.Emit(audit.Event{
		Type:      "rail.chargeback.received",
		PaymentID: in.PaymentID,
		Rail:      "upi",
		Operation: "chargeback",
		Status:    string(rail.StatusChargeback),
		Amount:    amount,
		Payload:   map[string]any{"chargeback_id": entry.ChargebackID, "dispute_id": dr.DisputeID, "reason_code": reasonCode},
	})
	return rail.Response{Status: rail.StatusChargeback, RailRef: rec.RailRef}, nil
}

func (c *Connector) mapErr(err error) *rail.Error {
	if err == nil {
		return nil
	}
	if ae, ok := err.(*npci.APIError); ok {
		if ae.Status >= 500 {
			return rail.NewError(rail.CodeRailUnavailable, err.Error())
		}
		if ae.Code != "" {
			re := mapResponseCode(ae.Code)
			if re != nil {
				return re
			}
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
		Rail:           "upi",
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
	case "CONFIRMED", "CAPTURED":
		return rail.StatusCaptured
	case "AUTHORIZED", "INITIATED":
		return rail.StatusAuthorized
	case "REFUNDED":
		return rail.StatusRefunded
	case "REJECTED", "DECLINED", "FAILED":
		return rail.StatusFailed
	case "PENDING":
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
