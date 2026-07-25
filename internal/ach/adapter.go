// Package ach implements the RailConnector for ACH payments: NACHA file
// generation, bank partner API submission, status polling, and ACH return
// handling.
package ach

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/ach/bankapi"
	"github.com/ai-crypto-onramp/rail-connector/internal/ach/nacha"
	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/metrics"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
)

// Default bank partner API base URL (overridden by RAIL_ACH_PARTNER_URL).
const DefaultBaseURL = "http://localhost:8080"

// Config configures the ACH adapter.
type Config struct {
	BaseURL         string
	APIKey          string
	ImmediateOrigin string // 10 digits
	ImmediateDest   string // 10 digits
	CompanyID       string // 10 digits
}

// Connector implements rail.Connector for the ACH rail.
type Connector struct {
	cfg     Config
	client  *bankapi.Client
	store   store.Store
	audit   audit.Sink
	log     *slog.Logger
	authLat *metrics.Histogram
	capLat  *metrics.Histogram
}

var (
	authLatency = metrics.Default.RegisterHistogram(
		"rail_authorize_latency", "ach authorize latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
	captureLatency = metrics.Default.RegisterHistogram(
		"rail_capture_latency", "ach capture latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
)

// New constructs an ACH Connector.
func New(cfg Config, s store.Store, as audit.Sink) (*Connector, error) {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = os.Getenv("RAIL_ACH_PARTNER_URL")
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if as == nil {
		as = audit.NewRecorder()
	}
	return &Connector{
		cfg:     cfg,
		client:  bankapi.New(baseURL, cfg.APIKey),
		store:   s,
		audit:   as,
		log:     slog.Default(),
		authLat: authLatency,
		capLat:  captureLatency,
	}, nil
}

// Authorize performs an ACH pre-note: a zero-amount NACHA file submitted to
// validate the receiver's account.
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
	fileBody, err := c.buildFile(in, 0)
	if err != nil {
		c.persist(in, "authorize", rail.StatusFailed, idem, "", rail.NewError(rail.CodeInvalidRequest, err.Error()))
		return failResp(rail.CodeInvalidRequest, err.Error()), nil
	}
	resp, err := c.client.SubmitNACHA(ctx, fileBody, idem)
	c.log.Info("ach authorize (prenote)",
		"tx_id", in.PaymentID, "rail", "ach", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "authorize", rail.StatusFailed, idem, "", re)
		return failResp(re.Code, re.Reason), nil
	}
	if resp.Rejected {
		re := rail.NewError(rail.CodeInvalidRequest, resp.RejectMsg)
		c.persist(in, "authorize", rail.StatusFailed, idem, "", re)
		return failResp(re.Code, re.Reason), nil
	}
	st := rail.StatusAuthorized
	c.store.Upsert(store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "ach",
		Operation:      "authorize",
		Amount:         in.Amount,
		Currency:       in.Currency,
		Status:         st,
		IdempotencyKey: idem,
		RailRef:        resp.BatchID,
	})
	c.persist(in, "authorize", st, idem, resp.BatchID, nil)
	c.audit.Emit(audit.Event{Type: "rail.authorize", PaymentID: in.PaymentID, Rail: "ach", Operation: "authorize", Status: string(st), Amount: in.Amount})
	return rail.Response{Status: st, RailRef: resp.BatchID, RawResponse: fileBody}, nil
}

// Capture submits a NACHA debit batch for the authorized amount.
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
	fileBody, err := c.buildFile(in, toMinor(amount))
	if err != nil {
		return failResp(rail.CodeInvalidRequest, err.Error()), nil
	}
	resp, err := c.client.SubmitNACHA(ctx, fileBody, idem)
	c.log.Info("ach capture (batch submit)",
		"tx_id", in.PaymentID, "rail", "ach", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	if resp.Rejected {
		re := rail.NewError(rail.CodeInvalidRequest, resp.RejectMsg)
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := rail.StatusCaptured
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.store.Upsert(store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "ach",
		Operation:      "capture",
		Amount:         amount,
		Currency:       in.Currency,
		Status:         st,
		IdempotencyKey: idem,
		RailRef:        resp.BatchID,
	})
	c.persist(in, "capture", st, idem, resp.BatchID, nil)
	c.audit.Emit(audit.Event{Type: "rail.capture", PaymentID: in.PaymentID, Rail: "ach", Operation: "capture", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: resp.BatchID}, nil
}

// Refund submits a reversing NACHA entry for the captured amount.
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
	fileBody, err := c.buildFile(in, toMinor(amount))
	if err != nil {
		return failResp(rail.CodeInvalidRequest, err.Error()), nil
	}
	resp, err := c.client.SubmitNACHA(ctx, fileBody, idem)
	c.log.Info("ach refund (reversing entry)",
		"tx_id", in.PaymentID, "rail", "ach", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "refund", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	if resp.Rejected {
		re := rail.NewError(rail.CodeInvalidRequest, resp.RejectMsg)
		c.persist(in, "refund", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := rail.StatusRefunded
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "refund", st, idem, resp.BatchID, nil)
	c.audit.Emit(audit.Event{Type: "rail.refund", PaymentID: in.PaymentID, Rail: "ach", Operation: "refund", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: resp.BatchID}, nil
}

// GetStatus polls the batch status from the bank partner API.
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
	bs, err := c.client.GetBatchStatus(ctx, rec.RailRef)
	if err != nil {
		re := c.mapErr(err)
		return rail.StatusUnknown, re
	}
	if bs.ReturnCode != "" {
		re := mapReturnCode(bs.ReturnCode)
		c.store.SetStatus(in.PaymentID, rail.StatusFailed, re.Code, re.Reason)
		return rail.StatusFailed, re
	}
	st := mapStatus(bs.Status)
	c.store.SetStatus(in.PaymentID, st, "", "")
	return st, nil
}

func (c *Connector) buildFile(in rail.Context, amountMinor int64) ([]byte, error) {
	routing := in.RailSpecific["routing"]
	account := in.RailSpecific["account"]
	receiver := in.RailSpecific["receiver"]
	if receiver == "" {
		receiver = in.PaymentID
	}
	trace := in.RailSpecific["trace"]
	if trace == "" {
		trace = in.PaymentID
	}
	f := nacha.File{
		ImmediateOrigin: orDefault(c.cfg.ImmediateOrigin, "1234567890"),
		ImmediateDest:   orDefault(c.cfg.ImmediateDest, "0987654321"),
		OriginName:      "ONRAMP",
		DestName:        "BANK PARTNER",
		Batches: []nacha.Batch{{
			CompanyID:   orDefault(c.cfg.CompanyID, "ORIGID0001"),
			CompanyName: "ONRAMP",
			Description: "PPD",
			Entries: []nacha.Entry{{
				TraceNumber:  trace,
				DFIAccount:   account,
				Routing:      routing,
				Amount:       amountMinor,
				ReceiverName: receiver,
			}},
		}},
	}
	enc, err := f.Encode()
	if err != nil {
		return nil, err
	}
	return []byte(enc), nil
}

func (c *Connector) mapErr(err error) *rail.Error {
	if err == nil {
		return nil
	}
	if ae, ok := err.(*bankapi.APIError); ok && ae.Status >= 500 {
		return rail.NewError(rail.CodeRailUnavailable, err.Error())
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
		Rail:           "ach",
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
	switch strings.ToLower(s) {
	case "accepted", "submitted", "authorized":
		return rail.StatusAuthorized
	case "settled", "captured", "processed":
		return rail.StatusCaptured
	case "refunded", "reversed":
		return rail.StatusRefunded
	case "returned", "failed", "rejected":
		return rail.StatusFailed
	case "pending":
		return rail.StatusPending
	default:
		return rail.StatusUnknown
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

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func failResp(code, msg string) rail.Response {
	return rail.Response{Status: rail.StatusFailed, ErrorCode: code, ErrorMessage: msg}
}
