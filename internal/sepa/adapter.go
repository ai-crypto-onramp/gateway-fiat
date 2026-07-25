// Package sepa implements the RailConnector for SEPA Instant payments using
// ISO20022 pain.001 initiation and pain.002 status polling.
package sepa

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
	"github.com/ai-crypto-onramp/rail-connector/internal/sepa/gateway"
	"github.com/ai-crypto-onramp/rail-connector/internal/sepa/iso20022"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
)

// Default gateway base URL.
const DefaultBaseURL = "http://localhost:8080"

// Config configures the SEPA adapter.
type Config struct {
	BaseURL    string
	APIKey     string
	MTLSCert   string
	MTLSKey    string
	DebtorName string
	DebtorIBAN string
}

// Connector implements rail.Connector for the SEPA rail.
type Connector struct {
	cfg     Config
	client  *gateway.Client
	store   store.Store
	audit   audit.Sink
	log     *slog.Logger
	authLat *metrics.Histogram
	capLat  *metrics.Histogram
}

var (
	authLatency = metrics.Default.RegisterHistogram(
		"rail_authorize_latency", "sepa authorize latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
	captureLatency = metrics.Default.RegisterHistogram(
		"rail_capture_latency", "sepa capture latency seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	)
)

// New constructs a SEPA Connector.
func New(cfg Config, s store.Store, as audit.Sink) (*Connector, error) {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = os.Getenv("RAIL_SEPA_API_URL")
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("RAIL_SEPA_API_KEY")
	}
	cert := cfg.MTLSCert
	if cert == "" {
		cert = os.Getenv("RAIL_SEPA_MTLS_CERT")
	}
	key := cfg.MTLSKey
	if key == "" {
		key = os.Getenv("RAIL_SEPA_MTLS_KEY")
	}
	cli, err := gateway.New(baseURL, apiKey, cert, key)
	if err != nil {
		return nil, err
	}
	if as == nil {
		as = audit.NewRecorder()
	}
	return &Connector{cfg: cfg, client: cli, store: s, audit: as, log: slog.Default(), authLat: authLatency, capLat: captureLatency}, nil
}

// Authorize submits a pain.001 initiation message to the SEPA gateway.
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
	xml, err := c.buildPain001(in, in.Amount, idem)
	if err != nil {
		c.persist(in, "authorize", rail.StatusFailed, idem, "", rail.NewError(rail.CodeInvalidRequest, err.Error()))
		return failResp(rail.CodeInvalidRequest, err.Error()), nil
	}
	sr, err := c.client.SubmitPain001(ctx, []byte(xml), idem)
	c.log.Info("sepa authorize (pain.001 submit)",
		"tx_id", in.PaymentID, "rail", "sepa", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "authorize", rail.StatusFailed, idem, "", re)
		return failResp(re.Code, re.Reason), nil
	}
	st := rail.StatusAuthorized
	railRef := sr.MsgID
	if railRef == "" {
		railRef = idem
	}
	c.store.Upsert(store.Record{
		PaymentID:      in.PaymentID,
		Rail:           "sepa",
		Operation:      "authorize",
		Amount:         in.Amount,
		Currency:       in.Currency,
		Status:         st,
		IdempotencyKey: idem,
		RailRef:        railRef,
	})
	c.persist(in, "authorize", st, idem, railRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.authorize", PaymentID: in.PaymentID, Rail: "sepa", Operation: "authorize", Status: string(st), Amount: in.Amount})
	return rail.Response{Status: st, RailRef: railRef, RawResponse: []byte(xml)}, nil
}

// Capture confirms the gateway settlement of the submitted pain.001 by
// polling pain.002 and treating an ACCEPTED status as captured.
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
	st, err := c.client.GetPain002(ctx, rec.RailRef)
	c.log.Info("sepa capture (pain.002 poll)",
		"tx_id", in.PaymentID, "rail", "sepa", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	rstatus := mapPain002Status(st.Status, st.Reason)
	if rstatus == rail.StatusFailed && st.Reason != "" {
		re := mapReasonCode(st.Reason)
		c.persist(in, "capture", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	c.store.SetStatus(in.PaymentID, rstatus, "", "")
	c.persist(in, "capture", rstatus, idem, rec.RailRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.capture", PaymentID: in.PaymentID, Rail: "sepa", Operation: "capture", Status: string(rstatus), Amount: amount})
	return rail.Response{Status: rstatus, RailRef: rec.RailRef}, nil
}

// Refund submits a reverse pain.001 message.
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
	xml, err := c.buildPain001(in, amount, idem)
	if err != nil {
		return failResp(rail.CodeInvalidRequest, err.Error()), nil
	}
	sr, err := c.client.SubmitPain001(ctx, []byte(xml), idem)
	c.log.Info("sepa refund (reverse pain.001)",
		"tx_id", in.PaymentID, "rail", "sepa", "rail_request_id", idem, "err", err)
	if err != nil {
		re := c.mapErr(err)
		c.persist(in, "refund", rail.StatusFailed, idem, rec.RailRef, re)
		return failResp(re.Code, re.Reason), nil
	}
	st := rail.StatusRefunded
	railRef := rec.RailRef
	if sr.MsgID != "" {
		railRef = sr.MsgID
	}
	c.store.SetStatus(in.PaymentID, st, "", "")
	c.persist(in, "refund", st, idem, railRef, nil)
	c.audit.Emit(audit.Event{Type: "rail.refund", PaymentID: in.PaymentID, Rail: "sepa", Operation: "refund", Status: string(st), Amount: amount})
	return rail.Response{Status: st, RailRef: railRef}, nil
}

// GetStatus polls pain.002 for the current status.
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
	st, err := c.client.GetPain002(ctx, rec.RailRef)
	if err != nil {
		re := c.mapErr(err)
		return rail.StatusUnknown, re
	}
	rstatus := mapPain002Status(st.Status, st.Reason)
	c.store.SetStatus(in.PaymentID, rstatus, "", "")
	return rstatus, nil
}

func (c *Connector) buildPain001(in rail.Context, amount decimal.Decimal, idem string) (string, error) {
	creditor := in.RailSpecific["creditor_name"]
	creditorIBAN := in.RailSpecific["creditor_iban"]
	if creditorIBAN == "" {
		return "", rail.NewError(rail.CodeInvalidRequest, "missing creditor_iban")
	}
	if creditor == "" {
		creditor = in.PayerRef
	}
	return iso20022.BuildPain001(iso20022.Payment{
		MsgID:         idem,
		Initiator:     c.cfg.DebtorName,
		DebtorName:    orDefault(c.cfg.DebtorName, "ONRAMP"),
		DebtorIBAN:    orDefault(c.cfg.DebtorIBAN, "DE89370400440532013000"),
		ExecutionDate: time.Now().UTC(),
		Currency:      orDefault(strings.ToUpper(in.Currency), "EUR"),
		Transfers: []iso20022.Transfer{{
			EndToEndID:   in.PaymentID,
			CreditorName: creditor,
			CreditorIBAN: creditorIBAN,
			Amount:       amount,
			Reference:    in.RailSpecific["reference"],
		}},
	})
}

func (c *Connector) mapErr(err error) *rail.Error {
	if err == nil {
		return nil
	}
	if ae, ok := err.(*gateway.APIError); ok {
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
		Rail:           "sepa",
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

func mapPain002Status(status, reason string) rail.Status {
	switch strings.ToUpper(status) {
	case "ACSC", "ACSP", "ACCD":
		return rail.StatusCaptured
	case "RJCT":
		return rail.StatusFailed
	case "ACCR", "ACWC":
		return rail.StatusFailed
	case "PDNG", "ACTC":
		return rail.StatusPending
	default:
		if reason != "" {
			return rail.StatusFailed
		}
		return rail.StatusUnknown
	}
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
