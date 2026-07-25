// Package server wires the rail connector, store, settlement tracker, audit
// sink, and webhook verifier into a single HTTP service with a REST API.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/audit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/dummy"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/metrics"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/settlement"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/webhooks"
)

// DefaultWebhookSecret is the fallback HMAC secret when WEBHOOK_SECRET is unset.
const DefaultWebhookSecret = "dev-secret"

// Config holds service configuration.
type Config struct {
	Addr          string
	WebhookSecret string
	Rail          string
	AuditSink     audit.Sink
	Store         store.Store
	Tracker       *settlement.Tracker
	Connector     rail.Connector
	Ready         bool // readyz toggles on/off
}

// Service bundles the dependencies the HTTP handlers use.
type Service struct {
	cfg     Config
	conn    rail.Connector
	store   store.Store
	tracker *settlement.Tracker
	audit   audit.Sink
	ready   bool
	now     func() time.Time
}

// New constructs a Service. If cfg.Connector is nil a DummyRailConnector is
// built around cfg.Store / cfg.Tracker.
func New(cfg Config) *Service {
	if cfg.WebhookSecret == "" {
		cfg.WebhookSecret = DefaultWebhookSecret
	}
	if cfg.Rail == "" {
		cfg.Rail = "dummy"
	}
	s := cfg.Store
	t := cfg.Tracker
	if s == nil {
		s = store.New()
	}
	if t == nil {
		t = settlement.New(s)
	}
	conn := cfg.Connector
	if conn == nil {
		conn = dummy.New(s, t, dummy.Config{Rail: cfg.Rail, AuditSink: cfg.AuditSink})
	}
	as := cfg.AuditSink
	if as == nil {
		as = audit.NewRecorder()
	}
	return &Service{
		cfg:     cfg,
		conn:    conn,
		store:   s,
		tracker: t,
		audit:   as,
		ready:   cfg.Ready,
		now:     time.Now,
	}
}

// Mux returns the HTTP mux with all routes registered.
func (s *Service) Mux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)
	mux.HandleFunc("POST /v1/authorize", s.authorize)
	mux.HandleFunc("POST /v1/capture/", s.capture)
	mux.HandleFunc("POST /v1/refund/", s.refund)
	mux.HandleFunc("GET /v1/status/", s.status)
	mux.HandleFunc("POST /webhooks/", s.webhook)
	mux.HandleFunc("/metrics", s.metrics)
	return mux
}

// SetReady toggles the readyz response (used by tests / lifecycle).
func (s *Service) SetReady(b bool) { s.ready = b }

func (s *Service) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Service) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(metrics.Default.Render()))
}

func (s *Service) readyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// authorizeReq is the JSON body for POST /v1/authorize.
//
// BREAKING: amount is a JSON string (decimal-encoded), not a JSON number.
type authorizeReq struct {
	PaymentID    string            `json:"payment_id"`
	Rail         string            `json:"rail"`
	Amount       decimal.Decimal   `json:"amount"`
	Currency     string            `json:"currency"`
	PayerRef     string            `json:"payer_ref"`
	Attempt      int               `json:"attempt"`
	RailSpecific map[string]string `json:"rail_specific"`
	Fail         bool              `json:"fail"` // debug: force dummy to fail
}

func (s *Service) authorize(w http.ResponseWriter, r *http.Request) {
	var req authorizeReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	railName := req.Rail
	if railName == "" {
		railName = s.cfg.Rail
	}
	if d, ok := s.conn.(*dummy.Connector); ok && req.Fail {
		d.SetFail(true)
	}
	ctx := rail.Context{
		PaymentID:      req.PaymentID,
		Rail:           railName,
		Amount:         req.Amount,
		Currency:       req.Currency,
		PayerRef:       req.PayerRef,
		Attempt:        req.Attempt,
		IdempotencyKey: rail.IdempotencyKey(req.PaymentID, "authorize", req.Attempt),
		RailSpecific:   req.RailSpecific,
	}
	resp, err := s.conn.Authorize(r.Context(), ctx)
	if err != nil {
		writeErr(w, http.StatusBadGateway, rail.AsError(err).Code, err.Error())
		return
	}
	writeJSON(w, statusForResp(resp), resp)
}

// amountReq is the JSON body for POST /v1/capture and /v1/refund.
//
// BREAKING: amount is a JSON string (decimal-encoded), not a JSON number.
type amountReq struct {
	Amount decimal.Decimal `json:"amount"`
}

func (s *Service) capture(w http.ResponseWriter, r *http.Request) {
	pid := strings.TrimPrefix(r.URL.Path, "/v1/capture/")
	if pid == "" {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "missing payment_id")
		return
	}
	var req amountReq
	if err := decodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	rec, ok := s.store.Get(pid)
	if !ok {
		writeErr(w, http.StatusNotFound, "INVALID_REQUEST", "unknown payment_id")
		return
	}
	ctx := rail.Context{
		PaymentID:      pid,
		Rail:           rec.Rail,
		Amount:         rec.Amount,
		Currency:       rec.Currency,
		IdempotencyKey: rail.IdempotencyKey(pid, "capture", 1),
	}
	resp, err := s.conn.Capture(r.Context(), ctx, req.Amount)
	if err != nil {
		writeErr(w, http.StatusBadGateway, rail.AsError(err).Code, err.Error())
		return
	}
	writeJSON(w, statusForResp(resp), resp)
}

func (s *Service) refund(w http.ResponseWriter, r *http.Request) {
	pid := strings.TrimPrefix(r.URL.Path, "/v1/refund/")
	if pid == "" {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "missing payment_id")
		return
	}
	var req amountReq
	if err := decodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	rec, ok := s.store.Get(pid)
	if !ok {
		writeErr(w, http.StatusNotFound, "INVALID_REQUEST", "unknown payment_id")
		return
	}
	ctx := rail.Context{
		PaymentID:      pid,
		Rail:           rec.Rail,
		Amount:         rec.Amount,
		Currency:       rec.Currency,
		IdempotencyKey: rail.IdempotencyKey(pid, "refund", 1),
	}
	resp, err := s.conn.Refund(r.Context(), ctx, req.Amount)
	if err != nil {
		writeErr(w, http.StatusBadGateway, rail.AsError(err).Code, err.Error())
		return
	}
	writeJSON(w, statusForResp(resp), resp)
}

func (s *Service) status(w http.ResponseWriter, r *http.Request) {
	pid := strings.TrimPrefix(r.URL.Path, "/v1/status/")
	if pid == "" {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "missing payment_id")
		return
	}
	st, err := s.conn.GetStatus(context.Background(), rail.Context{PaymentID: pid})
	if err != nil {
		re := rail.AsError(err)
		code := http.StatusBadRequest
		if re.Code == rail.CodeRailUnavailable {
			code = http.StatusBadGateway
		}
		writeErr(w, code, re.Code, re.Reason)
		return
	}
	writeJSON(w, http.StatusOK, rail.StatusResponse{PaymentID: pid, Status: st})
}

type webhookBody struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
	Rail      string `json:"rail"`
}

func (s *Service) webhook(w http.ResponseWriter, r *http.Request) {
	railName := strings.TrimPrefix(r.URL.Path, "/webhooks/")
	if railName == "" {
		writeErr(w, http.StatusNotFound, "INVALID_REQUEST", "missing rail")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	sig := r.Header.Get("X-Webhook-Signature")
	if err := webhooks.Verify(body, sig, s.cfg.WebhookSecret); err != nil {
		writeErr(w, http.StatusUnauthorized, "INVALID_REQUEST", err.Error())
		return
	}
	var wb webhookBody
	if err := json.Unmarshal(body, &wb); err != nil {
		writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid json body")
		return
	}
	if wb.PaymentID != "" && wb.Status != "" {
		st := rail.Status(wb.Status)
		if !rail.ValidStatus(st) {
			writeErr(w, http.StatusBadRequest, "INVALID_REQUEST", "unknown status")
			return
		}
		s.store.SetStatus(wb.PaymentID, st, "", "")
		s.audit.Emit(audit.Event{
			Type:      "rail.webhook",
			PaymentID: wb.PaymentID,
			Rail:      railName,
			Operation: "webhook",
			Status:    string(st),
		})
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "received"})
}

func statusForResp(r rail.Response) int {
	if r.Status == rail.StatusFailed && r.ErrorCode != "" {
		return http.StatusUnprocessableEntity
	}
	return http.StatusOK
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]string{
		"status":        "failed",
		"error_code":    errCode,
		"error_message": msg,
	})
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return io.EOF
	}
	return json.Unmarshal(body, v)
}
