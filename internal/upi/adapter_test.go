package upi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/metrics"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
)

func newTestAdapter(t *testing.T) (*Connector, *httptest.Server, store.Store, *audit.Recorder) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/upi/collect":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"collect_id":"C1","status":"INITIATED","response_code":"00"}`))
		case "/v1/upi/collect/C1":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"collect_id":"C1","status":"CONFIRMED","response_code":"00"}`))
		case "/v1/upi/refunds":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"refund_id":"R1","status":"REFUNDED","response_code":"00"}`))
		case "/v1/upi/disputes":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"dispute_id":"D1","status":"RECORDED","reason_code":"CHARGEBACK"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	s := store.New()
	rec := audit.NewRecorder()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k", PayeeVPA: "merchant@upi"}, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv, s, rec
}

func upiCtx(pid string) rail.Context {
	return rail.Context{
		PaymentID: pid,
		Rail:      "upi",
		Amount:    decimal.NewFromInt(100),
		Currency:  "INR",
		RailSpecific: map[string]string{
			"payer_vpa": "alice@upi",
			"remark":    "test payment",
		},
		IdempotencyKey: "k-" + pid,
	}
}

func TestUPIAuthorizeCollect(t *testing.T) {
	t.Parallel()
	c, srv, s, rec := newTestAdapter(t)
	defer srv.Close()
	resp, err := c.Authorize(context.Background(), upiCtx("pu1"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusAuthorized {
		t.Fatalf("status = %q", resp.Status)
	}
	if resp.RailRef != "C1" {
		t.Fatalf("rail_ref = %q", resp.RailRef)
	}
	r, _ := s.Get("pu1")
	if r.Status != rail.StatusAuthorized || r.IdempotencyKey != "k-pu1" {
		t.Fatalf("store = %+v", r)
	}
	if rec.Count() != 1 {
		t.Fatalf("audit count = %d", rec.Count())
	}
}

func TestUPIAuthorizeMissingPayerVPA(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Authorize(context.Background(), rail.Context{PaymentID: "pu2", Rail: "upi"})
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestUPIAuthorizeMissingPaymentID(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Authorize(context.Background(), rail.Context{Rail: "upi", RailSpecific: map[string]string{"payer_vpa": "p@upi"}})
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestUPICapturePoll(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := upiCtx("pu3")
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Capture(context.Background(), ctx, decimal.NewFromInt(100))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusCaptured {
		t.Fatalf("status = %q", resp.Status)
	}
	r, _ := s.Get("pu3")
	if r.Status != rail.StatusCaptured {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestUPIRefund(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := upiCtx("pu4")
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Refund(context.Background(), ctx, decimal.NewFromInt(50))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusRefunded {
		t.Fatalf("status = %q", resp.Status)
	}
	r, _ := s.Get("pu4")
	if r.Status != rail.StatusRefunded {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestUPIGetStatus(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := upiCtx("pu5")
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	st, err := c.GetStatus(context.Background(), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st != rail.StatusCaptured {
		t.Fatalf("status = %q", st)
	}
}

func TestUPIChargeback(t *testing.T) {
	t.Parallel()
	c, srv, s, rec := newTestAdapter(t)
	defer srv.Close()
	ctx := upiCtx("pu6")
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Capture(context.Background(), ctx, decimal.NewFromInt(100)); err != nil {
		t.Fatal(err)
	}
	before := c.chargeRate.Value()
	resp, err := c.Chargeback(context.Background(), ctx, decimal.NewFromInt(100), "CHARGEBACK")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusChargeback {
		t.Fatalf("status = %q", resp.Status)
	}
	if c.chargeRate.Value() != before+1 {
		t.Fatalf("chargeback rate = %v, want %v", c.chargeRate.Value(), before+1)
	}
	r, _ := s.Get("pu6")
	if r.Status != rail.StatusChargeback {
		t.Fatalf("store = %q", r.Status)
	}
	cbs := s.ChargebacksFor("pu6")
	if len(cbs) != 1 {
		t.Fatalf("chargebacks len = %d", len(cbs))
	}
	if cbs[0].ReasonCode != "CHARGEBACK" {
		t.Fatalf("reason = %q", cbs[0].ReasonCode)
	}
	events := rec.Events()
	found := false
	for _, e := range events {
		if e.Type == "rail.chargeback.received" {
			found = true
			if e.PaymentID != "pu6" {
				t.Fatalf("payment_id = %q", e.PaymentID)
			}
		}
	}
	if !found {
		t.Fatal("rail.chargeback.received event not emitted")
	}
}

func TestUPIAuthorizeResponseCode(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"response_code":"ZD","message":"declined"}`))
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k", PayeeVPA: "m@upi"}, store.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := c.Authorize(context.Background(), upiCtx("pu7"))
	if resp.ErrorCode != rail.CodeDoNotHonor {
		t.Fatalf("error_code = %q", resp.ErrorCode)
	}
}

func TestUPIServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k", PayeeVPA: "m@upi"}, store.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := c.Authorize(context.Background(), upiCtx("pu8"))
	if resp.ErrorCode != rail.CodeRailUnavailable {
		t.Fatalf("got %q", resp.ErrorCode)
	}
}

func TestMapResponseCode(t *testing.T) {
	t.Parallel()
	if MapResponseCode("00") != nil {
		t.Fatal("00 should map to nil")
	}
	if MapResponseCode("ZF").Code != rail.CodeFraudDecline {
		t.Fatal("ZF wrong")
	}
	if MapResponseCode("ZM").Code != rail.CodeInsufficientFunds {
		t.Fatal("ZM wrong")
	}
	if MapResponseCode("ZE").Code != rail.CodeExpiredInstrument {
		t.Fatal("ZE wrong")
	}
	if MapResponseCode("ZI").Code != rail.CodeInvalidRequest {
		t.Fatal("ZI wrong")
	}
}

func TestChargebackRateMetricRegistered(t *testing.T) {
	t.Parallel()
	if metrics.Default.Counter("rail_chargeback_rate") == nil {
		t.Fatal("rail_chargeback_rate metric not registered")
	}
}
