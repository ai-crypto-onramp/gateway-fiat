package card

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
)

func newStripeAdapter(t *testing.T) (*Connector, *httptest.Server, store.Store, *audit.Recorder) {
	t.Helper()
	var capturedHeaders http.Header
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		capturedBody, _ = io.ReadAll(r.Body)
		_ = capturedBody
		_ = capturedHeaders
		if r.URL.Path == "/v1/payment_intents" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"pi_123","status":"requires_capture","latest_charge":"ch_123"}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/capture") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"ch_123","status":"succeeded"}`))
			return
		}
		if r.URL.Path == "/v1/refunds" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"re_1","status":"succeeded","charge":"ch_123"}`))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/charges/") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"ch_123","status":"succeeded"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	s := store.New()
	rec := audit.NewRecorder()
	c, err := New(Config{Processor: "stripe", APIKey: "sk_test", BaseURL: srv.URL}, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv, s, rec
}

func TestCardAuthorizeStripeHappy(t *testing.T) {
	t.Parallel()
	c, srv, s, rec := newStripeAdapter(t)
	defer srv.Close()
	ctx := rail.Context{PaymentID: "p1", Rail: "card", Amount: decimal.NewFromFloat(10.0), Currency: "usd", IdempotencyKey: "k1"}
	resp, err := c.Authorize(context.Background(), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusAuthorized {
		t.Fatalf("status = %q", resp.Status)
	}
	if resp.RailRef == "" {
		t.Fatalf("rail_ref = %q", resp.RailRef)
	}
	r, ok := s.Get("p1")
	if !ok || r.Status != rail.StatusAuthorized {
		t.Fatalf("store = %+v", r)
	}
	if r.IdempotencyKey != "k1" {
		t.Fatalf("idem = %q", r.IdempotencyKey)
	}
	if rec.Count() != 1 {
		t.Fatalf("audit count = %d", rec.Count())
	}
}

func TestCardCaptureStripeHappy(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newStripeAdapter(t)
	defer srv.Close()
	ctx := rail.Context{PaymentID: "p2", Rail: "card", Amount: decimal.NewFromFloat(25.0), Currency: "usd", IdempotencyKey: "k2"}
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Capture(context.Background(), ctx, decimal.NewFromFloat(25.0))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusCaptured {
		t.Fatalf("status = %q", resp.Status)
	}
	r, _ := s.Get("p2")
	if r.Status != rail.StatusCaptured {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestCardRefundStripeHappy(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newStripeAdapter(t)
	defer srv.Close()
	ctx := rail.Context{PaymentID: "p3", Rail: "card", Amount: decimal.NewFromFloat(50.0), Currency: "usd", IdempotencyKey: "k3"}
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Capture(context.Background(), ctx, decimal.NewFromFloat(50.0)); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Refund(context.Background(), ctx, decimal.NewFromFloat(20.0))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusRefunded {
		t.Fatalf("status = %q", resp.Status)
	}
}

func TestCardGetStatusStripe(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newStripeAdapter(t)
	defer srv.Close()
	ctx := rail.Context{PaymentID: "p4", Rail: "card", Amount: decimal.NewFromFloat(1.0), Currency: "usd", IdempotencyKey: "k4"}
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

func TestCardAuthorizeMissingPaymentID(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newStripeAdapter(t)
	defer srv.Close()
	resp, _ := c.Authorize(context.Background(), rail.Context{Rail: "card"})
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("expected invalid, got %+v", resp)
	}
}

func TestCardCaptureUnknownPayment(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newStripeAdapter(t)
	defer srv.Close()
	resp, _ := c.Capture(context.Background(), rail.Context{PaymentID: "ghost"}, decimal.NewFromInt(1))
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("expected invalid, got %+v", resp)
	}
}

func newStripeDeclineAdapter(t *testing.T, decline string) (*Connector, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		body, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"type":         "card_error",
				"code":         "card_declined",
				"decline_code": decline,
				"message":      "declined",
			},
		})
		_, _ = w.Write(body)
	}))
	s := store.New()
	c, err := New(Config{Processor: "stripe", APIKey: "sk_test", BaseURL: srv.URL}, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv
}

func TestCardDeclineMapping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		decline, wantCode string
	}{
		{"insufficient_funds", rail.CodeInsufficientFunds},
		{"expired_card", rail.CodeExpiredInstrument},
		{"fraudulent", rail.CodeFraudDecline},
		{"do_not_honor", rail.CodeDoNotHonor},
	} {
		c, srv := newStripeDeclineAdapter(t, tc.decline)
		resp, _ := c.Authorize(context.Background(), rail.Context{PaymentID: "p9", Rail: "card", Amount: decimal.NewFromInt(1), Currency: "usd", IdempotencyKey: "k"})
		if resp.ErrorCode != tc.wantCode {
			t.Fatalf("decline %q: got %q want %q", tc.decline, resp.ErrorCode, tc.wantCode)
		}
		srv.Close()
	}
}

func TestCardServerErrorMapsUnavailable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s := store.New()
	c, err := New(Config{Processor: "stripe", APIKey: "sk_test", BaseURL: srv.URL}, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := c.Authorize(context.Background(), rail.Context{PaymentID: "p10", Rail: "card", Amount: decimal.NewFromInt(1), Currency: "usd", IdempotencyKey: "k"})
	if resp.ErrorCode != rail.CodeRailUnavailable {
		t.Fatalf("got %q", resp.ErrorCode)
	}
}

func newAdyenAdapter(t *testing.T) (*Connector, *httptest.Server, store.Store) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/payments":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"pspReference":"PSP_REF_1","resultCode":"Authorised"}`))
		case "/payments/PSP_REF_1/captures":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"pspReference":"PSP_REF_1","status":"received"}`))
		case "/payments/PSP_REF_1/refunds":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"pspReference":"PSP_REF_1","status":"received"}`))
		case "/payments/PSP_REF_1":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"pspReference":"PSP_REF_1","resultCode":"Settled"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	s := store.New()
	c, err := New(Config{Processor: "adyen", APIKey: "adyen_key", Merchant: "TestMerch", BaseURL: srv.URL}, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv, s
}

func TestCardAdyenAuthorizeHappy(t *testing.T) {
	t.Parallel()
	c, srv, s := newAdyenAdapter(t)
	defer srv.Close()
	resp, err := c.Authorize(context.Background(), rail.Context{PaymentID: "pa1", Rail: "card", Amount: decimal.NewFromFloat(15.0), Currency: "eur", IdempotencyKey: "ka"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusAuthorized {
		t.Fatalf("status = %q", resp.Status)
	}
	if resp.RailRef != "PSP_REF_1" {
		t.Fatalf("rail_ref = %q", resp.RailRef)
	}
	r, _ := s.Get("pa1")
	if r.Status != rail.StatusAuthorized {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestCardAdyenCaptureRefund(t *testing.T) {
	t.Parallel()
	c, srv, _ := newAdyenAdapter(t)
	defer srv.Close()
	ctx := rail.Context{PaymentID: "pa2", Rail: "card", Amount: decimal.NewFromFloat(30.0), Currency: "eur", IdempotencyKey: "ka2"}
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Capture(context.Background(), ctx, decimal.NewFromFloat(30.0))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusPending && resp.Status != rail.StatusCaptured {
		t.Fatalf("capture status = %q", resp.Status)
	}
}

func TestCardUnknownProcessor(t *testing.T) {
	t.Parallel()
	_, err := New(Config{Processor: "weird"}, store.New(), nil)
	if err != ErrProcessor {
		t.Fatalf("expected ErrProcessor, got %v", err)
	}
}

func TestMapDecline(t *testing.T) {
	t.Parallel()
	if MapDecline("stripe", "insufficient_funds").Code != rail.CodeInsufficientFunds {
		t.Fatal("stripe map wrong")
	}
	if MapDecline("adyen", "Expired Card").Code != rail.CodeExpiredInstrument {
		t.Fatal("adyen map wrong")
	}
	if MapDecline("nope", "x").Code != rail.CodeDoNotHonor {
		t.Fatal("default map wrong")
	}
}
