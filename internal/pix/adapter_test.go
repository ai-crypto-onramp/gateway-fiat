package pix

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/audit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
)

func newTestAdapter(t *testing.T) (*Connector, *httptest.Server, store.Store, *audit.Recorder) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/pix/dict/alice@example.com":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"key":"alice@example.com","account":"acct1","bank_code":"001","owner_name":"Alice","document":"123"}`))
		case "/v1/pix/payments":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"payment_id":"P1","status":"CONFIRMED"}`))
		case "/v1/pix/payments/P1":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"payment_id":"P1","status":"CONFIRMED"}`))
		case "/v1/pix/refunds":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"payment_id":"R1","status":"REFUNDED"}`))
		case "/v1/pix/payments/REJECTED":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"return_code":"BE01","message":"no balance"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	s := store.New()
	rec := audit.NewRecorder()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k"}, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv, s, rec
}

func pixCtx(pid string) rail.Context {
	return rail.Context{
		PaymentID: pid,
		Rail:      "pix",
		Amount:    decimal.NewFromInt(100),
		Currency:  "BRL",
		PayerRef:  "12345678901",
		RailSpecific: map[string]string{
			"pix_key": "alice@example.com",
		},
		IdempotencyKey: "k-" + pid,
	}
}

func TestPIXAuthorizeDICTAndPayment(t *testing.T) {
	t.Parallel()
	c, srv, s, rec := newTestAdapter(t)
	defer srv.Close()
	resp, err := c.Authorize(context.Background(), pixCtx("pp1"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusCaptured {
		t.Fatalf("status = %q", resp.Status)
	}
	if resp.RailRef != "P1" {
		t.Fatalf("rail_ref = %q", resp.RailRef)
	}
	r, _ := s.Get("pp1")
	if r.Status != rail.StatusCaptured || r.IdempotencyKey != "k-pp1" {
		t.Fatalf("store = %+v", r)
	}
	if rec.Count() != 1 {
		t.Fatalf("audit count = %d", rec.Count())
	}
}

func TestPIXAuthorizeMissingPixKey(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Authorize(context.Background(), rail.Context{PaymentID: "pp2", Rail: "pix"})
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestPIXAuthorizeMissingPaymentID(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Authorize(context.Background(), rail.Context{Rail: "pix", RailSpecific: map[string]string{"pix_key": "k"}})
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestPIXCapturePoll(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := pixCtx("pp3")
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
	r, _ := s.Get("pp3")
	if r.Status != rail.StatusCaptured {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestPIXRefund(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := pixCtx("pp4")
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
	r, _ := s.Get("pp4")
	if r.Status != rail.StatusRefunded {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestPIXGetStatus(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := pixCtx("pp5")
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

func TestPIXAuthorizeReturnCode(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/pix/dict/bad@example.com":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"account":"acct","bank_code":"001"}`))
		case "/v1/pix/payments":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"return_code":"BE01","message":"no balance"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k"}, store.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := rail.Context{
		PaymentID: "pp6",
		Rail:      "pix",
		Amount:    decimal.NewFromInt(100),
		Currency:  "BRL",
		RailSpecific: map[string]string{
			"pix_key": "bad@example.com",
		},
		IdempotencyKey: "k-pp6",
	}
	resp, _ := c.Authorize(context.Background(), ctx)
	if resp.ErrorCode != rail.CodeInsufficientFunds {
		t.Fatalf("error_code = %q", resp.ErrorCode)
	}
}

func TestPIXServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k"}, store.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := c.Authorize(context.Background(), pixCtx("pp7"))
	if resp.ErrorCode != rail.CodeRailUnavailable {
		t.Fatalf("got %q", resp.ErrorCode)
	}
}

func TestMapReasonCode(t *testing.T) {
	t.Parallel()
	if MapReasonCode("BE01").Code != rail.CodeInsufficientFunds {
		t.Fatal("BE01 wrong")
	}
	if MapReasonCode("PI02").Code != rail.CodeFraudDecline {
		t.Fatal("PI02 wrong")
	}
	if MapReasonCode("PI03").Code != rail.CodeExpiredInstrument {
		t.Fatal("PI03 wrong")
	}
	if MapReasonCode("PI01").Code != rail.CodeInvalidRequest {
		t.Fatal("PI01 wrong")
	}
}
