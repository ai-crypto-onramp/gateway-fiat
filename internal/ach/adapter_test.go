package ach

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
)

func newTestAdapter(t *testing.T) (*Connector, *httptest.Server, store.Store, *audit.Recorder) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/ach/batches":
			if r.Method == http.MethodPost {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"batch_id":"BATCH1","status":"accepted"}`))
				return
			}
		case "/v1/ach/batches/BATCH1":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"batch_id":"BATCH1","status":"settled"}`))
			return
		case "/v1/ach/batches/RET1":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"batch_id":"RET1","status":"returned","return_code":"R01"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	s := store.New()
	rec := audit.NewRecorder()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k"}, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv, s, rec
}

func achCtx(pid string) rail.Context {
	return rail.Context{
		PaymentID: pid,
		Rail:      "ach",
		Amount:    decimal.NewFromInt(10),
		Currency:  "USD",
		RailSpecific: map[string]string{
			"routing":  "012345678",
			"account":  "acct123",
			"receiver": "Alice",
			"trace":    "1234560001",
		},
		IdempotencyKey: "k-" + pid,
	}
}

func TestACHAuthorizePrenote(t *testing.T) {
	t.Parallel()
	c, srv, s, rec := newTestAdapter(t)
	defer srv.Close()
	resp, err := c.Authorize(context.Background(), achCtx("pa1"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusAuthorized {
		t.Fatalf("status = %q", resp.Status)
	}
	if resp.RailRef != "BATCH1" {
		t.Fatalf("rail_ref = %q", resp.RailRef)
	}
	r, _ := s.Get("pa1")
	if r.Status != rail.StatusAuthorized || r.IdempotencyKey != "k-pa1" {
		t.Fatalf("store = %+v", r)
	}
	if rec.Count() != 1 {
		t.Fatalf("audit count = %d", rec.Count())
	}
}

func TestACHCaptureBatchSubmit(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := achCtx("pa2")
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Capture(context.Background(), ctx, decimal.NewFromInt(10))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusCaptured {
		t.Fatalf("status = %q", resp.Status)
	}
	r, _ := s.Get("pa2")
	if r.Status != rail.StatusCaptured {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestACHRefundReversing(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := achCtx("pa3")
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Capture(context.Background(), ctx, decimal.NewFromInt(10)); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Refund(context.Background(), ctx, decimal.NewFromInt(5))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusRefunded {
		t.Fatalf("status = %q", resp.Status)
	}
	r, _ := s.Get("pa3")
	if r.Status != rail.StatusRefunded {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestACHGetStatusSettled(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := achCtx("pa4")
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
	r, _ := s.Get("pa4")
	if r.Status != rail.StatusCaptured {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestACHGetStatusReturnCode(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := achCtx("pa5")
	// Force the stored rail ref to RET1 so GetStatus fetches a returned batch.
	c.store.Upsert(store.Record{
		PaymentID: "pa5",
		Rail:      "ach",
		Status:    rail.StatusAuthorized,
		RailRef:   "RET1",
	})
	_, err := c.GetStatus(context.Background(), ctx)
	if err == nil {
		t.Fatal("expected error for return")
	}
	re, ok := err.(*rail.Error)
	if !ok || re.Code != rail.CodeInsufficientFunds {
		t.Fatalf("err = %v", err)
	}
	r, _ := s.Get("pa5")
	if r.Status != rail.StatusFailed {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestACHServerErrorMapsUnavailable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k"}, store.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := c.Authorize(context.Background(), achCtx("pa6"))
	if resp.ErrorCode != rail.CodeRailUnavailable {
		t.Fatalf("got %q", resp.ErrorCode)
	}
}

func TestACHAuthorizeMissingPaymentID(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Authorize(context.Background(), rail.Context{Rail: "ach"})
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestACHCaptureUnknownPayment(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Capture(context.Background(), rail.Context{PaymentID: "ghost"}, decimal.NewFromInt(1))
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestACHRawResponseIsNACHA(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, err := c.Authorize(context.Background(), achCtx("pa7"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(resp.RawResponse), "1") {
		t.Fatalf("raw response not a NACHA file: %q", string(resp.RawResponse)[:10])
	}
}

func TestMapReturnCode(t *testing.T) {
	t.Parallel()
	if MapReturnCode("R01").Code != rail.CodeInsufficientFunds {
		t.Fatal("R01 wrong")
	}
	if MapReturnCode("R02").Code != rail.CodeExpiredInstrument {
		t.Fatal("R02 wrong")
	}
	if MapReturnCode("R10").Code != rail.CodeFraudDecline {
		t.Fatal("R10 wrong")
	}
	if MapReturnCode("R99").Code != rail.CodeDoNotHonor {
		t.Fatal("R99 wrong")
	}
}
