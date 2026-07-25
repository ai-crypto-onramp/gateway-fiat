package sepa

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/audit"
	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/sepa/iso20022"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
)

func newTestAdapter(t *testing.T) (*Connector, *httptest.Server, store.Store, *audit.Recorder) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sepa/pain.001":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.001.001.09"><CstmrCdtTrfInitn><GrpHdr><MsgId>MSG1</MsgId></GrpHdr></CstmrCdtTrfInitn></Document>`))
		case "/v1/sepa/pain.002/MSG1":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0"?><Document><CstmrPmtStsRpt><OrgnlPmtInf><TxInf><OrgnlEndToEndId>MSG1</OrgnlEndToEndId><TxSts>ACSC</TxSts></TxInf></OrgnlPmtInf></CstmrPmtStsRpt></Document>`))
		case "/v1/sepa/pain.002/RJCT1":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0"?><Document><CstmrPmtStsRpt><OrgnlPmtInf><TxInf><OrgnlEndToEndId>RJCT1</OrgnlEndToEndId><TxSts>RJCT</TxSts><StsRsnInf><Rsn><Cd>AM04</Cd></Rsn></StsRsnInf></TxInf></OrgnlPmtInf></CstmrPmtStsRpt></Document>`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	s := store.New()
	rec := audit.NewRecorder()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k", DebtorName: "ONRAMP", DebtorIBAN: "DE89370400440532013000"}, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv, s, rec
}

func sepaCtx(pid string) rail.Context {
	return rail.Context{
		PaymentID: pid,
		Rail:      "sepa",
		Amount:    decimal.NewFromInt(100),
		Currency:  "EUR",
		RailSpecific: map[string]string{
			"creditor_name": "Bob",
			"creditor_iban": "FR1420041010050500013M02606",
			"reference":     "REF-1",
		},
		IdempotencyKey: "k-" + pid,
	}
}

func TestSEPAAuthorizePain001(t *testing.T) {
	t.Parallel()
	c, srv, s, rec := newTestAdapter(t)
	defer srv.Close()
	resp, err := c.Authorize(context.Background(), sepaCtx("ps1"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusAuthorized {
		t.Fatalf("status = %q", resp.Status)
	}
	if resp.RailRef != "MSG1" {
		t.Fatalf("rail_ref = %q", resp.RailRef)
	}
	r, _ := s.Get("ps1")
	if r.Status != rail.StatusAuthorized || r.IdempotencyKey != "k-ps1" {
		t.Fatalf("store = %+v", r)
	}
	if rec.Count() != 1 {
		t.Fatalf("audit count = %d", rec.Count())
	}
	if err := iso20022.Validate(string(resp.RawResponse)); err != nil {
		t.Fatalf("raw response not valid pain.001: %v", err)
	}
}

func TestSEPACapturePain002(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := sepaCtx("ps2")
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
	r, _ := s.Get("ps2")
	if r.Status != rail.StatusCaptured {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestSEPARefundReverse(t *testing.T) {
	t.Parallel()
	c, srv, s, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := sepaCtx("ps3")
	if _, err := c.Authorize(context.Background(), ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Capture(context.Background(), ctx, decimal.NewFromInt(100)); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Refund(context.Background(), ctx, decimal.NewFromInt(50))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusRefunded {
		t.Fatalf("status = %q", resp.Status)
	}
	r, _ := s.Get("ps3")
	if r.Status != rail.StatusRefunded {
		t.Fatalf("store = %q", r.Status)
	}
}

func TestSEPARejectReason(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	c.store.Upsert(store.Record{PaymentID: "ps4", Rail: "sepa", Status: rail.StatusAuthorized, RailRef: "RJCT1"})
	resp, err := c.Capture(context.Background(), rail.Context{PaymentID: "ps4", Currency: "EUR"}, decimal.NewFromInt(100))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != rail.StatusFailed {
		t.Fatalf("status = %q", resp.Status)
	}
	if resp.ErrorCode != rail.CodeInsufficientFunds {
		t.Fatalf("error_code = %q", resp.ErrorCode)
	}
}

func TestSEPAGetStatus(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	ctx := sepaCtx("ps5")
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

func TestSEPAServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, APIKey: "k"}, store.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := c.Authorize(context.Background(), sepaCtx("ps6"))
	if resp.ErrorCode != rail.CodeRailUnavailable {
		t.Fatalf("got %q", resp.ErrorCode)
	}
}

func TestSEPAAuthorizeMissingPaymentID(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Authorize(context.Background(), rail.Context{Rail: "sepa"})
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestSEPACaptureUnknownPayment(t *testing.T) {
	t.Parallel()
	c, srv, _, _ := newTestAdapter(t)
	defer srv.Close()
	resp, _ := c.Capture(context.Background(), rail.Context{PaymentID: "ghost"}, decimal.NewFromInt(1))
	if resp.ErrorCode != rail.CodeInvalidRequest {
		t.Fatalf("got %+v", resp)
	}
}

func TestMapReasonCode(t *testing.T) {
	t.Parallel()
	if MapReasonCode("AM04").Code != rail.CodeInsufficientFunds {
		t.Fatal("AM04 wrong")
	}
	if MapReasonCode("AC01").Code != rail.CodeInvalidRequest {
		t.Fatal("AC01 wrong")
	}
	if MapReasonCode("AC04").Code != rail.CodeExpiredInstrument {
		t.Fatal("AC04 wrong")
	}
	if MapReasonCode("AG01").Code != rail.CodeFraudDecline {
		t.Fatal("AG01 wrong")
	}
	if MapReasonCode("NOAS").Code != rail.CodeDoNotHonor {
		t.Fatal("NOAS wrong")
	}
}
