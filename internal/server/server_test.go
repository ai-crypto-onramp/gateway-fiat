package server

import (
	"bytes"
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
	"github.com/ai-crypto-onramp/rail-connector/internal/webhooks"
)

func newTestService(t *testing.T) (*Service, *audit.Recorder, store.Store) {
	t.Helper()
	s := store.New()
	rec := audit.NewRecorder()
	svc := New(Config{
		Rail:          "card",
		WebhookSecret: "dev-secret",
		Store:         s,
		AuditSink:     rec,
		Ready:         true,
	})
	return svc, rec, s
}

func doRequest(t *testing.T, svc *Service, method, path string, body any, sig string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	if sig != "" {
		req.Header.Set("X-Webhook-Signature", sig)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	rec := doRequest(t, svc, http.MethodGet, "/healthz", nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status = %q", body["status"])
	}
}

func TestReadyz(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	svc.SetReady(false)
	rec := doRequest(t, svc, http.MethodGet, "/readyz", nil, "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", rec.Code)
	}
	svc.SetReady(true)
	rec = doRequest(t, svc, http.MethodGet, "/readyz", nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestAuthorizeEndpoint(t *testing.T) {
	t.Parallel()
	svc, rec, s := newTestService(t)
	body := authorizeReq{PaymentID: "p1", Rail: "card", Amount: decimal.NewFromFloat(12.5), Currency: "USD"}
	resp := doRequest(t, svc, http.MethodPost, "/v1/authorize", body, "")
	if resp.Code != http.StatusOK {
		t.Fatalf("code = %d body=%s", resp.Code, resp.Body.String())
	}
	var r rail.Response
	if err := json.Unmarshal(resp.Body.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if r.Status != rail.StatusAuthorized || r.RailRef == "" {
		t.Fatalf("resp = %+v", r)
	}
	if rec.Count() != 1 {
		t.Fatalf("audit count = %d", rec.Count())
	}
	r2, ok := s.Get("p1")
	if !ok || r2.Status != rail.StatusAuthorized {
		t.Fatalf("store = %+v", r2)
	}
}

func TestAuthorizeMissingPaymentID(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	resp := doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{Rail: "card"}, "")
	if resp.Code != http.StatusUnprocessableEntity {
		t.Fatalf("code = %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestAuthorizeBadJSON(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/authorize", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestAuthorizeFailFlag(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	body := authorizeReq{PaymentID: "p2", Rail: "card", Fail: true}
	resp := doRequest(t, svc, http.MethodPost, "/v1/authorize", body, "")
	if resp.Code != http.StatusUnprocessableEntity {
		t.Fatalf("code = %d body=%s", resp.Code, resp.Body.String())
	}
}

func TestCaptureAndRefundFlow(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{PaymentID: "p3", Amount: decimal.NewFromInt(50)}, "")
	resp := doRequest(t, svc, http.MethodPost, "/v1/capture/p3", amountReq{Amount: decimal.NewFromInt(50)}, "")
	if resp.Code != http.StatusOK {
		t.Fatalf("capture code = %d body=%s", resp.Code, resp.Body.String())
	}
	var c rail.Response
	json.Unmarshal(resp.Body.Bytes(), &c)
	if c.Status != rail.StatusCaptured {
		t.Fatalf("status = %q", c.Status)
	}
	resp = doRequest(t, svc, http.MethodPost, "/v1/refund/p3", amountReq{Amount: decimal.NewFromInt(20)}, "")
	if resp.Code != http.StatusOK {
		t.Fatalf("refund code = %d body=%s", resp.Code, resp.Body.String())
	}
	var r rail.Response
	json.Unmarshal(resp.Body.Bytes(), &r)
	if r.Status != rail.StatusRefunded {
		t.Fatalf("status = %q", r.Status)
	}
}

func TestCaptureUnknownPayment(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	resp := doRequest(t, svc, http.MethodPost, "/v1/capture/ghost", amountReq{Amount: decimal.NewFromInt(1)}, "")
	if resp.Code != http.StatusNotFound {
		t.Fatalf("code = %d", resp.Code)
	}
}

func TestStatusEndpoint(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{PaymentID: "p4", Amount: decimal.NewFromInt(1)}, "")
	resp := doRequest(t, svc, http.MethodGet, "/v1/status/p4", nil, "")
	if resp.Code != http.StatusOK {
		t.Fatalf("code = %d body=%s", resp.Code, resp.Body.String())
	}
	var sr rail.StatusResponse
	json.Unmarshal(resp.Body.Bytes(), &sr)
	if sr.Status != rail.StatusAuthorized {
		t.Fatalf("status = %q", sr.Status)
	}
}

func TestStatusUnknownPayment(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	resp := doRequest(t, svc, http.MethodGet, "/v1/status/missing", nil, "")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", resp.Code)
	}
}

func TestWebhookHappy(t *testing.T) {
	t.Parallel()
	svc, _, s := newTestService(t)
	doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{PaymentID: "p5", Amount: decimal.NewFromInt(1)}, "")
	// Use a recognized status so the webhook updates the store.
	body := []byte(`{"payment_id":"p5","status":"settled"}`)
	sig := webhooks.Compute(body, "dev-secret")
	req := httptest.NewRequest(http.MethodPost, "/webhooks/card", bytes.NewReader(body))
	req.Header.Set("X-Webhook-Signature", sig)
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d body=%s", rec.Code, rec.Body.String())
	}
	r, _ := s.Get("p5")
	if r.Status != rail.StatusSettled {
		t.Fatalf("store status = %q", r.Status)
	}
}

func TestWebhookBadSignature(t *testing.T) {
	t.Parallel()
	svc, _, s := newTestService(t)
	doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{PaymentID: "p6", Amount: decimal.NewFromInt(1)}, "")
	body := []byte(`{"payment_id":"p6","status":"settled"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/card", bytes.NewReader(body))
	req.Header.Set("X-Webhook-Signature", "deadbeef")
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d", rec.Code)
	}
	r, _ := s.Get("p6")
	if r.Status == rail.StatusSettled {
		t.Fatal("store should not have been updated")
	}
}

func TestWebhookMissingSignature(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	body := []byte(`{"payment_id":"x","status":"settled"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/card", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestWebhookUnknownStatus(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	body := []byte(`{"payment_id":"p7","status":"weird"}`)
	sig := webhooks.Compute(body, "dev-secret")
	req := httptest.NewRequest(http.MethodPost, "/webhooks/card", bytes.NewReader(body))
	req.Header.Set("X-Webhook-Signature", sig)
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestWebhookInvalidJSON(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	body := []byte(`not-json`)
	sig := webhooks.Compute(body, "dev-secret")
	req := httptest.NewRequest(http.MethodPost, "/webhooks/card", bytes.NewReader(body))
	req.Header.Set("X-Webhook-Signature", sig)
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestWebhookMissingRail(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	body := []byte(`{}`)
	sig := webhooks.Compute(body, "dev-secret")
	req := httptest.NewRequest(http.MethodPost, "/webhooks/", bytes.NewReader(body))
	req.Header.Set("X-Webhook-Signature", sig)
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestWebhookNoPaymentIDNoOp(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	body := []byte(`{"some_other":"event"}`)
	sig := webhooks.Compute(body, "dev-secret")
	req := httptest.NewRequest(http.MethodPost, "/webhooks/card", bytes.NewReader(body))
	req.Header.Set("X-Webhook-Signature", sig)
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestIntegrationServerRoundTrip(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	srv := httptest.NewServer(svc.Mux())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/authorize", "application/json", strings.NewReader(`{"payment_id":"p8","amount":99,"currency":"USD"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authorize status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Post(srv.URL+"/v1/capture/p8", "application/json", strings.NewReader(`{"amount":99}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("capture status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(srv.URL + "/v1/status/p8")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", resp.StatusCode)
	}
}

func TestRefundUnknownPayment(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	resp := doRequest(t, svc, http.MethodPost, "/v1/refund/ghost", amountReq{Amount: decimal.NewFromInt(1)}, "")
	if resp.Code != http.StatusNotFound {
		t.Fatalf("code = %d", resp.Code)
	}
}

func TestRefundBadJSON(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{PaymentID: "p9", Amount: decimal.NewFromInt(1)}, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/refund/p9", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestCaptureBadJSON(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{PaymentID: "p10", Amount: decimal.NewFromInt(1)}, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/capture/p10", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestCaptureMissingPaymentIDInPath(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	resp := doRequest(t, svc, http.MethodPost, "/v1/capture/", amountReq{Amount: decimal.NewFromInt(1)}, "")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", resp.Code)
	}
}

func TestRefundMissingPaymentIDInPath(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	resp := doRequest(t, svc, http.MethodPost, "/v1/refund/", amountReq{Amount: decimal.NewFromInt(1)}, "")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", resp.Code)
	}
}

func TestStatusMissingPaymentIDInPath(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	resp := doRequest(t, svc, http.MethodGet, "/v1/status/", nil, "")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", resp.Code)
	}
}

func TestCaptureEmptyBody(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	doRequest(t, svc, http.MethodPost, "/v1/authorize", authorizeReq{PaymentID: "p11", Amount: decimal.NewFromInt(1)}, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/capture/p11", nil)
	rec := httptest.NewRecorder()
	svc.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("empty body capture code = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNewDefaults(t *testing.T) {
	t.Parallel()
	svc := New(Config{})
	if svc == nil {
		t.Fatal("nil service")
	}
	if svc.cfg.WebhookSecret != DefaultWebhookSecret {
		t.Fatalf("default secret wrong: %q", svc.cfg.WebhookSecret)
	}
	if svc.cfg.Rail != "dummy" {
		t.Fatalf("default rail wrong: %q", svc.cfg.Rail)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()
	svc, _, _ := newTestService(t)
	rec := doRequest(t, svc, http.MethodGet, "/metrics", nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "# TYPE") {
		t.Fatalf("missing TYPE in metrics output: %s", body)
	}
	if !strings.Contains(body, "rail_") {
		t.Fatalf("missing rail_ metrics: %s", body)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}
