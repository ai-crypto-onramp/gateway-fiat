package store

import (
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
)

func TestUpsertAndGet(t *testing.T) {
	t.Parallel()
	s := New()
	r := Record{PaymentID: "p1", Rail: "card", Status: rail.StatusAuthorized}
	s.Upsert(r)
	got, ok := s.Get("p1")
	if !ok {
		t.Fatal("expected record")
	}
	if got.Status != rail.StatusAuthorized {
		t.Fatalf("status = %q", got.Status)
	}
	// Update path preserves CreatedAt.
	orig := got.CreatedAt
	r.Status = rail.StatusCaptured
	s.Upsert(r)
	got2, _ := s.Get("p1")
	if got2.Status != rail.StatusCaptured {
		t.Fatalf("status = %q", got2.Status)
	}
	if !got2.CreatedAt.Equal(orig) {
		t.Fatal("CreatedAt changed on update")
	}
	if got2.UpdatedAt.IsZero() {
		t.Fatal("UpdatedAt should be set")
	}
}

func TestSetStatus(t *testing.T) {
	t.Parallel()
	s := New()
	s.Upsert(Record{PaymentID: "p2", Rail: "card", Status: rail.StatusAuthorized})
	if !s.SetStatus("p2", rail.StatusCaptured, "", "") {
		t.Fatal("expected update")
	}
	got, _ := s.Get("p2")
	if got.Status != rail.StatusCaptured {
		t.Fatalf("status = %q", got.Status)
	}
	if s.SetStatus("nope", rail.StatusFailed, "x", "y") {
		t.Fatal("expected false for unknown payment")
	}
}

func TestAddSettle(t *testing.T) {
	t.Parallel()
	s := New()
	s.Upsert(Record{PaymentID: "p3", Rail: "card", Status: rail.StatusCaptured})
	s.AddSettle(SettleEntry{Rail: "card", PaymentID: "p3", Amount: decimal.NewFromInt(10)})
	if s.SettledAmount("p3").Cmp(decimal.NewFromInt(10)) != 0 {
		t.Fatalf("settled = %v", s.SettledAmount("p3"))
	}
	got, _ := s.Get("p3")
	if got.Status != rail.StatusSettled {
		t.Fatalf("status = %q", got.Status)
	}
	if len(s.Settles()) != 1 {
		t.Fatalf("settles len = %d", len(s.Settles()))
	}
}

func TestAll(t *testing.T) {
	t.Parallel()
	s := New()
	s.Upsert(Record{PaymentID: "a"})
	s.Upsert(Record{PaymentID: "b"})
	if len(s.All()) != 2 {
		t.Fatalf("all len = %d", len(s.All()))
	}
}

func TestAddChargeback(t *testing.T) {
	t.Parallel()
	s := New()
	s.Upsert(Record{PaymentID: "p4", Rail: "upi", Status: rail.StatusCaptured})
	e := s.AddChargeback(ChargebackEntry{Rail: "upi", PaymentID: "p4", Amount: decimal.NewFromInt(5), ReasonCode: "ZD"})
	if e.ChargebackID == "" {
		t.Fatal("expected chargeback id assigned")
	}
	if e.ReceivedAt.IsZero() {
		t.Fatal("expected received_at set")
	}
	if e.Status != rail.StatusChargeback {
		t.Fatalf("status = %q", e.Status)
	}
	if len(s.Chargebacks()) != 1 {
		t.Fatalf("chargebacks len = %d", len(s.Chargebacks()))
	}
	got, _ := s.Get("p4")
	if got.Status != rail.StatusChargeback {
		t.Fatalf("request status = %q", got.Status)
	}
	if len(s.ChargebacksFor("p4")) != 1 {
		t.Fatalf("chargebacks for p4 len = %d", len(s.ChargebacksFor("p4")))
	}
	if len(s.ChargebacksFor("nope")) != 0 {
		t.Fatalf("expected 0 for unknown")
	}
}
