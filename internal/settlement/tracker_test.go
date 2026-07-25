package settlement

import (
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
)

func TestTrackerTotals(t *testing.T) {
	t.Parallel()
	s := store.New()
	tr := New(s)
	tr.RecordCapture("card", decimal.NewFromInt(100))
	tr.RecordCapture("ach", decimal.NewFromInt(50))
	tr.RecordRefund("card", decimal.NewFromInt(30))
	if tr.Total("card").Cmp(decimal.NewFromInt(70)) != 0 {
		t.Fatalf("card total = %v", tr.Total("card"))
	}
	if tr.Total("ach").Cmp(decimal.NewFromInt(50)) != 0 {
		t.Fatalf("ach total = %v", tr.Total("ach"))
	}
	if tr.Totals()["card"].Cmp(decimal.NewFromInt(70)) != 0 {
		t.Fatalf("totals card = %v", tr.Totals()["card"])
	}
}

func TestRecordSettleUpdatesStore(t *testing.T) {
	t.Parallel()
	s := store.New()
	tr := New(s)
	s.Upsert(store.Record{PaymentID: "p1", Rail: "card", Status: "captured"})
	tr.RecordSettle(store.SettleEntry{Rail: "card", PaymentID: "p1", Amount: decimal.NewFromInt(25)})
	if s.SettledAmount("p1").Cmp(decimal.NewFromInt(25)) != 0 {
		t.Fatalf("settled = %v", s.SettledAmount("p1"))
	}
	if tr.Total("card").Cmp(decimal.NewFromInt(25)) != 0 {
		t.Fatalf("card total = %v", tr.Total("card"))
	}
}
