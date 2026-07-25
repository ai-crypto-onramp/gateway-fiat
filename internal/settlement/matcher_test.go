package settlement

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/audit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
)

func TestMatchEntriesMatched(t *testing.T) {
	t.Parallel()
	s := store.New()
	rec := audit.NewRecorder()
	s.Upsert(store.Record{PaymentID: "p1", Rail: "card", Status: rail.StatusCaptured})
	entries := []SettledEntry{
		{PaymentID: "p1", Rail: "card", Amount: decimal.NewFromInt(100), Currency: "USD", SettledAt: time.Now(), SourceRef: "file.csv"},
	}
	results := MatchEntries(s, rec, entries)
	if len(results) != 1 || !results[0].Matched {
		t.Fatalf("results = %+v", results)
	}
	if results[0].Error != nil {
		t.Fatalf("unexpected error: %v", results[0].Error)
	}
	if s.SettledAmount("p1").Cmp(decimal.NewFromInt(100)) != 0 {
		t.Fatalf("settled = %v", s.SettledAmount("p1"))
	}
	r, _ := s.Get("p1")
	if r.Status != rail.StatusSettled {
		t.Fatalf("status = %q", r.Status)
	}
	found := false
	for _, e := range rec.Events() {
		if e.Type == "rail.settlement.completed" && e.PaymentID == "p1" {
			found = true
		}
	}
	if !found {
		t.Fatal("rail.settlement.completed not emitted")
	}
}

func TestMatchEntriesUnmatchedBreak(t *testing.T) {
	t.Parallel()
	s := store.New()
	rec := audit.NewRecorder()
	entries := []SettledEntry{
		{PaymentID: "ghost", Rail: "ach", Amount: decimal.NewFromInt(50), Currency: "USD", SettledAt: time.Now(), SourceRef: "file.csv"},
	}
	results := MatchEntries(s, rec, entries)
	if len(results) != 1 {
		t.Fatalf("len = %d", len(results))
	}
	if results[0].Matched {
		t.Fatal("should not be matched")
	}
	if results[0].Error == nil || results[0].Error.Code != rail.CodeSettlementBreak {
		t.Fatalf("error = %v", results[0].Error)
	}
	if !s.SettledAmount("ghost").IsZero() {
		t.Fatal("should not settle unmatched")
	}
	found := false
	for _, e := range rec.Events() {
		if e.Type == "rail.settlement.break" && e.PaymentID == "ghost" {
			found = true
		}
	}
	if !found {
		t.Fatal("rail.settlement.break not emitted")
	}
}

func TestMatchEntriesMixed(t *testing.T) {
	t.Parallel()
	s := store.New()
	rec := audit.NewRecorder()
	s.Upsert(store.Record{PaymentID: "p1", Rail: "sepa", Status: rail.StatusCaptured})
	s.Upsert(store.Record{PaymentID: "p2", Rail: "sepa", Status: rail.StatusCaptured})
	entries := []SettledEntry{
		{PaymentID: "p1", Rail: "sepa", Amount: decimal.NewFromInt(100), Currency: "EUR", SourceRef: "stmt.xml"},
		{PaymentID: "missing", Rail: "sepa", Amount: decimal.NewFromInt(200), Currency: "EUR", SourceRef: "stmt.xml"},
		{PaymentID: "p2", Rail: "sepa", Amount: decimal.NewFromInt(300), Currency: "EUR", SourceRef: "stmt.xml"},
	}
	results := MatchEntries(s, rec, entries)
	if len(results) != 3 {
		t.Fatalf("len = %d", len(results))
	}
	matched := 0
	broken := 0
	for _, r := range results {
		if r.Matched {
			matched++
		} else if r.Error != nil && r.Error.Code == rail.CodeSettlementBreak {
			broken++
		}
	}
	if matched != 2 || broken != 1 {
		t.Fatalf("matched=%d broken=%d", matched, broken)
	}
	if s.SettledAmount("p1").Cmp(decimal.NewFromInt(100)) != 0 || s.SettledAmount("p2").Cmp(decimal.NewFromInt(300)) != 0 {
		t.Fatalf("settled p1=%v p2=%v", s.SettledAmount("p1"), s.SettledAmount("p2"))
	}
	completed := 0
	breaks := 0
	for _, e := range rec.Events() {
		if e.Type == "rail.settlement.completed" {
			completed++
		}
		if e.Type == "rail.settlement.break" {
			breaks++
		}
	}
	if completed != 2 || breaks != 1 {
		t.Fatalf("completed=%d breaks=%d", completed, breaks)
	}
}

func TestMatchEntriesNilAuditUsesRecorder(t *testing.T) {
	t.Parallel()
	s := store.New()
	s.Upsert(store.Record{PaymentID: "p1", Rail: "card", Status: rail.StatusCaptured})
	results := MatchEntries(s, nil, []SettledEntry{{PaymentID: "p1", Rail: "card", Amount: decimal.NewFromInt(10), Currency: "USD"}})
	if len(results) != 1 || !results[0].Matched {
		t.Fatalf("results = %+v", results)
	}
}
