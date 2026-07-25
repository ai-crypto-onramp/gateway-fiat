package settlement

import (
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-fiat/internal/audit"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/rail"
	"github.com/ai-crypto-onramp/gateway-fiat/internal/store"
)

// SettledEntry is the common shape produced by all per-rail settlement
// parsers. The matcher uses it to reconcile parsed entries against
// rail_requests rows.
type SettledEntry struct {
	PaymentID string
	Rail      string
	Amount    decimal.Decimal
	Currency  string
	SettledAt time.Time
	SourceRef string
}

// MatchResult is the outcome of matching a single settled entry.
type MatchResult struct {
	Entry   SettledEntry
	Matched bool
	Error   *rail.Error
}

// MatchEntries reconciles parsed settlement entries against the store's
// rail_requests rows. Matched entries are recorded as rail_settlements rows
// (via Store.AddSettle) and emit rail.settlement.completed events; unmatched
// entries emit a SETTLEMENT_BREAK error and a rail.settlement.break alert
// event. Returns one MatchResult per input entry, in order.
func MatchEntries(s store.Store, as audit.Sink, entries []SettledEntry) []MatchResult {
	if as == nil {
		as = audit.NewRecorder()
	}
	results := make([]MatchResult, len(entries))
	for i, e := range entries {
		rec, ok := s.Get(e.PaymentID)
		if !ok {
			re := rail.NewError(rail.CodeSettlementBreak, "no rail_requests row for payment_id "+e.PaymentID)
			results[i] = MatchResult{Entry: e, Matched: false, Error: re}
			as.Emit(audit.Event{
				Type:      "rail.settlement.break",
				PaymentID: e.PaymentID,
				Rail:      e.Rail,
				Operation: "settle",
				Status:    string(rail.StatusFailed),
				Amount:    e.Amount,
				Payload:   map[string]any{"source_ref": e.SourceRef, "error_code": rail.CodeSettlementBreak},
			})
			continue
		}
		_ = rec
		s.AddSettle(store.SettleEntry{
			SettleID:  "settle-" + e.PaymentID,
			Rail:      e.Rail,
			PaymentID: e.PaymentID,
			Amount:    e.Amount,
			Currency:  e.Currency,
			SettledAt: e.SettledAt,
			SourceRef: e.SourceRef,
		})
		results[i] = MatchResult{Entry: e, Matched: true}
		as.Emit(audit.Event{
			Type:      "rail.settlement.completed",
			PaymentID: e.PaymentID,
			Rail:      e.Rail,
			Operation: "settle",
			Status:    string(rail.StatusSettled),
			Amount:    e.Amount,
			Payload:   map[string]any{"source_ref": e.SourceRef, "settled_at": e.SettledAt},
		})
	}
	return results
}
