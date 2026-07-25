package store

import (
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
)

// Record is a single rail payment request stored in-memory.
type Record struct {
	PaymentID      string
	Rail           string
	Operation      string
	Amount         decimal.Decimal
	Currency       string
	Status         rail.Status
	IdempotencyKey string
	RailRef        string
	ErrorCode      string
	ErrorMessage   string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// SettleEntry is an in-memory settlement record.
type SettleEntry struct {
	SettleID  string
	Rail      string
	PaymentID string
	Amount    decimal.Decimal
	Currency  string
	SettledAt time.Time
	SourceRef string
}

// ChargebackEntry is an in-memory dispute record received from a rail.
type ChargebackEntry struct {
	ChargebackID string
	Rail         string
	PaymentID    string
	Amount       decimal.Decimal
	ReasonCode   string
	ReceivedAt   time.Time
	Status       rail.Status
}

// Store is a concurrency-safe in-memory rail state store. It replaces the
// PostgreSQL tables from the full spec for the simplified implementation.
type Store interface {
	Upsert(r Record)
	Get(paymentID string) (Record, bool)
	SetStatus(paymentID string, status rail.Status, code, msg string) bool
	AddSettle(e SettleEntry)
	Settles() []SettleEntry
	SettledAmount(paymentID string) decimal.Decimal
	All() []Record
	AddChargeback(e ChargebackEntry) ChargebackEntry
	Chargebacks() []ChargebackEntry
	ChargebacksFor(paymentID string) []ChargebackEntry
}

type MemStore struct {
	mu          sync.RWMutex
	requests    map[string]*Record
	settles     []SettleEntry
	settleAmt   map[string]decimal.Decimal
	chargebacks []ChargebackEntry
}

func New() *MemStore {
	return &MemStore{
		requests:    make(map[string]*Record),
		settleAmt:   make(map[string]decimal.Decimal),
		chargebacks: nil,
	}
}

// Upsert inserts or updates the record for PaymentID.
func (s *MemStore) Upsert(r Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if existing, ok := s.requests[r.PaymentID]; ok {
		r.CreatedAt = existing.CreatedAt
		r.UpdatedAt = now
		s.requests[r.PaymentID] = &r
		return
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	r.UpdatedAt = now
	s.requests[r.PaymentID] = &r
}

// Get returns a copy of the record for PaymentID, or false.
func (s *MemStore) Get(paymentID string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.requests[paymentID]
	if !ok {
		return Record{}, false
	}
	return *r, true
}

// SetStatus updates just the status + error fields of a record.
func (s *MemStore) SetStatus(paymentID string, status rail.Status, code, msg string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.requests[paymentID]
	if !ok {
		return false
	}
	r.Status = status
	r.ErrorCode = code
	r.ErrorMessage = msg
	r.UpdatedAt = time.Now().UTC()
	return true
}

// AddSettle records a settlement and marks the matching request Settled.
func (s *MemStore) AddSettle(e SettleEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.SettledAt = time.Now().UTC()
	if e.SettleID == "" {
		e.SettleID = "settle-" + e.PaymentID
	}
	s.settles = append(s.settles, e)
	s.settleAmt[e.PaymentID] = s.settleAmt[e.PaymentID].Add(e.Amount)
	if r, ok := s.requests[e.PaymentID]; ok {
		r.Status = rail.StatusSettled
		r.UpdatedAt = time.Now().UTC()
	}
}

// Settles returns a copy of all settlement entries.
func (s *MemStore) Settles() []SettleEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SettleEntry, len(s.settles))
	copy(out, s.settles)
	return out
}

// SettledAmount returns the total settled amount for a payment.
func (s *MemStore) SettledAmount(paymentID string) decimal.Decimal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settleAmt[paymentID]
}

// All returns copies of all records (for tests/inspection).
func (s *MemStore) All() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Record, 0, len(s.requests))
	for _, r := range s.requests {
		out = append(out, *r)
	}
	return out
}

// AddChargeback records a chargeback / dispute and marks the matching request
// Chargeback. Returns the inserted entry with a generated ChargebackID and
// ReceivedAt if unset.
func (s *MemStore) AddChargeback(e ChargebackEntry) ChargebackEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = time.Now().UTC()
	}
	if e.ChargebackID == "" {
		e.ChargebackID = "cbk-" + e.PaymentID
	}
	if e.Status == "" {
		e.Status = rail.StatusChargeback
	}
	s.chargebacks = append(s.chargebacks, e)
	if r, ok := s.requests[e.PaymentID]; ok {
		r.Status = rail.StatusChargeback
		r.UpdatedAt = time.Now().UTC()
	}
	return e
}

// Chargebacks returns a copy of all recorded chargeback entries.
func (s *MemStore) Chargebacks() []ChargebackEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ChargebackEntry, len(s.chargebacks))
	copy(out, s.chargebacks)
	return out
}

// ChargebacksFor returns a copy of all chargeback entries for a payment id.
func (s *MemStore) ChargebacksFor(paymentID string) []ChargebackEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ChargebackEntry
	for _, c := range s.chargebacks {
		if c.PaymentID == paymentID {
			out = append(out, c)
		}
	}
	return out
}
