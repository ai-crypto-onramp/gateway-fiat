package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/rail-connector/internal/rail"
	"github.com/ai-crypto-onramp/rail-connector/internal/store"
	"github.com/ai-crypto-onramp/rail-connector/internal/store/migrations"
)

type DB struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	runner := migrations.NewRunner(
		func(c context.Context, q string, args ...any) error {
			_, err := pool.Exec(c, q, args...)
			return err
		},
		func(c context.Context, version string) (bool, error) {
			var exists bool
			err := pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists)
			return exists, err
		},
	)
	if err := runner.Up(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &DB{pool: pool}, nil
}

func (d *DB) Close() error {
	d.pool.Close()
	return nil
}

func (d *DB) Ping(ctx context.Context) error { return d.pool.Ping(ctx) }

func (d *DB) Upsert(r store.Record) {
	ctx := context.Background()
	now := time.Now().UTC()
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	r.UpdatedAt = now
	_, _ = d.pool.Exec(ctx, `INSERT INTO rail_requests
	(payment_id, rail, operation, amount, currency, status, idempotency_key, rail_ref, error_code, error_message, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	ON CONFLICT (payment_id) DO UPDATE SET
	  rail=EXCLUDED.rail, operation=EXCLUDED.operation, amount=EXCLUDED.amount, currency=EXCLUDED.currency,
	  status=EXCLUDED.status, idempotency_key=EXCLUDED.idempotency_key, rail_ref=EXCLUDED.rail_ref,
	  error_code=EXCLUDED.error_code, error_message=EXCLUDED.error_message, updated_at=EXCLUDED.updated_at`,
		r.PaymentID, r.Rail, r.Operation, r.Amount.String(), r.Currency, string(r.Status),
		r.IdempotencyKey, r.RailRef, r.ErrorCode, r.ErrorMessage, r.CreatedAt, r.UpdatedAt)
}

func (d *DB) Get(paymentID string) (store.Record, bool) {
	ctx := context.Background()
	r, err := scanRecord(d.pool.QueryRow(ctx, recordSelectSQL()+` WHERE payment_id=$1`, paymentID))
	if err != nil {
		return store.Record{}, false
	}
	return r, true
}

func (d *DB) SetStatus(paymentID string, status rail.Status, code, msg string) bool {
	ctx := context.Background()
	tag, err := d.pool.Exec(ctx, `UPDATE rail_requests SET status=$2, error_code=$3, error_message=$4, updated_at=now() WHERE payment_id=$1`,
		paymentID, string(status), code, msg)
	if err != nil {
		return false
	}
	return tag.RowsAffected() > 0
}

func (d *DB) AddSettle(e store.SettleEntry) {
	ctx := context.Background()
	if e.SettledAt.IsZero() {
		e.SettledAt = time.Now().UTC()
	}
	if e.SettleID == "" {
		e.SettleID = "settle-" + e.PaymentID
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO rail_settlements
	(settle_id, rail, payment_id, amount, currency, settled_at, source_ref)
	VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (settle_id) DO NOTHING`,
		e.SettleID, e.Rail, e.PaymentID, e.Amount.String(), e.Currency, e.SettledAt, e.SourceRef); err != nil {
		return
	}
	if _, err := tx.Exec(ctx, `UPDATE rail_requests SET status=$2, updated_at=now() WHERE payment_id=$1`,
		e.PaymentID, string(rail.StatusSettled)); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

func (d *DB) Settles() []store.SettleEntry {
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `SELECT settle_id, rail, payment_id, amount, currency, settled_at, source_ref FROM rail_settlements ORDER BY settled_at ASC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []store.SettleEntry{}
	for rows.Next() {
		var e store.SettleEntry
		var amt string
		if err := rows.Scan(&e.SettleID, &e.Rail, &e.PaymentID, &amt, &e.Currency, &e.SettledAt, &e.SourceRef); err != nil {
			return nil
		}
		e.Amount, _ = decimal.NewFromString(amt)
		out = append(out, e)
	}
	return out
}

func (d *DB) SettledAmount(paymentID string) decimal.Decimal {
	ctx := context.Background()
	var sum string
	err := d.pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount), 0) FROM rail_settlements WHERE payment_id=$1`, paymentID).Scan(&sum)
	if err != nil {
		return decimal.Zero
	}
	amt, _ := decimal.NewFromString(sum)
	return amt
}

func (d *DB) All() []store.Record {
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, recordSelectSQL()+` ORDER BY created_at ASC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []store.Record{}
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil
		}
		out = append(out, r)
	}
	return out
}

func (d *DB) AddChargeback(e store.ChargebackEntry) store.ChargebackEntry {
	ctx := context.Background()
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = time.Now().UTC()
	}
	if e.ChargebackID == "" {
		e.ChargebackID = "cbk-" + e.PaymentID
	}
	if e.Status == "" {
		e.Status = rail.StatusChargeback
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return e
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO rail_chargebacks
	(chargeback_id, rail, payment_id, amount, reason_code, received_at, status)
	VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (chargeback_id) DO NOTHING`,
		e.ChargebackID, e.Rail, e.PaymentID, e.Amount.String(), e.ReasonCode, e.ReceivedAt, string(e.Status)); err != nil {
		return e
	}
	if _, err := tx.Exec(ctx, `UPDATE rail_requests SET status=$2, updated_at=now() WHERE payment_id=$1`,
		e.PaymentID, string(rail.StatusChargeback)); err != nil {
		return e
	}
	_ = tx.Commit(ctx)
	return e
}

func (d *DB) Chargebacks() []store.ChargebackEntry {
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `SELECT chargeback_id, rail, payment_id, amount, reason_code, received_at, status FROM rail_chargebacks ORDER BY received_at ASC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []store.ChargebackEntry{}
	for rows.Next() {
		var e store.ChargebackEntry
		var status, amt string
		if err := rows.Scan(&e.ChargebackID, &e.Rail, &e.PaymentID, &amt, &e.ReasonCode, &e.ReceivedAt, &status); err != nil {
			return nil
		}
		e.Amount, _ = decimal.NewFromString(amt)
		e.Status = rail.Status(status)
		out = append(out, e)
	}
	return out
}

func (d *DB) ChargebacksFor(paymentID string) []store.ChargebackEntry {
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `SELECT chargeback_id, rail, payment_id, amount, reason_code, received_at, status FROM rail_chargebacks WHERE payment_id=$1 ORDER BY received_at ASC`, paymentID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []store.ChargebackEntry{}
	for rows.Next() {
		var e store.ChargebackEntry
		var status, amt string
		if err := rows.Scan(&e.ChargebackID, &e.Rail, &e.PaymentID, &amt, &e.ReasonCode, &e.ReceivedAt, &status); err != nil {
			return nil
		}
		e.Amount, _ = decimal.NewFromString(amt)
		e.Status = rail.Status(status)
		out = append(out, e)
	}
	return out
}

func recordSelectSQL() string {
	return `SELECT payment_id, rail, operation, amount, currency, status, idempotency_key, rail_ref, error_code, error_message, created_at, updated_at FROM rail_requests`
}

func scanRecord(row pgx.Row) (store.Record, error) {
	var r store.Record
	var status, amt string
	if err := row.Scan(&r.PaymentID, &r.Rail, &r.Operation, &amt, &r.Currency, &status,
		&r.IdempotencyKey, &r.RailRef, &r.ErrorCode, &r.ErrorMessage, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return store.Record{}, err
	}
	r.Amount, _ = decimal.NewFromString(amt)
	r.Status = rail.Status(status)
	return r, nil
}

var _ store.Store = (*DB)(nil)
