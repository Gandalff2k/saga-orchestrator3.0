package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// row is satisfied by *pgxpool.Pool and pgx.Tx: reads work in and out of a transaction.
type row interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// insertOrder inserts a PENDING order. inserted=false means the idempotency
// key is already taken (by a committed order, or by a concurrent transaction
// that we waited for — the unique index serializes them).
func insertOrder(ctx context.Context, tx pgx.Tx, id uuid.UUID, in PlaceOrder) (createdAt time.Time, inserted bool, err error) {
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (order_id, idempotency_key, seat_id, amount_cents, status)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING created_at`,
		id, nullIfEmpty(in.IdempotencyKey), in.SeatID, in.AmountCents, OrderPending,
	).Scan(&createdAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return time.Time{}, false, nil
	case err != nil:
		return time.Time{}, false, fmt.Errorf("insert order: %w", err)
	}
	return createdAt, true, nil
}

// insertSaga inserts a new saga. deadline_at is computed by Postgres (one clock
// for every replica) and is when the sweeper gives up waiting for a reply.
func insertSaga(ctx context.Context, tx pgx.Tx, s Saga, stepTimeout time.Duration) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO sagas (saga_id, order_id, state, seat_id, amount_cents, deadline_at)
		 VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))`,
		s.ID, s.OrderID, s.State, s.SeatID, s.AmountCents, stepTimeout.Seconds())
	if err != nil {
		return fmt.Errorf("insert saga: %w", err)
	}
	return nil
}

const selectOrder = `
	SELECT o.order_id, s.saga_id, o.seat_id, o.amount_cents, o.status, s.state,
	       COALESCE(o.failure_reason, ''), o.created_at
	FROM orders o
	JOIN sagas s ON s.order_id = o.order_id`

func orderByID(ctx context.Context, q row, id uuid.UUID) (Order, error) {
	return scanOrder(q.QueryRow(ctx, selectOrder+` WHERE o.order_id = $1`, id))
}

func orderByIdempotencyKey(ctx context.Context, q row, key string) (Order, error) {
	return scanOrder(q.QueryRow(ctx, selectOrder+` WHERE o.idempotency_key = $1`, key))
}

func scanOrder(r pgx.Row) (Order, error) {
	var o Order
	err := r.Scan(&o.ID, &o.SagaID, &o.SeatID, &o.AmountCents, &o.Status, &o.SagaState, &o.FailureReason, &o.CreatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Order{}, ErrNotFound
	case err != nil:
		return Order{}, fmt.Errorf("select order: %w", err)
	}
	return o, nil
}

// sagaForUpdate loads and row-locks a saga. ErrNotFound if it doesn't exist.
func sagaForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Saga, time.Time, error) {
	var (
		s       Saga
		started time.Time
	)
	err := tx.QueryRow(ctx,
		`SELECT saga_id, order_id, state, seat_id, amount_cents, created_at
		 FROM sagas WHERE saga_id = $1
		 FOR UPDATE`, id).Scan(&s.ID, &s.OrderID, &s.State, &s.SeatID, &s.AmountCents, &started)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Saga{}, time.Time{}, ErrNotFound
	case err != nil:
		return Saga{}, time.Time{}, fmt.Errorf("select saga: %w", err)
	}
	return s, started, nil
}

// updateSaga moves the saga to state and restarts its step deadline.
func updateSaga(ctx context.Context, tx pgx.Tx, id uuid.UUID, state State, stepTimeout time.Duration) error {
	_, err := tx.Exec(ctx,
		`UPDATE sagas
		 SET state = $2, version = version + 1, updated_at = now(),
		     deadline_at = now() + make_interval(secs => $3)
		 WHERE saga_id = $1`, id, state, stepTimeout.Seconds())
	if err != nil {
		return fmt.Errorf("update saga: %w", err)
	}
	return nil
}

// updateOrder sets the order status and/or failure reason; empty = unchanged.
func updateOrder(ctx context.Context, tx pgx.Tx, id uuid.UUID, status OrderStatus, reason string) error {
	_, err := tx.Exec(ctx,
		`UPDATE orders
		 SET status = COALESCE(NULLIF($2, ''), status),
		     failure_reason = COALESCE(NULLIF($3, ''), failure_reason),
		     updated_at = now()
		 WHERE order_id = $1`, id, string(status), reason)
	if err != nil {
		return fmt.Errorf("update order: %w", err)
	}
	return nil
}

// nullIfEmpty maps "" to NULL: the unique index on idempotency_key ignores
// NULLs, so requests without a key never collide with each other.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
