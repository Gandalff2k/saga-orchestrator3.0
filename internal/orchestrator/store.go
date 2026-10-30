package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type row interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

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

func insertSaga(ctx context.Context, tx pgx.Tx, s Saga) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO sagas (saga_id, order_id, state, seat_id, amount_cents)
		 VALUES ($1, $2, $3, $4, $5)`,
		s.ID, s.OrderID, s.State, s.SeatID, s.AmountCents)
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

func overdueForUpdate(ctx context.Context, tx pgx.Tx, timeout time.Duration, limit int) ([]Saga, []time.Time, error) {
	rows, err := tx.Query(ctx,
		`SELECT saga_id, order_id, state, seat_id, amount_cents, created_at
		 FROM sagas
		 WHERE state NOT IN ('COMPLETED', 'ABORTED')
		   AND updated_at < now() - make_interval(secs => $1)
		 ORDER BY updated_at
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`, timeout.Seconds(), limit)
	if err != nil {
		return nil, nil, fmt.Errorf("select overdue sagas: %w", err)
	}
	defer rows.Close()

	var (
		sagas   []Saga
		started []time.Time
	)
	for rows.Next() {
		var (
			s Saga
			t time.Time
		)
		if err := rows.Scan(&s.ID, &s.OrderID, &s.State, &s.SeatID, &s.AmountCents, &t); err != nil {
			return nil, nil, fmt.Errorf("scan overdue saga: %w", err)
		}
		sagas, started = append(sagas, s), append(started, t)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("overdue sagas: %w", err)
	}
	return sagas, started, nil
}

func updateSaga(ctx context.Context, tx pgx.Tx, id uuid.UUID, state State) error {
	_, err := tx.Exec(ctx,
		`UPDATE sagas
		 SET state = $2, version = version + 1, updated_at = now()
		 WHERE saga_id = $1`, id, state)
	if err != nil {
		return fmt.Errorf("update saga: %w", err)
	}
	return nil
}

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

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
