package payment

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func recordPayment(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID, amountCents int64, d decision) (decision, error) {
	err := tx.QueryRow(ctx,
		`INSERT INTO payments (saga_id, amount_cents, status, reason)
		 VALUES ($1, $2, $3, NULLIF($4, ''))
		 ON CONFLICT (saga_id) DO NOTHING
		 RETURNING saga_id`, sagaID, amountCents, d.status, d.reason).Scan(new(uuid.UUID))
	switch {
	case err == nil:
		return d, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return decision{}, fmt.Errorf("insert payment: %w", err)
	}

	var stored decision
	err = tx.QueryRow(ctx,
		`SELECT status, COALESCE(reason, '') FROM payments WHERE saga_id = $1`, sagaID,
	).Scan(&stored.status, &stored.reason)
	if err != nil {
		return decision{}, fmt.Errorf("select payment: %w", err)
	}
	return stored, nil
}

func refundPayment(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID) (string, error) {
	tag, err := tx.Exec(ctx,
		`UPDATE payments SET status = $2, updated_at = now() WHERE saga_id = $1 AND status = $3`,
		sagaID, statusRefunded, statusCharged)
	if err != nil {
		return "", fmt.Errorf("refund payment: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return "refunded", nil
	}

	tag, err = tx.Exec(ctx,
		`INSERT INTO payments (saga_id, amount_cents, status, reason)
		 VALUES ($1, 0, $2, 'cancelled before charge')
		 ON CONFLICT (saga_id) DO NOTHING`, sagaID, statusCancelled)
	if err != nil {
		return "", fmt.Errorf("payment tombstone: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return "cancelled", nil
	}
	return "noop", nil
}
