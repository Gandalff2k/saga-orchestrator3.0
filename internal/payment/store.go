package payment

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// recordPayment stores the saga's payment outcome once. If the saga already
// has a payment (a new charge command for the same saga), the STORED outcome
// is returned instead, so the customer is never charged twice and the reply
// never changes its mind.
//
// Two statements on purpose. A single "INSERT … ON CONFLICT DO NOTHING
// RETURNING … UNION SELECT …" reads the existing row with the statement's
// snapshot, which misses a row committed by a concurrent transaction after the
// statement started (exactly the case where the conflict happened). The
// second statement gets a fresh snapshot (READ COMMITTED) and sees it.
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
