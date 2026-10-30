package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	reservationHeld     = "HELD"
	reservationReleased = "RELEASED"
)

// reservationBySaga returns the saga's reservation, if it has one.
func reservationBySaga(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID) (seatID int, status string, found bool, err error) {
	err = tx.QueryRow(ctx,
		`SELECT seat_id, status FROM reservations WHERE saga_id = $1`, sagaID).Scan(&seatID, &status)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, "", false, nil
	case err != nil:
		return 0, "", false, fmt.Errorf("select reservation: %w", err)
	}
	return seatID, status, true, nil
}

// takeSeat flips a FREE seat to RESERVED. false = taken or nonexistent.
//
// Two sagas racing for one seat are serialized by the row lock: the loser's
// UPDATE waits, re-checks status='FREE' on the committed row (READ COMMITTED)
// and matches nothing. The partial unique index on reservations is the
// backstop if this logic were ever bypassed.
func takeSeat(ctx context.Context, tx pgx.Tx, seatID int) (bool, error) {
	tag, err := tx.Exec(ctx,
		`UPDATE seats SET status = 'RESERVED' WHERE seat_id = $1 AND status = 'FREE'`, seatID)
	if err != nil {
		return false, fmt.Errorf("take seat: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func seatExists(ctx context.Context, tx pgx.Tx, seatID int) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM seats WHERE seat_id = $1)`, seatID).Scan(&exists); err != nil {
		return false, fmt.Errorf("seat exists: %w", err)
	}
	return exists, nil
}

func insertReservation(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID, seatID int) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO reservations (saga_id, seat_id, status) VALUES ($1, $2, $3)`,
		sagaID, seatID, reservationHeld)
	if err != nil {
		return fmt.Errorf("insert reservation: %w", err)
	}
	return nil
}

// releaseReservation marks the saga's HELD reservation RELEASED and returns
// its seat. ok=false: nothing was held (never reserved, or already released).
func releaseReservation(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID) (seatID int, ok bool, err error) {
	err = tx.QueryRow(ctx,
		`UPDATE reservations SET status = $2, updated_at = now()
		 WHERE saga_id = $1 AND status = $3
		 RETURNING seat_id`, sagaID, reservationReleased, reservationHeld).Scan(&seatID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("release reservation: %w", err)
	}
	return seatID, true, nil
}

func freeSeat(ctx context.Context, tx pgx.Tx, seatID int) error {
	if _, err := tx.Exec(ctx, `UPDATE seats SET status = 'FREE' WHERE seat_id = $1`, seatID); err != nil {
		return fmt.Errorf("free seat: %w", err)
	}
	return nil
}
