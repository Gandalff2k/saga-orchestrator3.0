package risk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one past order as an investigator sees it.
type Event struct {
	ID           uuid.UUID `json:"id"`
	CustomerID   string    `json:"customer_id"`
	Card         string    `json:"card"`
	IP           string    `json:"ip"`
	Country      string    `json:"country"`
	AmountCents  int64     `json:"amount_cents"`
	Decision     string    `json:"decision"`
	ReviewStatus string    `json:"review_status,omitempty"`
	At           time.Time `json:"at"`
}

// ShortCard is how a card hash is shown to people and models: enough to compare, nothing more.
func ShortCard(hash string) string {
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return "card_" + hash
}

var historyColumn = map[string]string{"customer": "customer_id", "card": "card_hash", "ip": "ip"}

// History returns up to limit orders sharing the customer, card or ip, newest first, excluding one saga.
func History(ctx context.Context, q db, by, value string, exclude uuid.UUID, limit int) ([]Event, error) {
	col, ok := historyColumn[by]
	if !ok {
		return nil, fmt.Errorf("history: unknown key %q", by)
	}
	rows, err := q.Query(ctx, `
		SELECT e.saga_id, e.customer_id, e.card_hash, e.ip, e.country, e.amount_cents,
		       COALESCE(d.decision, ''), COALESCE(c.status, ''), e.created_at
		FROM risk_events e
		LEFT JOIN risk_decisions d USING (saga_id)
		LEFT JOIN review_cases c USING (saga_id)
		WHERE e.`+col+` = $1 AND e.saga_id <> $2
		ORDER BY e.created_at DESC
		LIMIT $3`, value, exclude, limit)
	if err != nil {
		return nil, fmt.Errorf("history by %s: %w", by, err)
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var (
			e    Event
			hash string
		)
		if err := rows.Scan(&e.ID, &e.CustomerID, &hash, &e.IP, &e.Country, &e.AmountCents, &e.Decision,
			&e.ReviewStatus, &e.At); err != nil {
			return nil, fmt.Errorf("scan history: %w", err)
		}
		e.Card = ShortCard(hash)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("history by %s: %w", by, err)
	}
	return out, nil
}

// ErrLeaseLost means another agent took the case, or a person already decided it.
var ErrLeaseLost = errors.New("review case lease lost")

// ClaimCase leases the oldest pending case without a recommendation to owner.
func ClaimCase(ctx context.Context, pool *pgxpool.Pool, owner string, lease time.Duration, maxAttempts int) (uuid.UUID, bool, error) {
	var id uuid.UUID
	err := pool.QueryRow(ctx, `
		UPDATE review_cases
		SET locked_by = $1, locked_until = now() + make_interval(secs => $2), agent_attempts = agent_attempts + 1
		WHERE saga_id = (
			SELECT saga_id FROM review_cases
			WHERE status = 'PENDING' AND recommendation IS NULL AND agent_attempts < $3
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING saga_id`, owner, lease.Seconds(), maxAttempts).Scan(&id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return uuid.Nil, false, nil
	case err != nil:
		return uuid.Nil, false, fmt.Errorf("claim review case: %w", err)
	}
	return id, true, nil
}

// SaveRecommendation stores the agent's advice if owner still holds the lease and the case is open.
func SaveRecommendation(ctx context.Context, pool *pgxpool.Pool, sagaID uuid.UUID, owner string, rec any) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tag, err := pool.Exec(ctx, `
		UPDATE review_cases SET recommendation = $3, locked_by = NULL, locked_until = NULL
		WHERE saga_id = $1 AND locked_by = $2 AND status = 'PENDING'`, sagaID, owner, b)
	if err != nil {
		return fmt.Errorf("save recommendation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ReleaseCase gives the lease back after a failed attempt, so another try can start at once.
func ReleaseCase(ctx context.Context, pool *pgxpool.Pool, sagaID uuid.UUID, owner string) error {
	_, err := pool.Exec(ctx,
		`UPDATE review_cases SET locked_by = NULL, locked_until = NULL WHERE saga_id = $1 AND locked_by = $2`,
		sagaID, owner)
	if err != nil {
		return fmt.Errorf("release review case: %w", err)
	}
	return nil
}
