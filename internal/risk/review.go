package risk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)

// Review case errors.
var (
	ErrCaseNotFound = errors.New("review case not found")
	ErrCaseClosed   = errors.New("review case is already closed")
)

// Case is a review case with everything a reviewer needs to decide it.
type Case struct {
	SagaID         uuid.UUID       `json:"saga_id"`
	Status         string          `json:"status"`
	CustomerID     string          `json:"customer_id"`
	CardHash       string          `json:"card_hash"`
	IP             string          `json:"ip"`
	Country        string          `json:"country"`
	AmountCents    int64           `json:"amount_cents"`
	Score          int             `json:"score"`
	Hits           []Hit           `json:"hits"`
	Recommendation json.RawMessage `json:"recommendation,omitempty"`
	DecidedBy      string          `json:"decided_by,omitempty"`
	Note           string          `json:"note,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

const selectCase = `
	SELECT c.saga_id, c.status, e.customer_id, e.card_hash, e.ip, e.country, e.amount_cents,
	       d.score, d.hits, c.recommendation, COALESCE(c.decided_by, ''), COALESCE(c.note, ''), c.created_at
	FROM review_cases c
	JOIN risk_decisions d USING (saga_id)
	JOIN risk_events e USING (saga_id)`

func scanCase(r pgx.Row) (Case, error) {
	var (
		c    Case
		hits []byte
		rec  []byte
	)
	err := r.Scan(&c.SagaID, &c.Status, &c.CustomerID, &c.CardHash, &c.IP, &c.Country, &c.AmountCents,
		&c.Score, &hits, &rec, &c.DecidedBy, &c.Note, &c.CreatedAt)
	if err != nil {
		return Case{}, err
	}
	if err := json.Unmarshal(hits, &c.Hits); err != nil {
		return Case{}, fmt.Errorf("decode hits: %w", err)
	}
	c.Recommendation = rec
	return c, nil
}

// ListCases returns cases with the given status, oldest first.
func ListCases(ctx context.Context, q db, status string, limit int) ([]Case, error) {
	rows, err := q.Query(ctx, selectCase+` WHERE c.status = $1 ORDER BY c.created_at LIMIT $2`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list review cases: %w", err)
	}
	defer rows.Close()
	var out []Case
	for rows.Next() {
		c, err := scanCase(rows)
		if err != nil {
			return nil, fmt.Errorf("scan review case: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list review cases: %w", err)
	}
	return out, nil
}

// GetCase returns one case.
func GetCase(ctx context.Context, q db, sagaID uuid.UUID) (Case, error) {
	c, err := scanCase(q.QueryRow(ctx, selectCase+` WHERE c.saga_id = $1`, sagaID))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Case{}, ErrCaseNotFound
	case err != nil:
		return Case{}, fmt.Errorf("select review case: %w", err)
	}
	return c, nil
}

// DecideCase records a reviewer's decision and tells the saga, in one transaction.
func DecideCase(ctx context.Context, pool postgres.TxBeginner, sagaID uuid.UUID, approve bool, reviewer, note string) error {
	if reviewer == "" {
		return errors.New("reviewer is required")
	}
	status, typ, reason := "REJECTED", messaging.EvtRiskRejected, ReasonReviewRejected
	if approve {
		status, typ, reason = "APPROVED", messaging.EvtRiskApproved, ""
	}
	return postgres.WithTx(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE review_cases SET status = $2, decided_by = $3, note = NULLIF($4, ''), decided_at = now()
			 WHERE saga_id = $1 AND status = 'PENDING'`, sagaID, status, reviewer, note)
		if err != nil {
			return fmt.Errorf("decide review case: %w", err)
		}
		if tag.RowsAffected() == 0 {
			if _, err := GetCase(ctx, tx, sagaID); err != nil {
				return err
			}
			return ErrCaseClosed
		}
		out, err := messaging.NewReply(typ, sagaID, reason)
		if err != nil {
			return err
		}
		return outbox.Write(ctx, tx, messaging.TopicOrchestratorReplies, out)
	})
}
