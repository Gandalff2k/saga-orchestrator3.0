package risk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Gandalff2k/saga3/internal/messaging"
)

type db interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func lockKeys(ctx context.Context, tx pgx.Tx, keys ...string) error {
	slices.Sort(keys)
	for _, k := range keys {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, k); err != nil {
			return fmt.Errorf("lock %s: %w", k, err)
		}
	}
	return nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID, s messaging.Score) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO risk_events (saga_id, customer_id, card_hash, ip, country, amount_cents)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		sagaID, s.CustomerID, s.CardHash, s.IP, s.Country, s.AmountCents)
	if err != nil {
		return fmt.Errorf("insert risk event: %w", err)
	}
	return nil
}

func features(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID, s messaging.Score) (Features, error) {
	var f Features
	err := tx.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM risk_events
		    WHERE card_hash = $1 AND amount_cents <= $2 AND created_at > now() - interval '10 minutes'),
		  (SELECT count(DISTINCT card_hash) FROM risk_events
		    WHERE customer_id = $3 AND created_at > now() - interval '24 hours'),
		  (SELECT count(*) FROM risk_events
		    WHERE customer_id = $3 AND saga_id <> $4 AND created_at > now() - interval '90 days'),
		  (SELECT EXISTS (SELECT 1 FROM risk_events
		    WHERE customer_id = $3 AND saga_id <> $4 AND country = $5 AND created_at > now() - interval '90 days')),
		  (SELECT COALESCE(avg(amount_cents), 0)::bigint FROM risk_events
		    WHERE customer_id = $3 AND saga_id <> $4 AND created_at > now() - interval '90 days'),
		  (SELECT count(DISTINCT customer_id) FROM risk_events
		    WHERE ip = $6 AND created_at > now() - interval '1 hour')`,
		s.CardHash, smallAmountCents, s.CustomerID, sagaID, s.Country, s.IP,
	).Scan(&f.CardSmallTxn10m, &f.CustomerCards24h, &f.CustomerPrior90d, &f.CustomerCountrySeen,
		&f.CustomerAvgCents, &f.IPCustomers1h)
	if err != nil {
		return Features{}, fmt.Errorf("risk features: %w", err)
	}
	return f, nil
}

func insertDecision(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID, r Result) error {
	hits, err := json.Marshal(nonNil(r.Hits))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO risk_decisions (saga_id, decision, score, hits, rules_version) VALUES ($1, $2, $3, $4, $5)`,
		sagaID, r.Decision, r.Score, hits, RulesVersion)
	if err != nil {
		return fmt.Errorf("insert risk decision: %w", err)
	}
	if r.Decision == Review {
		if _, err := tx.Exec(ctx, `INSERT INTO review_cases (saga_id) VALUES ($1)`, sagaID); err != nil {
			return fmt.Errorf("insert review case: %w", err)
		}
	}
	return nil
}

type stored struct {
	result     Result
	caseStatus string
}

func storedDecision(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID) (stored, bool, error) {
	var (
		s    stored
		hits []byte
	)
	err := tx.QueryRow(ctx,
		`SELECT d.decision, d.score, d.hits, COALESCE(c.status, '')
		 FROM risk_decisions d LEFT JOIN review_cases c USING (saga_id)
		 WHERE d.saga_id = $1`, sagaID).Scan(&s.result.Decision, &s.result.Score, &hits, &s.caseStatus)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return stored{}, false, nil
	case err != nil:
		return stored{}, false, fmt.Errorf("select risk decision: %w", err)
	}
	if err := json.Unmarshal(hits, &s.result.Hits); err != nil {
		return stored{}, false, fmt.Errorf("decode hits: %w", err)
	}
	return s, true, nil
}

func expireCase(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID) (bool, error) {
	tag, err := tx.Exec(ctx,
		`UPDATE review_cases SET status = 'EXPIRED', decided_by = 'saga-timeout', decided_at = now()
		 WHERE saga_id = $1 AND status = 'PENDING'`, sagaID)
	if err != nil {
		return false, fmt.Errorf("expire review case: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
