package inbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)

// janitorLockKey: one worker replica per database cleans the inbox at a time.
// Transaction-scoped advisory lock: released by commit/rollback, also when the
// holder dies, so it can never get stuck.
const janitorLockKey = int64(0x1b0c_7a9e) // arbitrary, unique within this schema

// Janitor deletes inbox rows older than the retention.
type Janitor struct {
	pool *pgxpool.Pool
	cfg  config.Inbox
	log  *slog.Logger
}

// NewJanitor builds a Janitor.
func NewJanitor(pool *pgxpool.Pool, cfg config.Inbox, log *slog.Logger) *Janitor {
	return &Janitor{pool: pool, cfg: cfg, log: log}
}

// Run cleans up every JanitorInterval until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) error {
	t := time.NewTicker(j.cfg.JanitorInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n, err := j.Clean(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			j.log.WarnContext(ctx, "inbox janitor failed", "err", err, "deleted", n)
		case n > 0:
			j.log.InfoContext(ctx, "inbox janitor", "deleted", n)
		}
	}
}

// Clean deletes expired rows in chunks. If another replica holds the lock it
// returns immediately: one cleaner is enough.
func (j *Janitor) Clean(ctx context.Context) (int64, error) {
	const chunk = 1000
	var total int64
	for ctx.Err() == nil {
		var deleted int64
		locked := true
		err := postgres.WithTx(ctx, j.pool, func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, janitorLockKey).Scan(&locked); err != nil || !locked {
				return err
			}
			tag, err := tx.Exec(ctx,
				`DELETE FROM inbox WHERE (consumer, message_id) IN (
				   SELECT consumer, message_id FROM inbox
				   WHERE received_at < now() - make_interval(secs => $1)
				   LIMIT $2)`, j.cfg.Retention.Seconds(), chunk)
			deleted = tag.RowsAffected()
			return err
		})
		if err != nil {
			return total, fmt.Errorf("inbox cleanup: %w", err)
		}
		total += deleted
		if !locked || deleted < chunk {
			return total, nil
		}
	}
	return total, nil
}
