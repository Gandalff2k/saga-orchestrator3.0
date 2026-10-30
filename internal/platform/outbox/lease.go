package outbox

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func plan(owned []int, total, alive int) (keep, release []int, claim int) {
	fair := fairShare(total, alive)
	owned = slices.Sorted(slices.Values(owned))
	if len(owned) > fair {
		return owned[:fair], owned[fair:], 0
	}
	return owned, nil, fair - len(owned)
}

func fairShare(total, alive int) int {
	alive = max(alive, 1)
	return (total + alive - 1) / alive
}

type leaseStore struct {
	pool     *pgxpool.Pool
	instance string
	ttl      time.Duration
}

func (s leaseStore) totalShards(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_shard_lease`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count shards: %w", err)
	}
	return n, nil
}

func (s leaseStore) heartbeat(ctx context.Context) (alive int, err error) {
	_, err = s.pool.Exec(ctx,
		`INSERT INTO outbox_relay_member (instance_id, heartbeat_at) VALUES ($1, now())
		 ON CONFLICT (instance_id) DO UPDATE SET heartbeat_at = now()`, s.instance)
	if err != nil {
		return 0, fmt.Errorf("heartbeat: %w", err)
	}
	err = s.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_relay_member
		 WHERE heartbeat_at > now() - make_interval(secs => $1)`, s.ttl.Seconds()).Scan(&alive)
	if err != nil {
		return 0, fmt.Errorf("count members: %w", err)
	}
	return alive, nil
}

func (s leaseStore) renew(ctx context.Context) ([]int, error) {
	return s.shards(ctx,
		`UPDATE outbox_shard_lease SET expires_at = now() + make_interval(secs => $2)
		 WHERE owner = $1 AND expires_at > now()
		 RETURNING shard`, s.instance, s.ttl.Seconds())
}

func (s leaseStore) claim(ctx context.Context, n int) ([]int, error) {
	return s.shards(ctx,
		`UPDATE outbox_shard_lease SET owner = $1, expires_at = now() + make_interval(secs => $2)
		 WHERE expires_at < now()
		   AND shard IN (SELECT shard FROM outbox_shard_lease WHERE expires_at < now() ORDER BY random() LIMIT $3)
		 RETURNING shard`, s.instance, s.ttl.Seconds(), n)
}

func (s leaseStore) release(ctx context.Context, shards []int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox_shard_lease SET owner = NULL, expires_at = '-infinity'
		 WHERE owner = $1 AND shard = ANY($2)`, s.instance, shards)
	if err != nil {
		return fmt.Errorf("release shards: %w", err)
	}
	return nil
}

func (s leaseStore) leave(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM outbox_relay_member WHERE instance_id = $1`, s.instance); err != nil {
		return fmt.Errorf("leave: %w", err)
	}
	return nil
}

func (s leaseStore) forgetDead(ctx context.Context) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM outbox_relay_member WHERE heartbeat_at < now() - make_interval(secs => $1)`,
		10*s.ttl.Seconds())
	if err != nil {
		return fmt.Errorf("forget dead members: %w", err)
	}
	return nil
}

func (s leaseStore) shards(ctx context.Context, sql string, args ...any) ([]int, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("lease query: %w", err)
	}
	defer rows.Close()

	var out []int
	for rows.Next() {
		var shard int
		if err := rows.Scan(&shard); err != nil {
			return nil, fmt.Errorf("lease scan: %w", err)
		}
		out = append(out, shard)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lease rows: %w", err)
	}
	return out, nil
}
