package outbox

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

// Relay publishes outbox rows of ONE service database to Kafka.
//
// Scaling: rows are split into a fixed number of shards (outbox.shard is a
// generated column = hash(key) % N, N fixed by the migration). Every relay
// replica holds time-limited leases on a fair share of shards
// (outbox_shard_lease table) and is the only publisher for them, so rows of
// one key are published in order while N shards spread over many replicas.
// Run 2+ replicas per service for HA; more than N is useless.
//
// Why leases and not `FOR UPDATE SKIP LOCKED` on rows: SKIP LOCKED lets two
// replicas publish rows of the same saga concurrently -> reordering.
// See docs/adr/0002-outbox-relay-sharding.md.
type Relay struct {
	pool       *pgxpool.Pool
	client     *kgo.Client
	cfg        config.Relay
	instanceID string
	log        *slog.Logger
}

func NewRelay(pool *pgxpool.Pool, client *kgo.Client, cfg config.Relay, instanceID string, log *slog.Logger) *Relay {
	return &Relay{pool: pool, client: client, cfg: cfg, instanceID: instanceID, log: log}
}

// Run publishes until ctx is cancelled, then releases its leases.
//
// TODO(phase-2):
//
//	membership: upsert outbox_relay_member(instance_id, heartbeat_at=now()) every LeaseTTL/3;
//	            alive = heartbeat within LeaseTTL; fairShare = ceil(shards / alive)
//	leases:     renew own leases; if own < fairShare claim expired/free ones
//	            (UPDATE ... WHERE shard=$1 AND (owner=$me OR expires_at < now()));
//	            if own > fairShare release the extras (rebalance after scale-out)
//	publish:    per owned shard, in order:
//	              SELECT ... WHERE shard=$1 AND published_at IS NULL ORDER BY seq LIMIT BatchSize
//	              produce all rows (ProduceSync, headers from outbox.headers -> traceparent etc.,
//	              span "outbox publish" as child of the stored trace context)
//	              UPDATE outbox SET published_at=now() WHERE seq = ANY($ids)
//	            crash between produce and update => duplicates, never loss (inbox dedups)
//	            produce errors -> retry.Do(cfg.Retry); keep order: stop the shard at the first failure
//	wake-up:    poll every PollInterval (optional: LISTEN/NOTIFY from an outbox insert trigger)
//	janitor:    DELETE published rows older than Retention in small batches
//	metrics:    outbox_published_total, outbox_publish_duration, outbox_backlog (unpublished count),
//	            outbox_oldest_unpublished_age_seconds (the alert you actually want), relay_owned_shards
//	shutdown:   finish current batch, release leases (expires_at=now()) so another replica takes over at once
func (r *Relay) Run(ctx context.Context) error {
	return todo.Phase(2, "outbox relay")
}
