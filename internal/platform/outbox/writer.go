// Package outbox implements the transactional outbox.
//
// Write: services NEVER produce to Kafka directly. They insert the message into
// their own `outbox` table in the same transaction as the business change, so
// "state changed" and "message will be sent" are atomic.
//
// Relay: a separate, horizontally scalable process reads unpublished rows and
// produces them to Kafka (at-least-once; consumers dedup via the inbox).
package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

// Write appends env to the outbox inside tx. key is the Kafka record key —
// always the saga id, which also determines the outbox shard (see
// migrations/platform) and therefore per-saga publish order.
//
// TODO(phase-1):
//   - inject the current trace context into a map carrier
//     (otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier{}))
//     and store it in outbox.headers together with message-id / message-type,
//     so the trace continues API -> outbox -> relay -> Kafka -> consumer
//   - INSERT INTO outbox (id, topic, key, type, payload, headers) VALUES (...)
//     (shard is a generated column, don't set it)
func Write(ctx context.Context, tx pgx.Tx, topic, key string, env messaging.Envelope) error {
	_, _, _, _ = tx, topic, key, env
	return todo.Phase(1, "outbox write")
}
