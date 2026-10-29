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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/Gandalff2k/saga3/internal/platform/message"
)

// Write appends env to topic's outbox inside tx.
//
// It takes a pgx.Tx, not a pool: an outbox row written outside the business
// transaction would defeat the pattern, so the type system forbids it.
//
// The Kafka key is always env.SagaID (not a parameter, so it can't be gotten
// wrong). The key also determines the outbox shard (generated column, see
// migrations/platform) and therefore per-saga publish order.
func Write(ctx context.Context, tx pgx.Tx, topic string, env message.Envelope) error {
	if topic == "" || env.ID == uuid.Nil || env.SagaID == uuid.Nil || env.Type == "" {
		return errors.New("outbox: topic, message id, saga id and type are required")
	}

	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal envelope: %w", err)
	}
	headers, err := json.Marshal(Headers(ctx, env))
	if err != nil {
		return fmt.Errorf("outbox: marshal headers: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, topic, key, type, payload, headers)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		env.ID, topic, env.SagaID.String(), env.Type, payload, headers)
	if err != nil {
		return fmt.Errorf("outbox: insert %s: %w", env.Type, err)
	}
	return nil
}

// Headers returns the Kafka headers stored with the row: message id/type plus
// the CURRENT trace context. The relay publishes them as-is, so the consumer's
// span becomes a child of the span that wrote the outbox row (e.g. the HTTP
// request), not of the relay's polling loop: one order = one trace.
func Headers(ctx context.Context, env message.Envelope) map[string]string {
	carrier := propagation.MapCarrier{
		message.HeaderID:   env.ID.String(),
		message.HeaderType: env.Type,
	}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}
