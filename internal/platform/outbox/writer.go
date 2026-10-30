
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


func Headers(ctx context.Context, env message.Envelope) map[string]string {
	carrier := propagation.MapCarrier{
		message.HeaderID:   env.ID.String(),
		message.HeaderType: env.Type,
	}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}
