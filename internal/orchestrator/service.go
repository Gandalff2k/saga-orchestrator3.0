package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

var (
	ErrNotFound     = errors.New("order not found")
	ErrInvalidInput = errors.New("invalid input")
)

type Order struct {
	ID          uuid.UUID   `json:"order_id"`
	SagaID      uuid.UUID   `json:"saga_id"`
	SeatID      int         `json:"seat_id"`
	AmountCents int64       `json:"amount_cents"`
	Status      OrderStatus `json:"status"`
	SagaState   State       `json:"saga_state"`
	CreatedAt   time.Time   `json:"created_at"`
}

type PlaceOrder struct {
	SeatID         int
	AmountCents    int64
	IdempotencyKey string // optional; same key -> same order, no second saga
}

func (p PlaceOrder) Validate() error {
	switch {
	case p.SeatID <= 0:
		return fmt.Errorf("%w: seat_id must be positive", ErrInvalidInput)
	case p.AmountCents <= 0:
		return fmt.Errorf("%w: amount_cents must be positive", ErrInvalidInput)
	case len(p.IdempotencyKey) > 128:
		return fmt.Errorf("%w: idempotency key too long", ErrInvalidInput)
	}
	return nil
}

type Service struct {
	pool        *pgxpool.Pool
	stepTimeout time.Duration
	log         *slog.Logger
}

func NewService(pool *pgxpool.Pool, stepTimeout time.Duration, log *slog.Logger) *Service {
	return &Service{pool: pool, stepTimeout: stepTimeout, log: log}
}

// PlaceOrder creates the order and starts its saga. created=false means the
// idempotency key was seen before and the existing order is returned.
//
// TODO(phase-1): one transaction (postgres.WithTx):
//
//	INSERT orders ... ON CONFLICT (idempotency_key) DO NOTHING
//	  -> conflict: SELECT the existing order, return created=false
//	saga, cmd := Start(...); INSERT sagas (deadline_at = now() + stepTimeout)
//	outbox.Write(tx, cmd.Topic, saga.ID, NewEnvelope(cmd.Type, saga.ID, cmd.Payload))
//
// Order row + saga row + first command commit atomically; the API never talks to Kafka.
func (s *Service) PlaceOrder(ctx context.Context, in PlaceOrder) (order Order, created bool, err error) {
	if err := in.Validate(); err != nil {
		return Order{}, false, err
	}
	return Order{}, false, todo.Phase(1, "place order")
}

// GetOrder returns the order with its saga state.
//
// TODO(phase-1): SELECT orders JOIN sagas; pgx.ErrNoRows -> ErrNotFound.
func (s *Service) GetOrder(ctx context.Context, id uuid.UUID) (Order, error) {
	return Order{}, todo.Phase(1, "get order")
}

// HandleReply is the inbox.Handler for orchestrator.replies.
//
// TODO(phase-4), inside the inbox tx:
//
//	SELECT ... FROM sagas WHERE saga_id = env.SagaID FOR UPDATE
//	  no row -> retry.Permanent (reply for an unknown saga = bug or foreign data -> DLQ)
//	tr := saga.On(env.Type)
//	tr.Ignored -> log at debug, saga_replies_ignored_total++, return nil
//	UPDATE sagas SET state, version+1, deadline_at; UPDATE orders SET status if tr.Order != ""
//	for each tr.Emit: outbox.Write(...)
//	metrics: saga_transitions_total{from,to}; on terminal state saga_duration_seconds
func (s *Service) HandleReply(ctx context.Context, tx pgx.Tx, env messaging.Envelope) error {
	return todo.Phase(4, "orchestrator reply handler")
}
