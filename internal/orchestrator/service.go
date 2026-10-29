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

	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

// Errors the HTTP layer maps to status codes (404, 400, 422).
var (
	ErrNotFound     = errors.New("order not found")
	ErrInvalidInput = errors.New("invalid input")
	// ErrIdempotencyKeyReused: the key belongs to an order with a different body.
	ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different request")
)

// Order is the read model returned by the API: the order plus its saga state.
type Order struct {
	ID          uuid.UUID   `json:"order_id"`
	SagaID      uuid.UUID   `json:"saga_id"`
	SeatID      int         `json:"seat_id"`
	AmountCents int64       `json:"amount_cents"`
	Status      OrderStatus `json:"status"`
	SagaState   State       `json:"saga_state"`
	CreatedAt   time.Time   `json:"created_at"`
}

// PlaceOrder is the input of Service.PlaceOrder.
type PlaceOrder struct {
	SeatID         int
	AmountCents    int64
	IdempotencyKey string // optional; same key -> same order, no second saga
}

// Validate rejects input that must never reach the database. Errors wrap ErrInvalidInput.
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

// Service holds the orchestrator use cases: the API side (PlaceOrder,
// GetOrder) and the worker side (HandleReply).
type Service struct {
	pool        *pgxpool.Pool
	stepTimeout time.Duration
	log         *slog.Logger
}

// NewService builds the service. pool may be nil in the worker, which only
// uses HandleReply (it runs inside the inbox tx). stepTimeout sets how long a
// saga may wait for one reply before the sweeper compensates (phase 5).
func NewService(pool *pgxpool.Pool, stepTimeout time.Duration, log *slog.Logger) *Service {
	return &Service{pool: pool, stepTimeout: stepTimeout, log: log}
}

// PlaceOrder creates the order and starts its saga. created=false means the
// idempotency key was seen before and the existing order is returned.
//
// Order row + saga row + first command commit in ONE transaction; the API
// never talks to Kafka. If the process dies before commit, nothing happened;
// after commit, the relay will deliver the command eventually.
func (s *Service) PlaceOrder(ctx context.Context, in PlaceOrder) (Order, bool, error) {
	if err := in.Validate(); err != nil {
		return Order{}, false, err
	}
	orderID, err := uuid.NewV7()
	if err != nil {
		return Order{}, false, fmt.Errorf("order id: %w", err)
	}
	sagaID, err := uuid.NewV7()
	if err != nil {
		return Order{}, false, fmt.Errorf("saga id: %w", err)
	}

	var (
		order   Order
		created bool
	)
	err = postgres.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		createdAt, inserted, err := insertOrder(ctx, tx, orderID, in)
		if err != nil {
			return err
		}
		if !inserted {
			order, err = replay(ctx, tx, in)
			return err
		}

		saga, cmd := Start(sagaID, orderID, in.SeatID, in.AmountCents)
		if err := insertSaga(ctx, tx, saga, s.stepTimeout); err != nil {
			return err
		}
		env, err := message.New(cmd.Type, saga.ID, cmd.Payload)
		if err != nil {
			return err
		}
		if err := outbox.Write(ctx, tx, cmd.Topic, env); err != nil {
			return err
		}

		order = Order{
			ID: orderID, SagaID: sagaID, SeatID: in.SeatID, AmountCents: in.AmountCents,
			Status: OrderPending, SagaState: saga.State, CreatedAt: createdAt,
		}
		created = true
		return nil
	})
	if err != nil {
		return Order{}, false, err
	}

	if created {
		s.log.InfoContext(ctx, "order placed", "order_id", order.ID, "saga_id", order.SagaID)
	}
	return order, created, nil
}

// replay returns the order an idempotency key already produced. Reusing a key
// for a DIFFERENT request is a client bug; silently returning the old order
// would hide it, so it's an error.
func replay(ctx context.Context, tx pgx.Tx, in PlaceOrder) (Order, error) {
	existing, err := orderByIdempotencyKey(ctx, tx, in.IdempotencyKey)
	if err != nil {
		return Order{}, err
	}
	if existing.SeatID != in.SeatID || existing.AmountCents != in.AmountCents {
		return Order{}, fmt.Errorf("%w: key %q", ErrIdempotencyKeyReused, in.IdempotencyKey)
	}
	return existing, nil
}

// GetOrder returns the order with its saga state.
func (s *Service) GetOrder(ctx context.Context, id uuid.UUID) (Order, error) {
	return orderByID(ctx, s.pool, id)
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
func (s *Service) HandleReply(ctx context.Context, tx pgx.Tx, env message.Envelope) error {
	return todo.Phase(4, "orchestrator reply handler")
}
