// Package payment is the saga participant that charges the customer.
package payment

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// Config is the payment worker's business configuration.
type Config struct {
	// LimitCents: charges above this are declined (business failure ->
	// payment.failed -> saga compensates). Lets you trigger compensation from curl.
	LimitCents int64 `env:"PAYMENT_LIMIT_CENTS" envDefault:"100000"`
	Faults     faults.Config
}

// Service handles payment commands. It is stateless: all state is in the DB
// and reached through the inbox tx passed to Handle.
type Service struct {
	cfg Config
	log *slog.Logger
}

// NewService builds the service.
func NewService(cfg Config, log *slog.Logger) *Service {
	return &Service{cfg: cfg, log: log}
}

// Handle is the inbox.Handler for payment.commands.
func (s *Service) Handle(ctx context.Context, tx pgx.Tx, env message.Envelope) error {
	if err := s.cfg.Faults.Maybe(env.Type); err != nil {
		return err
	}
	switch env.Type {
	case messaging.CmdCharge:
		var cmd messaging.Charge
		if err := env.DecodePayload(&cmd); err != nil {
			return retry.Permanent(err)
		}
		return s.charge(ctx, tx, env, cmd)
	default:
		return retry.Permanent(fmt.Errorf("payment: unknown message type %q", env.Type))
	}
}

// Payment statuses (payments.status).
const (
	statusCharged  = "CHARGED"
	statusDeclined = "DECLINED"
)

const reasonLimitExceeded = "amount exceeds the payment limit"

// decision is the outcome of a charge: a status and, when declined, why.
type decision struct {
	status string
	reason string
}

// decide is the payment rule, pure so it's trivially testable. A real
// provider call would go here; note it must then be idempotent on the saga id
// (most PSPs accept an idempotency key), because this handler can run again.
func decide(amountCents, limitCents int64) decision {
	if amountCents > limitCents {
		return decision{status: statusDeclined, reason: reasonLimitExceeded}
	}
	return decision{status: statusCharged}
}

// charge records the payment and replies payment.charged or payment.failed.
//
// A DECLINED payment is a business outcome (a reply; the saga compensates).
// A DB error is infrastructure (returned; retried). A non-positive amount
// can't come from a valid orchestrator: it's a contract violation and goes to
// the DLQ instead of being "declined" and hidden.
func (s *Service) charge(ctx context.Context, tx pgx.Tx, env message.Envelope, cmd messaging.Charge) error {
	if cmd.AmountCents <= 0 {
		return retry.Permanent(fmt.Errorf("payment: invalid amount %d for saga %s", cmd.AmountCents, env.SagaID))
	}

	d, err := recordPayment(ctx, tx, env.SagaID, cmd.AmountCents, decide(cmd.AmountCents, s.cfg.LimitCents))
	if err != nil {
		return err
	}

	replyType := messaging.EvtPaymentCharged
	if d.status == statusDeclined {
		replyType = messaging.EvtPaymentFailed
	}
	s.log.InfoContext(ctx, "payment processed", "saga_id", env.SagaID, "status", d.status, "reason", d.reason)

	r, err := messaging.NewReply(replyType, env.SagaID, d.reason)
	if err != nil {
		return err
	}
	return outbox.Write(ctx, tx, messaging.TopicOrchestratorReplies, r)
}
