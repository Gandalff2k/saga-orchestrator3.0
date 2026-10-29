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
	"github.com/Gandalff2k/saga3/internal/platform/retry"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
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

// charge records the payment and replies payment.charged / payment.failed.
//
// TODO(phase-4):
//
//	INSERT payments (saga_id, amount, status) ON CONFLICT (saga_id) DO NOTHING RETURNING status
//	  conflict -> re-read status and reply the same outcome again (idempotent)
//	amount > LimitCents -> status DECLINED, reply EvtPaymentFailed{reason:"limit exceeded"}
//	else                -> status CHARGED,  reply EvtPaymentCharged
//
// Note the difference: a DECLINED payment is a business outcome (reply, saga
// compensates); a DB outage is an infrastructure error (return err, retry).
func (s *Service) charge(ctx context.Context, tx pgx.Tx, env message.Envelope, cmd messaging.Charge) error {
	return todo.Phase(4, "payment charge")
}
