// Package payment is the saga participant that charges the customer.
package payment

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// Config is the payment worker's business configuration.
type Config struct {
	LimitCents int64 `env:"PAYMENT_LIMIT_CENTS" envDefault:"100000"`
	Faults     faults.Config
}

// Service handles payment commands.
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
	case messaging.CmdRefund:
		return s.refund(ctx, tx, env)
	default:
		return retry.Permanent(fmt.Errorf("payment: unknown message type %q", env.Type))
	}
}

const (
	statusCharged   = "CHARGED"
	statusDeclined  = "DECLINED"
	statusRefunded  = "REFUNDED"
	statusCancelled = "CANCELLED"
)

const reasonLimitExceeded = "amount exceeds the payment limit"

type decision struct {
	status string
	reason string
}

func decide(amountCents, limitCents int64) decision {
	if amountCents > limitCents {
		return decision{status: statusDeclined, reason: reasonLimitExceeded}
	}
	return decision{status: statusCharged}
}

func (s *Service) charge(ctx context.Context, tx pgx.Tx, env message.Envelope, cmd messaging.Charge) error {
	if cmd.AmountCents <= 0 {
		return retry.Permanent(fmt.Errorf("payment: invalid amount %d for saga %s", cmd.AmountCents, env.SagaID))
	}

	d, err := recordPayment(ctx, tx, env.SagaID, cmd.AmountCents, decide(cmd.AmountCents, s.cfg.LimitCents))
	if err != nil {
		return err
	}

	replyType, reason := messaging.EvtPaymentCharged, d.reason
	if d.status != statusCharged {
		replyType = messaging.EvtPaymentFailed
		if reason == "" {
			reason = "payment " + strings.ToLower(d.status)
		}
	}
	s.log.InfoContext(ctx, "payment processed", "saga_id", env.SagaID, "status", d.status, "reason", reason)

	r, err := messaging.NewReply(replyType, env.SagaID, reason)
	if err != nil {
		return err
	}
	return outbox.Write(ctx, tx, messaging.TopicOrchestratorReplies, r)
}

func (s *Service) refund(ctx context.Context, tx pgx.Tx, env message.Envelope) error {
	outcome, err := refundPayment(ctx, tx, env.SagaID)
	if err != nil {
		return err
	}
	s.log.InfoContext(ctx, "payment refund processed", "saga_id", env.SagaID, "outcome", outcome)

	r, err := messaging.NewReply(messaging.EvtPaymentRefunded, env.SagaID, "")
	if err != nil {
		return err
	}
	return outbox.Write(ctx, tx, messaging.TopicOrchestratorReplies, r)
}
