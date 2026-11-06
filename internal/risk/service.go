package risk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// Reasons sent back to the orchestrator on rejection.
const (
	ReasonBlockedPrefix  = "blocked by risk rules: "
	ReasonReviewRejected = "rejected in manual review"
)

// Service handles risk commands.
type Service struct {
	cfg    Config
	faults faults.Config
	log    *slog.Logger
	m      *metrics
}

// NewService builds the service.
func NewService(cfg Config, f faults.Config, log *slog.Logger) (*Service, error) {
	if cfg.ReviewScore <= 0 || cfg.BlockScore <= cfg.ReviewScore || cfg.ReviewLimitCents <= 0 {
		return nil, errors.New("risk: need 0 < RISK_REVIEW_SCORE < RISK_BLOCK_SCORE and RISK_REVIEW_LIMIT_CENTS > 0")
	}
	m, err := newMetrics()
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, faults: f, log: log, m: m}, nil
}

// Handle is the inbox.Handler for risk.commands.
func (s *Service) Handle(ctx context.Context, tx pgx.Tx, env message.Envelope) error {
	if err := s.faults.Maybe(env.Type); err != nil {
		return err
	}
	switch env.Type {
	case messaging.CmdScore:
		var cmd messaging.Score
		if err := env.DecodePayload(&cmd); err != nil {
			return retry.Permanent(err)
		}
		if err := validate(cmd); err != nil {
			return retry.Permanent(fmt.Errorf("risk: saga %s: %w", env.SagaID, err))
		}
		return s.score(ctx, tx, env, cmd)
	case messaging.CmdCancelReview:
		expired, err := expireCase(ctx, tx, env.SagaID)
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "review cancelled by saga", "saga_id", env.SagaID, "expired", expired)
		return nil
	default:
		return retry.Permanent(fmt.Errorf("risk: unknown message type %q", env.Type))
	}
}

func validate(c messaging.Score) error {
	switch {
	case c.AmountCents <= 0:
		return errors.New("amount must be positive")
	case c.CustomerID == "" || c.CardHash == "" || c.IP == "" || len(c.Country) != 2:
		return errors.New("customer, card, ip and country are required")
	}
	return nil
}

func (s *Service) score(ctx context.Context, tx pgx.Tx, env message.Envelope, cmd messaging.Score) error {
	prev, found, err := storedDecision(ctx, tx, env.SagaID)
	if err != nil {
		return err
	}
	if found {
		s.log.InfoContext(ctx, "risk decision replayed", "saga_id", env.SagaID, "decision", prev.result.Decision)
		return reply(ctx, tx, env, prev.result, prev.caseStatus)
	}

	if err := lockKeys(ctx, tx, "card:"+cmd.CardHash, "customer:"+cmd.CustomerID, "ip:"+cmd.IP); err != nil {
		return err
	}
	if err := insertEvent(ctx, tx, env.SagaID, cmd); err != nil {
		return err
	}
	f, err := features(ctx, tx, env.SagaID, cmd)
	if err != nil {
		return err
	}
	r := Evaluate(cmd.AmountCents, f, s.cfg)
	if err := insertDecision(ctx, tx, env.SagaID, r); err != nil {
		return err
	}

	s.m.decided(ctx, r)
	s.log.InfoContext(ctx, "order scored", "saga_id", env.SagaID, "decision", r.Decision, "score", r.Score,
		"rules", r.RuleNames())
	return reply(ctx, tx, env, r, "PENDING")
}

func reply(ctx context.Context, tx pgx.Tx, env message.Envelope, r Result, caseStatus string) error {
	typ, reason := messaging.EvtRiskApproved, ""
	switch {
	case r.Decision == Block:
		typ, reason = messaging.EvtRiskRejected, ReasonBlockedPrefix+r.RuleNames()
	case r.Decision == Review && caseStatus == "PENDING":
		typ = messaging.EvtRiskReviewRequired
	case r.Decision == Review && caseStatus != "APPROVED":
		typ, reason = messaging.EvtRiskRejected, ReasonReviewRejected
	}
	out, err := messaging.NewReply(typ, env.SagaID, reason)
	if err != nil {
		return err
	}
	return outbox.Write(ctx, tx, messaging.TopicOrchestratorReplies, out)
}
