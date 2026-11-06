package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
	"github.com/Gandalff2k/saga3/internal/risk"
)

// DBEvidence reads evidence from the risk database.
type DBEvidence struct{ Pool *pgxpool.Pool }

// Case implements Evidence.
func (d DBEvidence) Case(ctx context.Context, id uuid.UUID) (risk.Case, error) {
	return risk.GetCase(ctx, d.Pool, id)
}

// History implements Evidence.
func (d DBEvidence) History(ctx context.Context, by, value string, exclude uuid.UUID, limit int) ([]risk.Event, error) {
	return risk.History(ctx, d.Pool, by, value, exclude, limit)
}

// RunnerConfig paces the agent.
type RunnerConfig struct {
	PollInterval time.Duration `env:"AGENT_POLL_INTERVAL" envDefault:"5s"`
	CaseTimeout  time.Duration `env:"AGENT_CASE_TIMEOUT" envDefault:"3m"`
	MaxAttempts  int           `env:"AGENT_MAX_ATTEMPTS" envDefault:"3"`
}

// Runner takes review cases one by one and stores a recommendation for each.
type Runner struct {
	pool  *pgxpool.Pool
	inv   *Investigator
	cfg   RunnerConfig
	owner string
	log   *slog.Logger

	cases    metric.Int64Counter
	tokens   metric.Int64Counter
	duration metric.Float64Histogram
}

// NewRunner builds a Runner; owner identifies this replica in case leases.
func NewRunner(pool *pgxpool.Pool, inv *Investigator, cfg RunnerConfig, owner string, log *slog.Logger) (*Runner, error) {
	meter := otel.Meter(telemetry.ScopeName)
	r := &Runner{pool: pool, inv: inv, cfg: cfg, owner: owner, log: log}
	var errs []error
	var err error
	r.cases, err = meter.Int64Counter("agent_cases_total",
		metric.WithDescription("Review cases the agent worked on, by outcome."))
	errs = append(errs, err)
	r.tokens, err = meter.Int64Counter("agent_tokens_total",
		metric.WithDescription("LLM tokens spent, by kind (prompt, completion)."))
	errs = append(errs, err)
	r.duration, err = meter.Float64Histogram("agent_case_duration_seconds", metric.WithUnit("s"),
		metric.WithDescription("Time to investigate one case."),
		metric.WithExplicitBucketBoundaries(1, 2.5, 5, 10, 20, 40, 80, 160))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("agent metrics: %w", err)
	}
	return r, nil
}

// Run works through the queue until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	for {
		worked, err := r.Once(ctx)
		if err != nil && ctx.Err() == nil {
			r.log.WarnContext(ctx, "agent iteration failed", "err", err)
		}
		if worked && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.PollInterval):
		}
	}
}

// Once handles at most one case and reports whether there was one.
func (r *Runner) Once(ctx context.Context) (bool, error) {
	lease := r.cfg.CaseTimeout + 30*time.Second
	id, ok, err := risk.ClaimCase(ctx, r.pool, r.owner, lease, r.cfg.MaxAttempts)
	if err != nil || !ok {
		return false, err
	}

	cctx, cancel := context.WithTimeout(ctx, r.cfg.CaseTimeout)
	start := time.Now()
	rec, err := r.inv.Investigate(cctx, id)
	cancel()
	r.duration.Record(ctx, time.Since(start).Seconds())
	r.tokens.Add(ctx, int64(rec.PromptTokens), metric.WithAttributes(attribute.String("kind", "prompt")))
	r.tokens.Add(ctx, int64(rec.CompletionTokens), metric.WithAttributes(attribute.String("kind", "completion")))

	if err != nil {
		r.count(ctx, "failed")
		r.log.WarnContext(ctx, "no recommendation, case left to the reviewer", "saga_id", id, "err", err)
		return true, risk.ReleaseCase(context.WithoutCancel(ctx), r.pool, id, r.owner)
	}
	if err := risk.SaveRecommendation(ctx, r.pool, id, r.owner, rec); err != nil {
		if errors.Is(err, risk.ErrLeaseLost) {
			r.count(ctx, "lease_lost")
			return true, nil
		}
		return true, err
	}
	r.count(ctx, rec.Action)
	r.log.InfoContext(ctx, "recommendation saved", "saga_id", id, "action", rec.Action, "confidence", rec.Confidence,
		"steps", rec.Steps, "tokens", rec.PromptTokens+rec.CompletionTokens)
	return true, nil
}

func (r *Runner) count(ctx context.Context, outcome string) {
	r.cases.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
