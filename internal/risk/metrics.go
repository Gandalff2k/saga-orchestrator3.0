package risk

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
)

type metrics struct {
	decisions metric.Int64Counter
	ruleHits  metric.Int64Counter
}

func newMetrics() (*metrics, error) {
	meter := otel.Meter(telemetry.ScopeName)
	var (
		m    metrics
		errs []error
		err  error
	)
	m.decisions, err = meter.Int64Counter("risk_decisions_total",
		metric.WithDescription("Orders scored, by decision."))
	errs = append(errs, err)
	m.ruleHits, err = meter.Int64Counter("risk_rule_hits_total",
		metric.WithDescription("Rules that fired, by rule."))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("risk metrics: %w", err)
	}
	return &m, nil
}

func (m *metrics) decided(ctx context.Context, r Result) {
	m.decisions.Add(ctx, 1, metric.WithAttributes(attribute.String("decision", string(r.Decision))))
	for _, h := range r.Hits {
		m.ruleHits.Add(ctx, 1, metric.WithAttributes(attribute.String("rule", h.Rule)))
	}
}
