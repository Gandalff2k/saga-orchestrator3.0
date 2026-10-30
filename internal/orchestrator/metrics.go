package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
)

type sagaMetrics struct {
	transitions    metric.Int64Counter
	ignoredReplies metric.Int64Counter
	timeouts       metric.Int64Counter
	duration       metric.Float64Histogram
}

func newSagaMetrics() (*sagaMetrics, error) {
	meter := otel.Meter(telemetry.ScopeName)
	var (
		m    sagaMetrics
		errs []error
		err  error
	)
	m.transitions, err = meter.Int64Counter("saga_transitions_total",
		metric.WithDescription("Saga state transitions, by from/to state."))
	errs = append(errs, err)
	m.ignoredReplies, err = meter.Int64Counter("saga_replies_ignored_total",
		metric.WithDescription("Replies that didn't apply to the saga's state (late or duplicated)."))
	errs = append(errs, err)
	m.timeouts, err = meter.Int64Counter("saga_timeouts_total",
		metric.WithDescription("Sagas whose step deadline passed, by the state they were stuck in."))
	errs = append(errs, err)
	m.duration, err = meter.Float64Histogram("saga_duration_seconds", metric.WithUnit("s"),
		metric.WithDescription("Time from order placement to a terminal saga state, by outcome."),
		metric.WithExplicitBucketBoundaries(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("saga metrics: %w", err)
	}
	return &m, nil
}

func (m *sagaMetrics) transition(ctx context.Context, from, to State, started time.Time) {
	m.transitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("from", string(from)), attribute.String("to", string(to))))
	if to.Terminal() {
		m.duration.Record(ctx, time.Since(started).Seconds(),
			metric.WithAttributes(attribute.String("outcome", string(to))))
	}
}

func (m *sagaMetrics) ignored(ctx context.Context, replyType string) {
	m.ignoredReplies.Add(ctx, 1, metric.WithAttributes(attribute.String("type", replyType)))
}

func (m *sagaMetrics) zeroTimeouts(ctx context.Context) {
	for _, s := range []State{StateReserving, StateCharging, StateRefunding, StateCompensating} {
		m.timeouts.Add(ctx, 0, metric.WithAttributes(attribute.String("state", string(s))))
	}
}

func (m *sagaMetrics) timedOut(ctx context.Context, state State) {
	m.timeouts.Add(ctx, 1, metric.WithAttributes(attribute.String("state", string(state))))
}
