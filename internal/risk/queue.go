package risk

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
)

// QueueMonitor reports the review queue: how many cases wait and how long the oldest has waited.
type QueueMonitor struct {
	q        db
	interval time.Duration
	log      *slog.Logger

	pending   atomic.Int64
	oldestSec atomic.Int64
	valid     atomic.Bool
}

// NewQueueMonitor registers the gauges; Run keeps them fresh.
func NewQueueMonitor(q db, interval time.Duration, log *slog.Logger) (*QueueMonitor, error) {
	m := &QueueMonitor{q: q, interval: interval, log: log}
	meter := otel.Meter(telemetry.ScopeName)
	pending, err := meter.Int64ObservableGauge("risk_review_pending",
		metric.WithDescription("Review cases waiting for a decision."))
	if err != nil {
		return nil, fmt.Errorf("risk queue metrics: %w", err)
	}
	oldest, err := meter.Int64ObservableGauge("risk_review_oldest_age_seconds", metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest review case waiting for a decision."))
	if err != nil {
		return nil, fmt.Errorf("risk queue metrics: %w", err)
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if m.valid.Load() {
			o.ObserveInt64(pending, m.pending.Load())
			o.ObserveInt64(oldest, m.oldestSec.Load())
		}
		return nil
	}, pending, oldest)
	if err != nil {
		return nil, fmt.Errorf("risk queue metrics callback: %w", err)
	}
	return m, nil
}

// Run refreshes the gauges every interval until ctx is cancelled.
func (m *QueueMonitor) Run(ctx context.Context) error {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		if err := m.refresh(ctx); err != nil && ctx.Err() == nil {
			m.log.WarnContext(ctx, "review queue stats failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (m *QueueMonitor) refresh(ctx context.Context) error {
	var n, oldest int64
	err := m.q.QueryRow(ctx,
		`SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::bigint
		 FROM review_cases WHERE status = 'PENDING'`).Scan(&n, &oldest)
	if err != nil {
		m.valid.Store(false)
		return err
	}
	m.pending.Store(n)
	m.oldestSec.Store(oldest)
	m.valid.Store(true)
	return nil
}
