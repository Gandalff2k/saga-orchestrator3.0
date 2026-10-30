package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
)

type metrics struct {
	published metric.Int64Counter
	failures  metric.Int64Counter
	batchDur  metric.Float64Histogram

	ownedShards     atomic.Int64
	backlog         atomic.Int64
	oldestAgeMillis atomic.Int64
	statsValid      atomic.Bool
}

func newMetrics() (*metrics, error) {
	meter := otel.Meter(telemetry.ScopeName)
	m := &metrics{}

	var errs []error
	collect := func(err error) { errs = append(errs, err) }

	var err error
	m.published, err = meter.Int64Counter("outbox_published_total",
		metric.WithDescription("Outbox rows acknowledged by Kafka and marked published."))
	collect(err)
	m.failures, err = meter.Int64Counter("outbox_publish_failures_total",
		metric.WithDescription("Outbox batches that failed and will be retried with backoff."))
	collect(err)
	m.batchDur, err = meter.Float64Histogram("outbox_publish_batch_duration_seconds", metric.WithUnit("s"),
		metric.WithDescription("Fetch + produce + mark duration of one outbox batch."))
	collect(err)

	owned, err := meter.Int64ObservableGauge("outbox_relay_owned_shards",
		metric.WithDescription("Outbox shards leased by this relay replica."))
	collect(err)
	backlog, err := meter.Int64ObservableGauge("outbox_backlog",
		metric.WithDescription("Unpublished outbox rows (reported by the janitor replica only)."))
	collect(err)
	oldest, err := meter.Float64ObservableGauge("outbox_oldest_unpublished_age_seconds", metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest unpublished outbox row (reported by the janitor replica only)."))
	collect(err)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("outbox metrics: %w", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(owned, m.ownedShards.Load())
		if m.statsValid.Load() {
			o.ObserveInt64(backlog, m.backlog.Load())
			o.ObserveFloat64(oldest, float64(m.oldestAgeMillis.Load())/1000)
		}
		return nil
	}, owned, backlog, oldest)
	if err != nil {
		return nil, fmt.Errorf("outbox metrics callback: %w", err)
	}
	return m, nil
}

func (m *metrics) recordBatch(ctx context.Context, published []row, took time.Duration) {
	m.batchDur.Record(ctx, took.Seconds())
	perTopic := map[string]int64{}
	for _, rw := range published {
		perTopic[rw.Topic]++
	}
	for topic, n := range perTopic {
		m.published.Add(ctx, n, metric.WithAttributes(attribute.String("topic", topic)))
	}
}
