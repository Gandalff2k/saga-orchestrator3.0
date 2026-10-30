package kafka

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


type consumerMetrics struct {
	group          attribute.KeyValue
	recordsTotal   metric.Int64Counter
	retries        metric.Int64Counter
	slowPath       metric.Int64Counter
	hurries        metric.Int64Counter
	commitFailures metric.Int64Counter
	rewinds        metric.Int64Counter
	batchSize      metric.Int64Histogram
	batchDuration  metric.Float64Histogram
}

func newConsumerMetrics(group string) (*consumerMetrics, error) {
	meter := otel.Meter(telemetry.ScopeName)
	m := &consumerMetrics{group: attribute.String("consumer_group", group)}

	var errs []error
	var err error
	m.recordsTotal, err = meter.Int64Counter("consumer_records_total",
		metric.WithDescription("Records handled, by outcome: processed | dead_lettered."))
	errs = append(errs, err)
	m.retries, err = meter.Int64Counter("consumer_retries_total",
		metric.WithDescription("Retries after transient failures, by path: batch | record."))
	errs = append(errs, err)
	m.slowPath, err = meter.Int64Counter("consumer_slow_path_total",
		metric.WithDescription("Partition batches that failed and were re-processed record by record."))
	errs = append(errs, err)
	m.hurries, err = meter.Int64Counter("consumer_rebalance_hurry_total",
		metric.WithDescription("Polls cut short because a rebalance was waiting."))
	errs = append(errs, err)
	m.commitFailures, err = meter.Int64Counter("consumer_commit_failures_total",
		metric.WithDescription("Offset commits that failed (records will be redelivered and deduplicated)."))
	errs = append(errs, err)
	m.rewinds, err = meter.Int64Counter("consumer_rewinds_total",
		metric.WithDescription("Polls after which some partition was rewound to re-read unhandled records."))
	errs = append(errs, err)
	m.batchSize, err = meter.Int64Histogram("consumer_batch_size",
		metric.WithDescription("Records per partition batch."),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 25, 50, 100, 250, 500))
	errs = append(errs, err)
	m.batchDuration, err = meter.Float64Histogram("consumer_batch_duration_seconds", metric.WithUnit("s"),
		metric.WithDescription("Time to handle one partition batch, retries and DLQ included."))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("consumer metrics: %w", err)
	}
	return m, nil
}


func (m *consumerMetrics) zero(ctx context.Context, topics []string) {
	for _, topic := range topics {
		m.records(ctx, topic, "processed", 0)
		m.records(ctx, topic, "dead_lettered", 0)
		m.recordRetry(ctx, topic, "batch", 0)
		m.recordRetry(ctx, topic, "record", 0)
		m.slowPath.Add(ctx, 0, metric.WithAttributes(m.group, attribute.String("topic", topic)))
	}
	m.hurries.Add(ctx, 0, metric.WithAttributes(m.group))
	m.commitFailures.Add(ctx, 0, metric.WithAttributes(m.group))
	m.rewinds.Add(ctx, 0, metric.WithAttributes(m.group))
}

func (m *consumerMetrics) records(ctx context.Context, topic, outcome string, n int) {
	m.recordsTotal.Add(ctx, int64(n), metric.WithAttributes(m.group,
		attribute.String("topic", topic), attribute.String("outcome", outcome)))
}

func (m *consumerMetrics) retry(ctx context.Context, topic, path string) {
	m.recordRetry(ctx, topic, path, 1)
}

func (m *consumerMetrics) recordRetry(ctx context.Context, topic, path string, n int64) {
	m.retries.Add(ctx, n, metric.WithAttributes(m.group,
		attribute.String("topic", topic), attribute.String("path", path)))
}

func (m *consumerMetrics) batch(ctx context.Context, topic string, size int, took time.Duration) {
	attrs := metric.WithAttributes(m.group, attribute.String("topic", topic))
	m.batchSize.Record(ctx, int64(size), attrs)
	m.batchDuration.Record(ctx, took.Seconds(), attrs)
}

func (m *consumerMetrics) slow(ctx context.Context, topic string) {
	m.slowPath.Add(ctx, 1, metric.WithAttributes(m.group, attribute.String("topic", topic)))
}

func (m *consumerMetrics) hurry(ctx context.Context) {
	m.hurries.Add(ctx, 1, metric.WithAttributes(m.group))
}

func (m *consumerMetrics) commitFailed(ctx context.Context) {
	m.commitFailures.Add(ctx, 1, metric.WithAttributes(m.group))
}

func (m *consumerMetrics) rewound(ctx context.Context) {
	m.rewinds.Add(ctx, 1, metric.WithAttributes(m.group))
}
