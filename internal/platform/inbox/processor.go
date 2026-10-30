// Package inbox implements idempotent consumption: every consumed message id
// is recorded in the `inbox` table in the same transaction as its effects, so
// a redelivered message (Kafka is at-least-once, the relay can duplicate) is
// detected and skipped.
package inbox

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
)

// Handler applies one message inside tx. It writes business state and any
// outgoing messages (outbox.Write) through the same tx. Return
// retry.Permanent(err) for errors that will never succeed (bad payload,
// unknown type) so the record goes to the DLQ without retries.
type Handler func(ctx context.Context, tx pgx.Tx, env message.Envelope) error

// Processor adapts a Handler to kafka.Processor.
type Processor struct {
	pool       *pgxpool.Pool
	consumer   string // consumer group: part of the inbox key, so two groups on one DB don't collide
	handle     Handler
	log        *slog.Logger
	tracer     trace.Tracer
	duplicates metric.Int64Counter
}

// NewProcessor builds a Processor. consumer must be the consumer group name:
// it scopes inbox dedup, so changing it makes old messages look new.
func NewProcessor(pool *pgxpool.Pool, consumer string, h Handler, log *slog.Logger) (*Processor, error) {
	dups, err := otel.Meter(telemetry.ScopeName).Int64Counter("inbox_duplicates_total",
		metric.WithDescription("Messages skipped because their id was already in the inbox (redeliveries)."))
	if err != nil {
		return nil, fmt.Errorf("inbox metrics: %w", err)
	}
	return &Processor{
		pool: pool, consumer: consumer, handle: h, log: log,
		tracer:     otel.Tracer(telemetry.ScopeName),
		duplicates: dups,
	}, nil
}

// ProcessBatch applies all records in ONE transaction: one commit (one WAL
// fsync) per batch instead of per message — this is where batching pays off.
// If any record fails, nothing is applied and the consumer falls back to
// ProcessOne to find the culprit.
func (p *Processor) ProcessBatch(ctx context.Context, recs []*kgo.Record) error {
	return postgres.WithTx(ctx, p.pool, func(tx pgx.Tx) error {
		for _, rec := range recs {
			if err := p.apply(ctx, tx, rec); err != nil {
				return err
			}
		}
		return nil
	})
}

// ProcessOne is ProcessBatch for a single record (poison isolation path).
func (p *Processor) ProcessOne(ctx context.Context, rec *kgo.Record) error {
	return p.ProcessBatch(ctx, []*kgo.Record{rec})
}

// Healthy pings the database: the consumer uses it to tell poison records
// from an outage.
func (p *Processor) Healthy(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *Processor) apply(ctx context.Context, tx pgx.Tx, rec *kgo.Record) error {
	env, err := message.Unmarshal(rec.Value)
	if err != nil {
		return retry.Permanent(fmt.Errorf("%s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err))
	}

	// The span is a child of the PRODUCER's trace (the original HTTP request),
	// while ctx keeps the batch's cancellation and DB transaction.
	ctx, span := p.tracer.Start(producerContext(ctx, rec), "process "+env.Type,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.message.id", env.ID.String()),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("saga.id", env.SagaID.String()),
		))
	defer span.End()

	tag, err := tx.Exec(ctx,
		`INSERT INTO inbox (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		p.consumer, env.ID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("inbox insert: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Seen before (relay re-send, redelivery after a rebalance or crash,
		// DLQ replay of something that did succeed). Effects already exist.
		span.SetAttributes(attribute.Bool("inbox.duplicate", true))
		p.duplicates.Add(ctx, 1, metric.WithAttributes(attribute.String("consumer_group", p.consumer)))
		return nil
	}

	if err := p.handle(ctx, tx, env); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("handle %s %s: %w", env.Type, env.ID, err)
	}
	return nil
}

// producerContext returns ctx with the record's trace as parent: kotel's
// "receive" span if the client is instrumented, otherwise the traceparent
// header straight from the record.
func producerContext(ctx context.Context, rec *kgo.Record) context.Context {
	if rec.Context != nil {
		if sc := trace.SpanContextFromContext(rec.Context); sc.IsValid() {
			return trace.ContextWithSpanContext(ctx, sc)
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, kotel.NewRecordCarrier(rec))
}
