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


type Handler func(ctx context.Context, tx pgx.Tx, env message.Envelope) error

type Processor struct {
	pool       *pgxpool.Pool
	consumer   string 
	handle     Handler
	log        *slog.Logger
	tracer     trace.Tracer
	duplicates metric.Int64Counter
}


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

func (p *Processor) ProcessOne(ctx context.Context, rec *kgo.Record) error {
	return p.ProcessBatch(ctx, []*kgo.Record{rec})
}


func (p *Processor) InitMetrics(ctx context.Context) {
	p.duplicates.Add(ctx, 0, metric.WithAttributes(attribute.String("consumer_group", p.consumer)))
}


func (p *Processor) Healthy(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *Processor) apply(ctx context.Context, tx pgx.Tx, rec *kgo.Record) error {
	env, err := message.Unmarshal(rec.Value)
	if err != nil {
		return retry.Permanent(fmt.Errorf("%s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err))
	}

	
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


func producerContext(ctx context.Context, rec *kgo.Record) context.Context {
	if rec.Context != nil {
		if sc := trace.SpanContextFromContext(rec.Context); sc.IsValid() {
			return trace.ContextWithSpanContext(ctx, sc)
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, kotel.NewRecordCarrier(rec))
}
