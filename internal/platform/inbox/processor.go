// Package inbox implements idempotent consumption: every consumed message id
// is recorded in the `inbox` table in the same transaction as its effects, so
// a redelivered message (Kafka is at-least-once, the relay can duplicate) is
// detected and skipped.
package inbox

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

// Handler applies one message inside tx. It writes business state and any
// outgoing messages (outbox.Write) through the same tx. Return
// retry.Permanent(err) for errors that will never succeed (bad payload,
// unknown type) so the record goes to the DLQ without retries.
type Handler func(ctx context.Context, tx pgx.Tx, env messaging.Envelope) error

// Processor adapts a Handler to kafka.Processor.
type Processor struct {
	pool     *pgxpool.Pool
	consumer string // consumer group: part of the inbox key, so two groups on one DB don't collide
	handle   Handler
	log      *slog.Logger
}

func NewProcessor(pool *pgxpool.Pool, consumer string, h Handler, log *slog.Logger) *Processor {
	return &Processor{pool: pool, consumer: consumer, handle: h, log: log}
}

// ProcessBatch applies all records in ONE transaction.
//
// TODO(phase-3):
//
//	WithTx:
//	  for each rec:
//	    env, err := messaging.Unmarshal(rec.Value)       -> retry.Permanent on error
//	    extract trace ctx from rec.Headers, start span "process <type>" (link to the batch span)
//	    INSERT INTO inbox (consumer, message_id) ... ON CONFLICT DO NOTHING
//	    0 rows -> duplicate, skip (count saga_inbox_duplicates_total)
//	    handle(ctx, tx, env)
//
// One tx per batch = one fsync per batch instead of per message: this is
// where batching pays off.
func (p *Processor) ProcessBatch(ctx context.Context, recs []*kgo.Record) error {
	_ = recs
	return todo.Phase(3, "inbox batch processing")
}

// ProcessOne is ProcessBatch for a single record (poison isolation path).
func (p *Processor) ProcessOne(ctx context.Context, rec *kgo.Record) error {
	return p.ProcessBatch(ctx, []*kgo.Record{rec})
}
