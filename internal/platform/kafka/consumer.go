package kafka

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

// Processor applies records to the service's database. Implemented by
// inbox.Processor; kept as an interface so the consumer's batching / retry /
// DLQ logic is unit-testable with fakes and no broker.
type Processor interface {
	// ProcessBatch applies records of ONE partition, in offset order, in one
	// DB transaction. Any error means nothing was applied.
	ProcessBatch(ctx context.Context, recs []*kgo.Record) error
	// ProcessOne applies a single record in its own transaction. Used to find
	// and isolate the poison record after ProcessBatch failed.
	ProcessOne(ctx context.Context, rec *kgo.Record) error
}

// DeadLetterer parks a record that cannot be processed (see dlq.go).
type DeadLetterer interface {
	DeadLetter(ctx context.Context, rec *kgo.Record, cause error, attempts int) error
}

// Consumer is the batch consumer used by every worker.
//
// Scaling model: parallelism = partitions. Within a pod, partitions of one
// poll are processed concurrently (cfg.Concurrency); across pods, the
// consumer group spreads partitions. Ordering is preserved per partition and
// therefore per saga (key = saga_id).
type Consumer struct {
	client *kgo.Client
	proc   Processor
	dlq    DeadLetterer
	cfg    config.Consumer
	log    *slog.Logger
}

// NewConsumer builds a Consumer. client must be built with ConsumerOpts
// (manual commits, blocked rebalances); the loop relies on both.
func NewConsumer(client *kgo.Client, proc Processor, dlq DeadLetterer, cfg config.Consumer, log *slog.Logger) *Consumer {
	return &Consumer{client: client, proc: proc, dlq: dlq, cfg: cfg, log: log}
}

// Run polls until ctx is cancelled.
//
// TODO(phase-3) loop:
//  1. fetches := client.PollRecords(ctx, cfg.MaxPollRecords); handle fetches.Errors()
//  2. split by partition (fetches.EachPartition); run handlePartition for each
//     with an errgroup limited to cfg.Concurrency
//  3. if all partitions succeeded: client.CommitUncommittedOffsets(ctx)
//     (a DLQ'd record counts as handled — its offset IS committed)
//     if any failed fatally (DLQ unreachable, ctx cancelled): do NOT commit,
//     return the error -> process restarts, records are redelivered, inbox dedups
//  4. client.AllowRebalance() — rebalances wait for step 3 (BlockRebalanceOnPoll),
//     so a partition is never revoked mid-batch with uncommitted work
//  5. on ctx.Done(): finish the current poll, commit, client.Close() (leaves the group)
//
// Metrics: batch size histogram, batch duration, records processed/failed/
// dead-lettered counters, retries counter; consumer lag comes from kotel/kminion.
func (c *Consumer) Run(ctx context.Context) error {
	return todo.Phase(3, "batch consumer loop")
}

// handlePartition processes records of one partition.
//
// TODO(phase-3) algorithm ("batch with poison isolation"):
//  1. fast path: retry.Do(cfg.Retry, proc.ProcessBatch(recs))
//     -> ok: done, the whole batch was applied in one transaction
//  2. slow path (batch failed permanently or retries exhausted): for each rec in order
//     err := retry.Do(cfg.Retry, proc.ProcessOne(rec))
//     -> ok: continue
//     -> permanent or exhausted: dlq.DeadLetter(rec, err, attempts), continue
//  3. DeadLetter itself is retried with unlimited attempts until ctx is done:
//     if we can't park a record we must NOT skip it (that would lose data).
func (c *Consumer) handlePartition(ctx context.Context, recs []*kgo.Record) error {
	_ = recs
	return todo.Phase(3, "partition batch processing")
}
