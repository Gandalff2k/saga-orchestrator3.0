package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
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
	// Healthy reports whether the processor's dependencies (the DB) work.
	// It decides between "this record is poison" and "we're in an outage".
	Healthy(ctx context.Context) error
}

// DeadLetterer parks a record that cannot be processed (see dlq.go).
type DeadLetterer interface {
	DeadLetter(ctx context.Context, rec *kgo.Record, cause error, attempts int) error
}

// groupOffsets is the part of *kgo.Client that settling a poll needs;
// an interface so commit/rewind decisions are unit-testable.
type groupOffsets interface {
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset)
}

// ErrDependencyDown is returned by Run when records keep failing while the
// processor's dependencies are unhealthy: nothing is dead-lettered and the
// failing records are not committed, so they are redelivered after restart.
var ErrDependencyDown = errors.New("dependency down")

// Consumer is the batch consumer used by every worker. See ADR 0003.
//
// Scaling model: parallelism = partitions. Within a pod, partitions of one
// poll are processed concurrently (cfg.Concurrency); across pods, the
// consumer group spreads partitions. Ordering is preserved per partition and
// therefore per saga (key = saga_id).
//
// Delivery: at-least-once. Only the prefix of each partition that was
// processed or dead-lettered is committed, after the fact; everything else is
// redelivered and deduplicated by the inbox.
type Consumer struct {
	client  *kgo.Client
	offsets groupOffsets // = client
	proc    Processor
	dlq     DeadLetterer
	cfg     config.Consumer
	log     *slog.Logger
	m       *consumerMetrics
	tracer  trace.Tracer

	mu    sync.Mutex
	hurry context.CancelFunc // cancels the current poll's processing; nil between polls
}

// NewConsumer builds a Consumer with its own Kafka client (consumer group +
// producer for the DLQ). It owns the client: Run closes it.
func NewConsumer(brokers []string, clientID string, cfg config.Consumer, proc Processor, log *slog.Logger) (*Consumer, error) {
	m, err := newConsumerMetrics(cfg.Group)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		proc:   proc,
		cfg:    cfg,
		log:    log.With("consumer_group", cfg.Group),
		m:      m,
		tracer: otel.Tracer(telemetry.ScopeName),
	}

	opts := append(ProducerOpts(), consumerOpts(cfg)...)
	opts = append(opts,
		// A rebalance is waiting for us (scale-out, a pod leaving): stop after
		// the current record, commit what's done and let it proceed, instead
		// of finishing the whole poll or exceeding the rebalance timeout.
		kgo.OnPartitionsCallbackBlocked(func(ctx context.Context, _ *kgo.Client) { c.hurryUp(ctx) }),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, p map[string][]int32) {
			c.log.Info("partitions revoked", "partitions", p)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, p map[string][]int32) {
			c.log.Warn("partitions lost (session expired); their uncommitted records will be redelivered", "partitions", p)
		}),
	)
	c.client, err = NewClient(brokers, clientID, opts...)
	if err != nil {
		return nil, err
	}
	c.offsets = c.client
	c.dlq = NewDLQProducer(c.client, cfg.Group)
	return c, nil
}

// Ping checks broker connectivity (used by /readyz).
func (c *Consumer) Ping(ctx context.Context) error { return c.client.Ping(ctx) }

// Run polls until ctx is cancelled (returns nil) or records can't be handled
// because a dependency is down (returns ErrDependencyDown). Either way the
// handled prefix of the last poll is committed and the client is closed,
// which leaves the group so partitions move to other members at once.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()

	for {
		fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			c.client.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.WarnContext(ctx, "fetch error", "topic", topic, "partition", partition, "err", err)
		})

		err := c.handlePoll(ctx, byPartition(fetches))
		c.client.AllowRebalance() // with BlockRebalanceOnPoll, ALWAYS after handling a poll
		if err != nil {
			return err
		}
	}
}

// handlePoll processes partitions concurrently, then settles each one:
// commit what was handled, rewind to what wasn't.
func (c *Consumer) handlePoll(ctx context.Context, parts [][]*kgo.Record) error {
	if len(parts) == 0 {
		return nil
	}

	pollCtx, cancel := context.WithCancel(ctx)
	c.setHurry(cancel)
	defer func() {
		c.setHurry(nil)
		cancel()
	}()

	handled := make([]int, len(parts))
	fatals := make([]error, len(parts))
	var g errgroup.Group
	g.SetLimit(max(1, c.cfg.Concurrency))
	for i, recs := range parts {
		g.Go(func() error {
			handled[i], fatals[i] = c.handlePartition(pollCtx, recs)
			return nil
		})
	}
	_ = g.Wait() // errors are collected per partition above

	c.settle(ctx, parts, handled)
	return errors.Join(fatals...)
}

// settle commits the handled prefix of every partition and rewinds the fetch
// position of every partition that was only partly handled.
//
// The rewind is not optional. The client's fetch position is already past
// everything it returned from the poll; if a partition was cut short (a
// rebalance asked us to hurry, shutdown, an outage) and we simply didn't
// commit the rest, the next poll would continue AFTER the unhandled records,
// and the next commit would move past them: silent loss. Called before
// AllowRebalance (revokes are blocked) and after the commit, never
// concurrently with it, as kgo requires for SetOffsets.
func (c *Consumer) settle(ctx context.Context, parts [][]*kgo.Record, handled []int) {
	var done []*kgo.Record
	rewind := map[string]map[int32]kgo.EpochOffset{}
	for i, recs := range parts {
		n := handled[i]
		if n > 0 {
			done = append(done, recs[n-1])
		}
		if n < len(recs) {
			next := recs[n]
			if rewind[next.Topic] == nil {
				rewind[next.Topic] = map[int32]kgo.EpochOffset{}
			}
			rewind[next.Topic][next.Partition] = kgo.EpochOffset{Epoch: next.LeaderEpoch, Offset: next.Offset}
		}
	}

	if len(done) > 0 {
		// Detached: on shutdown the handled work must still be committed,
		// otherwise it's all redelivered (harmless thanks to the inbox, but wasteful).
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer ccancel()
		if err := c.offsets.CommitRecords(cctx, done...); err != nil {
			// Not fatal: the records were applied; whoever owns the partition
			// next re-reads them and the inbox drops them.
			c.m.commitFailed(ctx)
			c.log.WarnContext(ctx, "commit failed; records will be redelivered", "err", err)
		}
	}
	if len(rewind) > 0 {
		c.offsets.SetOffsets(rewind)
		c.m.rewound(ctx)
		c.log.InfoContext(ctx, "partitions only partly handled, rewound to the first unhandled record", "offsets", rewind)
	}
}

// handlePartition processes the records of one partition in order and returns
// how many of them (a prefix) are handled — applied or dead-lettered — and so
// may be committed. A non-nil error means the consumer must stop (outage).
//
// Algorithm ("batch with poison isolation", ADR 0003):
//  1. fast path: the whole batch in one transaction, retried on transient errors;
//  2. slow path (batch failed): record by record, each retried; a record that
//     still fails is dead-lettered — unless the dependencies are unhealthy, in
//     which case it's an outage, not poison, and we stop without dead-lettering.
func (c *Consumer) handlePartition(ctx context.Context, recs []*kgo.Record) (handled int, err error) {
	topic := recs[0].Topic
	ctx, span := c.tracer.Start(ctx, topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(links(recs)...),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", topic),
			attribute.Int("messaging.destination.partition.id", int(recs[0].Partition)),
			attribute.Int("messaging.batch.message_count", len(recs)),
		))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	start := time.Now()
	defer func() { c.m.batch(ctx, topic, len(recs), time.Since(start)) }()

	// Work runs on a context detached from cancellation: a transaction that
	// has started is finished, not torn down. ctx (hurry/shutdown) is checked
	// between steps and stops retry backoffs.
	work := context.WithoutCancel(ctx)

	err = retry.Do(ctx, c.cfg.Retry, func(_ context.Context, attempt int) error {
		if attempt > 1 {
			c.m.retry(ctx, topic, "batch")
		}
		return c.proc.ProcessBatch(work, recs)
	})
	if err == nil {
		c.m.records(ctx, topic, "processed", len(recs))
		return len(recs), nil
	}
	if ctx.Err() != nil {
		return 0, nil // stopping; nothing applied, nothing committed
	}
	if !retry.IsPermanent(err) {
		if herr := c.proc.Healthy(work); herr != nil {
			return 0, fmt.Errorf("%w: %s[%d]: %w", ErrDependencyDown, topic, recs[0].Partition, errors.Join(err, herr))
		}
	}

	c.m.slow(ctx, topic)
	c.log.WarnContext(ctx, "batch failed, isolating records one by one",
		"topic", topic, "partition", recs[0].Partition, "records", len(recs), "err", err)

	for i, rec := range recs {
		if ctx.Err() != nil {
			return i, nil
		}
		attempts := 0
		err := retry.Do(ctx, c.cfg.Retry, func(_ context.Context, attempt int) error {
			attempts = attempt
			if attempt > 1 {
				c.m.retry(ctx, topic, "record")
			}
			return c.proc.ProcessOne(work, rec)
		})
		if err == nil {
			c.m.records(ctx, topic, "processed", 1)
			continue
		}
		if ctx.Err() != nil {
			return i, nil
		}

		if !retry.IsPermanent(err) {
			// Retries exhausted on a transient error. If the DB is down, every
			// record would end up here: dead-lettering them all would turn an
			// outage into a mass DLQ incident. Stop instead.
			if herr := c.proc.Healthy(work); herr != nil {
				return i, fmt.Errorf("%w: %s[%d]@%d: %w", ErrDependencyDown, rec.Topic, rec.Partition, rec.Offset,
					errors.Join(err, herr))
			}
		}

		if !c.deadLetter(ctx, rec, err, attempts) {
			return i, nil // stopping before the record was parked: it stays uncommitted
		}
		c.m.records(ctx, topic, "dead_lettered", 1)
	}
	return len(recs), nil
}

// deadLetter parks rec in its DLQ, retrying until it succeeds or ctx is done.
// A record we can't park must not be skipped, so there's no attempt limit.
func (c *Consumer) deadLetter(ctx context.Context, rec *kgo.Record, cause error, attempts int) bool {
	c.log.ErrorContext(ctx, "dead-lettering record", "topic", rec.Topic, "partition", rec.Partition,
		"offset", rec.Offset, "attempts", attempts, "err", cause)

	forever := c.cfg.Retry
	forever.MaxAttempts = 0
	err := retry.Do(ctx, forever, func(ctx context.Context, attempt int) error {
		err := c.dlq.DeadLetter(ctx, rec, cause, attempts)
		if err != nil && attempt%5 == 1 {
			c.log.WarnContext(ctx, "DLQ write failed, retrying", "attempt", attempt, "err", err)
		}
		return err
	})
	return err == nil
}

func (c *Consumer) setHurry(cancel context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hurry = cancel
}

func (c *Consumer) hurryUp(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hurry != nil {
		c.m.hurry(ctx)
		c.hurry()
	}
}

// byPartition groups a poll's records per topic-partition, preserving offset
// order. A poll can contain several fetches for the same partition, so
// EachPartition alone is not enough.
func byPartition(fetches kgo.Fetches) [][]*kgo.Record {
	type tp struct {
		topic     string
		partition int32
	}
	index := map[tp]int{}
	var out [][]*kgo.Record
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		k := tp{p.Topic, p.Partition}
		i, ok := index[k]
		if !ok {
			i = len(out)
			index[k] = i
			out = append(out, nil)
		}
		out[i] = append(out[i], p.Records...)
	})
	return out
}

// links connects the batch span to every record's producer trace (OTel
// batch-processing convention): one batch serves many traces.
func links(recs []*kgo.Record) []trace.Link {
	var out []trace.Link
	for _, r := range recs {
		if r.Context == nil {
			continue
		}
		if sc := trace.SpanContextFromContext(r.Context); sc.IsValid() {
			out = append(out, trace.Link{SpanContext: sc})
		}
	}
	return out
}
