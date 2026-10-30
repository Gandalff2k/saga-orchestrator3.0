package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
)

// Relay publishes outbox rows of ONE service database to Kafka.
type Relay struct {
	pool   *pgxpool.Pool
	client *kgo.Client
	cfg    config.Relay
	ns     kafka.Namespace
	leases leaseStore
	m      *metrics
	log    *slog.Logger

	mu    sync.Mutex
	owned map[int]*shardLease
}

type shardLease struct {
	deadline atomic.Int64

	mu       sync.Mutex
	released bool
	failures int
	retryAt  time.Time
}

func (l *shardLease) setDeadline(t time.Time) { l.deadline.Store(t.UnixNano()) }
func (l *shardLease) remaining() time.Duration {
	return time.Until(time.Unix(0, l.deadline.Load()))
}

// NewRelay builds a Relay.
func NewRelay(pool *pgxpool.Pool, client *kgo.Client, cfg config.Relay, ns kafka.Namespace, instanceID string, log *slog.Logger) (*Relay, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	id := instanceID + "-" + uuid.NewString()[:8]
	m, err := newMetrics()
	if err != nil {
		return nil, err
	}
	return &Relay{
		pool:   pool,
		client: client,
		cfg:    cfg,
		ns:     ns,
		leases: leaseStore{pool: pool, instance: id, ttl: cfg.LeaseTTL},
		m:      m,
		log:    log.With("relay_instance", id),
		owned:  map[int]*shardLease{},
	}, nil
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	total, err := r.leases.totalShards(ctx)
	if err != nil {
		return err
	}
	r.log.InfoContext(ctx, "outbox relay starting", "shards", total)
	r.m.failures.Add(ctx, 0)

	var wg sync.WaitGroup
	wg.Go(func() { r.leaseLoop(ctx, total) })
	wg.Go(func() { r.publishLoop(ctx) })
	wg.Go(func() { r.janitorLoop(ctx) })
	wg.Wait()

	r.shutdown(ctx)
	return nil
}

func (r *Relay) leaseLoop(ctx context.Context, total int) {
	t := time.NewTicker(r.cfg.LeaseTTL / 3)
	defer t.Stop()
	for {
		if err := r.rebalance(ctx, total); err != nil && ctx.Err() == nil {
			r.log.WarnContext(ctx, "lease rebalance failed", "err", err)
		}
		r.refreshStats(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r *Relay) rebalance(ctx context.Context, total int) error {
	alive, err := r.leases.heartbeat(ctx)
	if err != nil {
		return err
	}

	t0 := time.Now()
	renewed, err := r.leases.renew(ctx)
	if err != nil {
		return err
	}
	r.syncOwned(renewed, t0)

	keep, release, claim := plan(renewed, total, alive)
	if len(release) > 0 {
		r.drop(release)
		if err := r.leases.release(ctx, release); err != nil {
			return err
		}
		r.log.InfoContext(ctx, "shards released", "shards", release, "kept", len(keep), "alive", alive)
	}
	if claim > 0 {
		t0 = time.Now()
		got, err := r.leases.claim(ctx, claim)
		if err != nil {
			return err
		}
		if len(got) > 0 {
			r.add(got, t0)
			r.log.InfoContext(ctx, "shards claimed", "shards", got, "alive", alive)
		}
	}

	if err := r.leases.forgetDead(ctx); err != nil {
		r.log.WarnContext(ctx, "forget dead relay members", "err", err)
	}
	return nil
}

func (r *Relay) syncOwned(renewed []int, t0 time.Time) {
	r.mu.Lock()
	var lost []int
	for shard := range r.owned {
		if !slices.Contains(renewed, shard) {
			lost = append(lost, shard)
		}
	}
	r.mu.Unlock()

	if len(lost) > 0 {
		r.drop(lost)
		r.log.Warn("shard leases lost (expired before renewal)", "shards", lost)
	}
	r.add(renewed, t0)
}

func (r *Relay) add(shards []int, t0 time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range shards {
		l, ok := r.owned[s]
		if !ok {
			l = &shardLease{}
			r.owned[s] = l
		}
		l.setDeadline(t0.Add(r.cfg.LeaseTTL))
	}
	r.m.ownedShards.Store(int64(len(r.owned)))
}

func (r *Relay) drop(shards []int) {
	r.mu.Lock()
	var leases []*shardLease
	for _, s := range shards {
		if l, ok := r.owned[s]; ok {
			leases = append(leases, l)
			delete(r.owned, s)
		}
	}
	r.m.ownedShards.Store(int64(len(r.owned)))
	r.mu.Unlock()

	for _, l := range leases {
		l.mu.Lock()
		l.released = true
		l.mu.Unlock()
	}
}

func (r *Relay) ownedShards() map[int]*shardLease {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.owned)
}

func (r *Relay) shutdown(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	shards := slices.Collect(maps.Keys(r.ownedShards()))
	r.drop(shards)
	if err := errors.Join(r.leases.release(ctx, shards), r.leases.leave(ctx)); err != nil {
		r.log.WarnContext(ctx, "release leases on shutdown (they will expire instead)", "err", err)
		return
	}
	r.log.InfoContext(ctx, "outbox relay stopped, leases released", "shards", len(shards))
}

func (r *Relay) publishLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		var g errgroup.Group
		g.SetLimit(r.cfg.Concurrency)
		for shard, lease := range r.ownedShards() {
			g.Go(func() error {
				r.drainShard(ctx, shard, lease)
				return nil
			})
		}
		_ = g.Wait()
	}
}

func (r *Relay) drainShard(ctx context.Context, shard int, lease *shardLease) {
	lease.mu.Lock()
	defer lease.mu.Unlock()

	for ctx.Err() == nil && !lease.released {
		if time.Now().Before(lease.retryAt) {
			return
		}
		if lease.remaining() < r.cfg.LeaseTTL/3 {
			return
		}

		n, full, err := r.publishBatch(ctx, shard)
		if err != nil {
			lease.failures++
			backoff := r.cfg.Retry.Backoff(lease.failures)
			lease.retryAt = time.Now().Add(backoff)
			r.m.failures.Add(ctx, 1)
			r.log.WarnContext(ctx, "outbox batch failed", "shard", shard, "published", n,
				"failures", lease.failures, "retry_in", backoff, "err", err)
			return
		}
		lease.failures = 0
		if !full {
			return
		}
	}
}

type row struct {
	Seq     int64
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

func (r *Relay) publishBatch(ctx context.Context, shard int) (published int, full bool, err error) {
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.PublishTimeout)
	defer cancel()
	start := time.Now()

	rows, err := r.fetch(bctx, shard)
	if err != nil || len(rows) == 0 {
		return 0, false, err
	}

	recs := make([]*kgo.Record, len(rows))
	for i, rw := range rows {
		recs[i] = toRecord(bctx, rw, r.ns)
	}
	results := r.client.ProduceSync(bctx, recs...)

	seqs, produceErr := acknowledgedPrefix(rows, recs, results)
	if len(seqs) > 0 {
		if err := r.markPublished(bctx, seqs); err != nil {
			return 0, false, errors.Join(produceErr, err)
		}
	}
	r.m.recordBatch(ctx, rows[:len(seqs)], time.Since(start))
	return len(seqs), produceErr == nil && len(rows) == r.cfg.BatchSize, produceErr
}

func (r *Relay) fetch(ctx context.Context, shard int) ([]row, error) {
	rs, err := r.pool.Query(ctx,
		`SELECT seq, topic, key, payload, headers FROM outbox
		 WHERE shard = $1 AND published_at IS NULL
		 ORDER BY seq LIMIT $2`, shard, r.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("fetch shard %d: %w", shard, err)
	}
	defer rs.Close()

	var out []row
	for rs.Next() {
		var rw row
		if err := rs.Scan(&rw.Seq, &rw.Topic, &rw.Key, &rw.Payload, &rw.Headers); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		out = append(out, rw)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("fetch shard %d: %w", shard, err)
	}
	return out, nil
}

func (r *Relay) markPublished(ctx context.Context, seqs []int64) error {
	if _, err := r.pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE seq = ANY($1)`, seqs); err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	return nil
}

func toRecord(ctx context.Context, rw row, ns kafka.Namespace) *kgo.Record {
	keys := slices.Sorted(maps.Keys(rw.Headers))
	headers := make([]kgo.RecordHeader, 0, len(keys))
	for _, k := range keys {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(rw.Headers[k])})
	}
	return &kgo.Record{
		Topic:   ns.Topic(rw.Topic),
		Key:     []byte(rw.Key),
		Value:   rw.Payload,
		Headers: headers,
		Context: otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(rw.Headers)),
	}
}

func acknowledgedPrefix(rows []row, recs []*kgo.Record, results kgo.ProduceResults) ([]int64, error) {
	errByRec := make(map[*kgo.Record]error, len(results))
	for _, res := range results {
		errByRec[res.Record] = res.Err
	}

	seqs := make([]int64, 0, len(rows))
	for i, rw := range rows {
		if err, ok := errByRec[recs[i]]; !ok || err != nil {
			if err == nil {
				err = errors.New("no produce result for record")
			}
			return seqs, fmt.Errorf("produce %s: %w", rw.Topic, err)
		}
		seqs = append(seqs, rw.Seq)
	}
	return seqs, nil
}

const janitorShard = 0

func (r *Relay) ownsJanitor() bool {
	_, ok := r.ownedShards()[janitorShard]
	return ok
}

func (r *Relay) janitorLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.JanitorInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !r.ownsJanitor() {
			continue
		}
		n, err := r.deletePublished(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			r.log.WarnContext(ctx, "outbox janitor failed", "err", err, "deleted", n)
		case n > 0:
			r.log.InfoContext(ctx, "outbox janitor", "deleted", n)
		}
	}
}

func (r *Relay) deletePublished(ctx context.Context) (int64, error) {
	const chunk = 1000
	var total int64
	for ctx.Err() == nil {
		tag, err := r.pool.Exec(ctx,
			`DELETE FROM outbox WHERE seq IN (
			   SELECT seq FROM outbox
			   WHERE published_at < now() - make_interval(secs => $1)
			   LIMIT $2)`, r.cfg.Retention.Seconds(), chunk)
		if err != nil {
			return total, fmt.Errorf("delete published: %w", err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < chunk {
			break
		}
	}
	return total, nil
}

func (r *Relay) refreshStats(ctx context.Context) {
	if !r.ownsJanitor() {
		r.m.statsValid.Store(false)
		return
	}
	var (
		backlog int64
		oldest  float64
	)
	err := r.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8
		 FROM outbox WHERE published_at IS NULL`).Scan(&backlog, &oldest)
	if err != nil {
		r.m.statsValid.Store(false)
		return
	}
	r.m.backlog.Store(backlog)
	r.m.oldestAgeMillis.Store(int64(oldest * 1000))
	r.m.statsValid.Store(true)
}
