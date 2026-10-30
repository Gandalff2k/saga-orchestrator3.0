//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/inbox"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// Consumer tests run the real batch consumer + inbox against Kafka and
// Postgres with a test handler that records each applied message in
// it_effects (n = global application order).

const (
	typeStep   = "test.step.v1"
	typeReject = "test.reject.v1" // the handler rejects it permanently
)

type step struct {
	Step int `json:"step"`
}

func setupEffects(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	mustExec(t, pool, `CREATE TABLE it_effects (
		message_id uuid PRIMARY KEY,  -- a duplicate effect would violate it
		saga_id    uuid NOT NULL,
		step       int  NOT NULL,
		n          bigserial)`)
}

// effectsHandler applies typeStep, rejects typeReject. fail(id) may inject an error.
func effectsHandler(fail func(message.Envelope) error) inbox.Handler {
	return func(ctx context.Context, tx pgx.Tx, env message.Envelope) error {
		if fail != nil {
			if err := fail(env); err != nil {
				return err
			}
		}
		switch env.Type {
		case typeStep:
			var p step
			if err := env.DecodePayload(&p); err != nil {
				return retry.Permanent(err)
			}
			_, err := tx.Exec(ctx, `INSERT INTO it_effects (message_id, saga_id, step) VALUES ($1, $2, $3)`,
				env.ID, env.SagaID, p.Step)
			return err
		default:
			return retry.Permanent(fmt.Errorf("unknown type %q", env.Type))
		}
	}
}

func consumerCfg(group, topic string) config.Consumer {
	return config.Consumer{
		Group: group, Topics: []string{topic},
		MaxPollRecords: 100, Concurrency: 4,
		Retry: retry.Policy{MaxAttempts: 3, Initial: 20 * time.Millisecond, Max: 200 * time.Millisecond, Multiplier: 2},
	}
}

// runningConsumer is a consumer started in the background.
type runningConsumer struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returned; err is set before that
	err    error
}

// stop cancels the consumer and returns Run's error. Safe to call many times
// and after the consumer already stopped on its own.
func (r *runningConsumer) stop() error {
	r.cancel()
	<-r.done
	return r.err
}

func startConsumer(t *testing.T, pool *pgxpool.Pool, cfg config.Consumer, h inbox.Handler) *runningConsumer {
	t.Helper()
	proc, err := inbox.NewProcessor(pool, cfg.Group, h, env.Log)
	if err != nil {
		t.Fatal(err)
	}
	c, err := kafka.NewConsumer(env.Brokers, "it-"+cfg.Group, cfg, proc, env.Log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningConsumer{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.err = c.Run(ctx)
	}()
	t.Cleanup(func() { _ = r.stop() })
	return r
}

type produced struct {
	env   message.Envelope
	value []byte
}

// produce sends steps 0..perSaga-1 for each saga, keyed by saga id, in order.
func produce(t *testing.T, topic string, sagas, perSaga int) []produced {
	t.Helper()
	var out []produced
	ids := make([]uuid.UUID, sagas)
	for i := range ids {
		ids[i] = uuid.New()
	}
	for s := range perSaga {
		for _, saga := range ids {
			e, err := message.New(typeStep, saga, step{Step: s})
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(e)
			out = append(out, produced{env: e, value: raw})
		}
	}
	send(t, topic, out...)
	return out
}

func send(t *testing.T, topic string, msgs ...produced) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		recs[i] = &kgo.Record{Topic: topic, Key: []byte(m.env.SagaID.String()), Value: m.value,
			Headers: []kgo.RecordHeader{{Key: message.HeaderID, Value: []byte(m.env.ID.String())}}}
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// lag = end offsets - committed offsets of group on topic (uncommitted partitions count fully).
func lag(t *testing.T, group, topic string) int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	committed, err := adm.FetchOffsets(t.Context(), group)
	if err != nil {
		t.Fatal(err)
	}
	ends, err := adm.ListEndOffsets(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	ends.Each(func(lo kadm.ListedOffset) {
		at := int64(0)
		if c, ok := committed.Lookup(lo.Topic, lo.Partition); ok && c.At >= 0 {
			at = c.At
		}
		total += lo.Offset - at
	})
	return total
}

// assertEffects: every message applied exactly once, per saga in step order.
func assertEffects(t *testing.T, pool *pgxpool.Pool, msgs []produced) {
	t.Helper()
	if n := count(t, pool, `SELECT count(*) FROM it_effects`); n != len(msgs) {
		t.Fatalf("effects = %d, want %d", n, len(msgs))
	}
	if n := count(t, pool, `SELECT count(*) FROM inbox`); n != len(msgs) {
		t.Fatalf("inbox rows = %d, want %d", n, len(msgs))
	}
	// Out-of-order application: a later step applied before an earlier one of the same saga.
	if n := count(t, pool, `
		SELECT count(*) FROM it_effects a JOIN it_effects b
		  ON a.saga_id = b.saga_id AND a.step < b.step AND a.n > b.n`); n != 0 {
		t.Fatalf("%d step pairs applied out of order", n)
	}
}

func dlqRecords(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()
	return consumeAll(t, kafka.DLQTopic(topic), want, 1500*time.Millisecond, 15*time.Second)
}

func TestConsumer_AppliesEachMessageOnceInOrder_DespiteDuplicates(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	setupEffects(t, pool)
	topic := env.FreshTopic(t, 3)
	group := "g-" + uuid.NewString()

	msgs := produce(t, topic, 30, 5) // 150 messages
	send(t, topic, msgs[10:40]...)   // 30 redeliveries: same ids
	startConsumer(t, pool, consumerCfg(group, topic), effectsHandler(nil))

	eventually(t, 20*time.Second, "all messages applied and committed", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) == len(msgs) && lag(t, group, topic) == 0
	})
	assertEffects(t, pool, msgs)
	if dl := dlqRecords(t, topic, 0); len(dl) != 0 {
		t.Fatalf("DLQ not empty: %d", len(dl))
	}
}

func TestConsumer_PoisonGoesToDLQ_EverythingElseApplied(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	setupEffects(t, pool)
	topic := env.FreshTopic(t, 3)
	group := "g-" + uuid.NewString()

	good := produce(t, topic, 10, 3)
	rejected, _ := message.New(typeReject, good[0].env.SagaID, step{})
	rejectedRaw, _ := json.Marshal(rejected)
	garbage := produced{env: message.Envelope{ID: uuid.New(), SagaID: good[1].env.SagaID}, value: []byte(`{not json`)}
	send(t, topic, produced{env: rejected, value: rejectedRaw}, garbage)
	more := produce(t, topic, 10, 2) // after the poison: must still be applied
	all := slices.Concat(good, more)

	startConsumer(t, pool, consumerCfg(group, topic), effectsHandler(nil))
	eventually(t, 20*time.Second, "valid messages applied, offsets committed", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) == len(all) && lag(t, group, topic) == 0
	})
	assertEffects(t, pool, all)

	dl := dlqRecords(t, topic, 2)
	if len(dl) != 2 {
		t.Fatalf("DLQ records = %d, want 2", len(dl))
	}
	for _, r := range dl {
		if header(r, kafka.HeaderDLQOriginalTopic) != topic || header(r, kafka.HeaderDLQConsumerGroup) != group ||
			header(r, kafka.HeaderDLQError) == "" || header(r, kafka.HeaderDLQOriginalOffset) == "" {
			t.Errorf("DLQ headers incomplete: %v", r.Headers)
		}
		if header(r, kafka.HeaderDLQAttempts) != "1" {
			t.Errorf("permanent failure retried: attempts = %s", header(r, kafka.HeaderDLQAttempts))
		}
	}
}

func TestConsumer_TransientFailuresAreRetriedNotDeadLettered(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	setupEffects(t, pool)
	topic := env.FreshTopic(t, 3)
	group := "g-" + uuid.NewString()

	msgs := produce(t, topic, 10, 2)
	var (
		mu       sync.Mutex
		attempts = map[uuid.UUID]int{}
		failures atomic.Int64
	)
	flaky := func(e message.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		attempts[e.ID]++
		if attempts[e.ID] <= 2 { // every message fails its first two attempts
			failures.Add(1)
			return errors.New("deadlock detected (simulated)")
		}
		return nil
	}

	startConsumer(t, pool, consumerCfg(group, topic), effectsHandler(flaky))
	eventually(t, 30*time.Second, "all applied despite failures", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) == len(msgs) && lag(t, group, topic) == 0
	})
	assertEffects(t, pool, msgs)
	if dl := dlqRecords(t, topic, 0); len(dl) != 0 {
		t.Fatalf("transient failures were dead-lettered: %d", len(dl))
	}
	if failures.Load() < int64(2*len(msgs)) {
		t.Fatalf("only %d failures injected; the scenario didn't happen", failures.Load())
	}
}

// A real outage: the database refuses connections. The consumer must stop
// without dead-lettering anything and without committing, and process
// everything once the database is back.
func TestConsumer_DatabaseOutage_NothingDeadLettered_NothingCommitted(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	setupEffects(t, pool)
	topic := env.FreshTopic(t, 3)
	group := "g-" + uuid.NewString()
	db := pool.Config().ConnConfig.Database

	msgs := produce(t, topic, 10, 3)

	env.Admin(t, fmt.Sprintf(`ALTER DATABASE %s WITH ALLOW_CONNECTIONS false`, db))
	env.Admin(t, fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s'`, db))
	reopened := false
	reopen := func() {
		if !reopened {
			env.Admin(t, fmt.Sprintf(`ALTER DATABASE %s WITH ALLOW_CONNECTIONS true`, db))
			reopened = true
		}
	}
	t.Cleanup(reopen)

	c := startConsumer(t, pool, consumerCfg(group, topic), effectsHandler(nil))
	select {
	case <-c.done:
		if !errors.Is(c.err, kafka.ErrDependencyDown) {
			t.Fatalf("Run = %v, want ErrDependencyDown", c.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("consumer kept running through the outage")
	}
	if l := lag(t, group, topic); l != int64(len(msgs)) {
		t.Fatalf("lag = %d after outage, want %d (nothing committed)", l, len(msgs))
	}
	if dl := dlqRecords(t, topic, 0); len(dl) != 0 {
		t.Fatalf("outage dead-lettered %d records", len(dl))
	}

	reopen()
	startConsumer(t, pool, consumerCfg(group, topic), effectsHandler(nil))
	eventually(t, 20*time.Second, "everything applied after recovery", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) == len(msgs) && lag(t, group, topic) == 0
	})
	assertEffects(t, pool, msgs)
}

// Consumers join and leave while messages flow: nothing lost, nothing applied
// twice, per-saga order kept.
func TestConsumer_ScaleOutAndIn_NoLossNoDoubleEffects(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	setupEffects(t, pool)
	topic := env.FreshTopic(t, 6)
	group := "g-" + uuid.NewString()
	cfg := consumerCfg(group, topic)
	cfg.MaxPollRecords = 20

	slow := func(message.Envelope) error { time.Sleep(2 * time.Millisecond); return nil }

	var msgs []produced
	msgs = append(msgs, produce(t, topic, 40, 5)...) // 200
	startConsumer(t, pool, cfg, effectsHandler(slow))

	eventually(t, 20*time.Second, "first consumer made progress", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) > 30
	})
	second := startConsumer(t, pool, cfg, effectsHandler(slow))
	startConsumer(t, pool, cfg, effectsHandler(slow))
	msgs = append(msgs, produce(t, topic, 40, 5)...) // 200 more while rebalancing

	eventually(t, 20*time.Second, "more progress with 3 members", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) > 250
	})
	if err := second.stop(); err != nil { // graceful leave mid-stream
		t.Fatalf("graceful stop: %v", err)
	}
	msgs = append(msgs, produce(t, topic, 20, 5)...) // 100 more

	eventually(t, 40*time.Second, "everything applied and committed", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) == len(msgs) && lag(t, group, topic) == 0
	})
	assertEffects(t, pool, msgs)
	if dl := dlqRecords(t, topic, 0); len(dl) != 0 {
		t.Fatalf("DLQ not empty: %d", len(dl))
	}
}

// A rebalance that interrupts a record mid-retry must not make the consumer
// skip it. Regression test: kgo's fetch position had already moved past the
// polled records, so without rewinding, the interrupted record was skipped
// and the next commit went past it — silent loss. Found in the compose run.
//
// One partition: it stays with consumer A (cooperative-sticky) when B joins,
// so A itself must re-read the interrupted record.
func TestConsumer_RebalanceMidRetry_DoesNotSkipTheRecord(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	setupEffects(t, pool)
	topic := env.FreshTopic(t, 1)
	group := "g-" + uuid.NewString()

	first := produce(t, topic, 1, 1)
	x := first[0].env.ID
	var attempts atomic.Int64
	tried := make(chan struct{}, 1)
	failX := func(e message.Envelope) error {
		if e.ID != x {
			return nil
		}
		n := attempts.Add(1)
		if n == 1 {
			tried <- struct{}{}
		}
		if n <= 6 { // batch path (4 attempts) + 2 in the slow path: a ~9s window for the rebalance
			return errors.New("lock timeout (simulated)")
		}
		return nil
	}
	cfg := consumerCfg(group, topic)
	cfg.Retry = retry.Policy{MaxAttempts: 4, Initial: 1500 * time.Millisecond, Max: 1500 * time.Millisecond, Multiplier: 1}

	startConsumer(t, pool, cfg, effectsHandler(failX))
	select {
	case <-tried:
	case <-time.After(20 * time.Second):
		t.Fatal("X was never attempted")
	}
	startConsumer(t, pool, cfg, effectsHandler(failX)) // B joins -> rebalance -> A is told to hurry
	more := produce(t, topic, 3, 1)

	all := slices.Concat(first, more)
	// Wait until everything is COMMITTED, then check X was applied: with the
	// bug, lag reaches 0 while X is missing (committed past, never applied).
	eventually(t, 40*time.Second, "all messages committed", func() bool {
		return lag(t, group, topic) == 0 && count(t, pool, `SELECT count(*) FROM it_effects`) >= len(more)
	})
	time.Sleep(time.Second) // let an in-flight transaction for X finish if there is one
	if n := count(t, pool, `SELECT count(*) FROM it_effects WHERE message_id = $1`, x); n != 1 {
		t.Fatalf("X committed but applied %d times: the consumer skipped it (attempts=%d)", n, attempts.Load())
	}
	assertEffects(t, pool, all)
}
