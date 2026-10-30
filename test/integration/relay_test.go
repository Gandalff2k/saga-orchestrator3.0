//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// relayCfg: small TTL so takeover/rebalance happen within a test's patience.
func relayCfg() config.Relay {
	return config.Relay{
		BatchSize:       50, // < messages per test: forces multi-batch draining
		PollInterval:    50 * time.Millisecond,
		LeaseTTL:        3 * time.Second,
		PublishTimeout:  900 * time.Millisecond,
		Concurrency:     4,
		Retention:       24 * time.Hour,
		JanitorInterval: time.Hour,
		Retry:           retry.Policy{Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2},
	}
}

// startRelay runs a relay in the background; stop() cancels it and waits
// for its graceful shutdown (leases released).
func startRelay(t *testing.T, pool *pgxpool.Pool, cfg config.Relay, name string) (stop func()) {
	t.Helper()
	return startRelayNS(t, pool, cfg, "", name)
}

type sent struct {
	sagaID uuid.UUID
	msgID  uuid.UUID
}

// writeMessages writes perSaga messages for each of sagas sagas, each in its
// own transaction (like real services do), with a span in ctx so trace
// propagation can be checked. Returns them in write order.
func writeMessages(t *testing.T, pool *pgxpool.Pool, topic string, sagas, perSaga int, traceID trace.TraceID) []sent {
	t.Helper()
	ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: trace.SpanID{9, 9, 9, 9, 9, 9, 9, 9}, TraceFlags: trace.FlagsSampled,
	}))
	ids := make([]uuid.UUID, sagas)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}

	var out []sent
	for step := range perSaga {
		for _, saga := range ids {
			env, err := message.New("test.step.v1", saga, map[string]int{"step": step})
			if err != nil {
				t.Fatal(err)
			}
			err = postgres.WithTx(ctx, pool, func(tx pgx.Tx) error { return outbox.Write(ctx, tx, topic, env) })
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, sent{sagaID: saga, msgID: env.ID})
		}
	}
	return out
}

// consumeAll reads topic from the start until no new record arrives for
// settle (so duplicates would be caught too), or until timeout.
func consumeAll(t *testing.T, topic string, want int, settle, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	deadline := time.Now().Add(timeout)
	var out []*kgo.Record
	for time.Now().Before(deadline) {
		wait := settle
		if len(out) < want {
			wait = time.Until(deadline)
		}
		ctx, cancel := context.WithTimeout(t.Context(), wait)
		fetches := cl.PollFetches(ctx)
		cancel()
		n := 0
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r); n++ })
		if n == 0 && len(out) >= want {
			break // settled
		}
	}
	return out
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

func leaseOwners(t *testing.T, pool *pgxpool.Pool) map[string]int {
	t.Helper()
	rows, err := pool.Query(t.Context(),
		`SELECT owner, count(*) FROM outbox_shard_lease
		 WHERE owner IS NOT NULL AND expires_at > now() GROUP BY owner`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct {
		owner string
		n     int
	}, error) {
		var v struct {
			owner string
			n     int
		}
		err := r.Scan(&v.owner, &v.n)
		return v, err
	})
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]int{}
	for _, v := range out {
		m[v.owner] = v.n
	}
	return m
}

func unpublished(t *testing.T, pool *pgxpool.Pool) int {
	return count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`)
}

// assertDelivered: every sent message arrived exactly once, and per saga in
// the order it was written.
func assertDelivered(t *testing.T, msgs []sent, recs []*kgo.Record) {
	t.Helper()
	if len(recs) != len(msgs) {
		t.Errorf("received %d records, sent %d (duplicates or loss)", len(recs), len(msgs))
	}
	wantOrder := map[string][]string{}
	for _, m := range msgs {
		wantOrder[m.sagaID.String()] = append(wantOrder[m.sagaID.String()], m.msgID.String())
	}
	gotOrder := map[string][]string{}
	seen := map[string]bool{}
	for _, r := range recs {
		id := header(r, message.HeaderID)
		if seen[id] {
			t.Errorf("duplicate message %s", id)
		}
		seen[id] = true
		gotOrder[string(r.Key)] = append(gotOrder[string(r.Key)], id)
	}
	for saga, want := range wantOrder {
		if got := gotOrder[saga]; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("saga %s: order %v, want %v", saga, got, want)
		}
	}
}

func TestRelay_PublishesEverythingInOrderPerKey(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	topic := env.FreshTopic(t, 3)
	traceID := trace.TraceID(uuid.New())

	msgs := writeMessages(t, pool, topic, 20, 5, traceID) // 100 msgs, 2+ batches per busy shard
	startRelay(t, pool, relayCfg(), "relay-a")

	recs := consumeAll(t, topic, len(msgs), time.Second, 20*time.Second)
	assertDelivered(t, msgs, recs)

	for _, r := range recs {
		if tp := header(r, "traceparent"); !strings.Contains(tp, traceID.String()) {
			t.Fatalf("record %s: traceparent %q lost the writer's trace %s", header(r, message.HeaderID), tp, traceID)
		}
		if header(r, message.HeaderType) != "test.step.v1" {
			t.Fatalf("message-type header missing: %v", r.Headers)
		}
	}
	eventually(t, 5*time.Second, "all rows marked published", func() bool { return unpublished(t, pool) == 0 })
}

func TestRelay_ReplicasSplitShardsFairly(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	topic := env.FreshTopic(t, 3)

	startRelay(t, pool, relayCfg(), "relay-a")
	eventually(t, 5*time.Second, "relay-a owns all 16 shards", func() bool {
		o := leaseOwners(t, pool)
		return len(o) == 1 && sum(o) == 16
	})

	stopB := startRelay(t, pool, relayCfg(), "relay-b")
	eventually(t, 10*time.Second, "two replicas own 8 shards each", func() bool {
		o := leaseOwners(t, pool)
		return len(o) == 2 && sum(o) == 16 && allEqual(o, 8)
	})

	msgs := writeMessages(t, pool, topic, 30, 3, trace.TraceID(uuid.New()))
	assertDelivered(t, msgs, consumeAll(t, topic, len(msgs), time.Second, 20*time.Second))

	// Scale in: B leaves gracefully -> A takes everything without waiting for TTL.
	stopB()
	eventually(t, 3*time.Second, "relay-a owns all shards again", func() bool {
		o := leaseOwners(t, pool)
		return len(o) == 1 && sum(o) == 16
	})
}

func TestRelay_CrashedOwnerIsTakenOverAfterTTL(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	topic := env.FreshTopic(t, 3)

	// A replica "crashed" holding every lease: no graceful release, the leases
	// are valid for 2 more seconds.
	mustExec(t, pool, `UPDATE outbox_shard_lease SET owner = 'crashed', expires_at = now() + interval '2 seconds'`)
	msgs := writeMessages(t, pool, topic, 10, 2, trace.TraceID(uuid.New()))

	start := time.Now()
	startRelay(t, pool, relayCfg(), "relay-survivor")

	time.Sleep(time.Second)
	if n := unpublished(t, pool); n != len(msgs) {
		t.Fatalf("%d rows published while the crashed owner's leases were still valid", len(msgs)-n)
	}

	assertDelivered(t, msgs, consumeAll(t, topic, len(msgs), time.Second, 15*time.Second))
	if took := time.Since(start); took < 2*time.Second {
		t.Fatalf("published after %s, before the crashed leases expired", took)
	}
	if o := leaseOwners(t, pool); o["crashed"] != 0 {
		t.Fatalf("crashed owner still holds leases: %v", o)
	}
}

func TestRelay_JanitorDeletesOnlyOldPublishedRows(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	topic := env.FreshTopic(t, 3)
	msgs := writeMessages(t, pool, topic, 20, 1, trace.TraceID(uuid.New()))

	// Block one shard (not shard 0: its owner runs the janitor) by leasing it
	// to a replica that doesn't exist. Its rows stay unpublished, and the
	// janitor must not delete them however old they get.
	var blockedShard, blocked int
	err := pool.QueryRow(t.Context(),
		`SELECT shard, count(*) FROM outbox WHERE shard <> 0
		 GROUP BY shard ORDER BY count(*) DESC, shard LIMIT 1`).Scan(&blockedShard, &blocked)
	if err != nil || blocked == len(msgs) {
		t.Fatalf("setup: need rows on a non-zero shard and elsewhere (err=%v, %d of %d)", err, blocked, len(msgs))
	}
	if _, err := pool.Exec(t.Context(),
		`UPDATE outbox_shard_lease SET owner = 'other', expires_at = now() + interval '1 hour' WHERE shard = $1`,
		blockedShard); err != nil {
		t.Fatal(err)
	}

	cfg := relayCfg()
	cfg.Retention = 500 * time.Millisecond
	cfg.JanitorInterval = 200 * time.Millisecond
	startRelay(t, pool, cfg, "relay-janitor")

	eventually(t, 10*time.Second, "published rows deleted, blocked ones kept", func() bool {
		return count(t, pool, `SELECT count(*) FROM outbox`) == blocked
	})
	if n := count(t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL AND shard = $1`, blockedShard); n != blocked {
		t.Fatalf("blocked shard rows left = %d, want %d", n, blocked)
	}
}

func sum(m map[string]int) int {
	s := 0
	for _, v := range m {
		s += v
	}
	return s
}

func allEqual(m map[string]int, want int) bool {
	for _, v := range m {
		if v != want {
			return false
		}
	}
	return true
}
