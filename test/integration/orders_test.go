//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/platform/message"
)

func newOrchestrator(t *testing.T) (*orchestrator.Service, *pgxpool.Pool) {
	t.Helper()
	pool := env.Pool(t, "orchestrator")
	svc, err := orchestrator.NewService(pool, env.Log)
	if err != nil {
		t.Fatal(err)
	}
	return svc, pool
}

type outboxRow struct {
	ID          uuid.UUID
	Topic, Key  string
	Type        string
	Payload     []byte
	Headers     map[string]string
	Shard       int
	PublishedAt *time.Time
}

func outboxRowsForKey(t *testing.T, pool *pgxpool.Pool, key string) []outboxRow {
	t.Helper()
	rows, err := pool.Query(t.Context(),
		`SELECT id, topic, key, type, payload, headers, shard, published_at
		 FROM outbox WHERE key = $1 ORDER BY seq`, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.ID, &r.Topic, &r.Key, &r.Type, &r.Payload, &r.Headers, &r.Shard, &r.PublishedAt); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func TestPlaceOrder_WritesOrderSagaAndFirstCommand(t *testing.T) {
	svc, pool := newOrchestrator(t)

	// Pretend we're inside an HTTP request span: its trace must reach the outbox.
	traceID := trace.TraceID(uuid.New())
	ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}, TraceFlags: trace.FlagsSampled,
	}))

	in := orderIn(7, 12_345, "")
	order, created, err := svc.PlaceOrder(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if !created || order.Status != orchestrator.OrderPending || order.SagaState != orchestrator.StateScoring {
		t.Fatalf("order = %+v, created = %v", order, created)
	}
	if order.ID.Version() != 7 || order.SagaID.Version() != 7 {
		t.Errorf("ids must be UUIDv7: order %v saga %v", order.ID, order.SagaID)
	}

	// orders + sagas
	got, err := svc.GetOrder(t.Context(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SagaID != order.SagaID || got.SeatID != 7 || got.AmountCents != 12_345 || got.CreatedAt.IsZero() ||
		got.CustomerID != in.CustomerID || got.CardHash != in.CardHash || got.IP != in.IP || got.Country != in.Country {
		t.Fatalf("stored order = %+v, want %+v", got, order)
	}
	if n := count(t, pool, `SELECT count(*) FROM sagas WHERE saga_id = $1 AND state = 'SCORING'`, order.SagaID); n != 1 {
		t.Fatalf("saga in SCORING: %d rows", n)
	}

	// outbox: exactly the first command, unpublished, keyed by saga id
	rows := outboxRowsForKey(t, pool, order.SagaID.String())
	if len(rows) != 1 {
		t.Fatalf("outbox rows = %d, want 1", len(rows))
	}
	r := rows[0]
	if r.Topic != messaging.TopicRiskCommands || r.Type != messaging.CmdScore || r.PublishedAt != nil {
		t.Fatalf("outbox row = %+v", r)
	}
	if r.Shard < 0 || r.Shard > 15 {
		t.Errorf("shard = %d, want 0..15", r.Shard)
	}

	// payload is a valid Envelope the consumer will be able to read
	envl, err := message.Unmarshal(r.Payload)
	if err != nil {
		t.Fatal(err)
	}
	var cmd messaging.Score
	if err := envl.DecodePayload(&cmd); err != nil || cmd.AmountCents != 12_345 || cmd.CustomerID != in.CustomerID ||
		cmd.CardHash != in.CardHash || cmd.IP != in.IP || cmd.Country != in.Country {
		t.Fatalf("command payload = %+v, err = %v", cmd, err)
	}
	if envl.ID != r.ID || envl.SagaID != order.SagaID {
		t.Errorf("envelope ids %v/%v != row %v / saga %v", envl.ID, envl.SagaID, r.ID, order.SagaID)
	}

	// headers carry message metadata and the caller's trace
	if r.Headers[message.HeaderID] != r.ID.String() || r.Headers[message.HeaderType] != messaging.CmdScore {
		t.Errorf("headers = %v", r.Headers)
	}
	if tp := r.Headers["traceparent"]; !strings.Contains(tp, traceID.String()) {
		t.Errorf("traceparent %q does not carry trace %s", tp, traceID)
	}
}

func TestPlaceOrder_IdempotencyKey(t *testing.T) {
	svc, pool := newOrchestrator(t)
	key := "it-" + uuid.NewString()
	in := orderIn(3, 500, key)

	first, created, err := svc.PlaceOrder(t.Context(), in)
	if err != nil || !created {
		t.Fatalf("first: created=%v err=%v", created, err)
	}
	second, created, err := svc.PlaceOrder(t.Context(), in)
	if err != nil || created {
		t.Fatalf("replay: created=%v err=%v", created, err)
	}
	if second.ID != first.ID || second.SagaID != first.SagaID {
		t.Fatalf("replay returned another order: %v vs %v", second.ID, first.ID)
	}
	if n := len(outboxRowsForKey(t, pool, first.SagaID.String())); n != 1 {
		t.Fatalf("replay must not emit a second command: %d outbox rows", n)
	}

	t.Run("same key, different body is rejected", func(t *testing.T) {
		_, _, err := svc.PlaceOrder(t.Context(), orderIn(4, 500, key))
		if !errors.Is(err, orchestrator.ErrIdempotencyKeyReused) {
			t.Fatalf("want ErrIdempotencyKeyReused, got %v", err)
		}
	})

	t.Run("no key: every request is a new order", func(t *testing.T) {
		a, _, errA := svc.PlaceOrder(t.Context(), orderIn(3, 500, ""))
		b, _, errB := svc.PlaceOrder(t.Context(), orderIn(3, 500, ""))
		if errA != nil || errB != nil || a.ID == b.ID {
			t.Fatalf("a=%v b=%v errs=%v,%v", a.ID, b.ID, errA, errB)
		}
	})
}

// Concurrent retries with one key (client timeout + retry storm) must still
// produce exactly one order and one saga: the unique index serializes them.
func TestPlaceOrder_ConcurrentSameKey(t *testing.T) {
	svc, pool := newOrchestrator(t)
	key := "it-race-" + uuid.NewString()
	in := orderIn(9, 900, key)

	const n = 20
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[uuid.UUID]int{}
		created int
		errs    []error
	)
	for range n {
		wg.Go(func() {
			o, c, err := svc.PlaceOrder(t.Context(), in)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids[o.ID]++
			if c {
				created++
			}
		})
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("errors: %v", errs)
	}
	if created != 1 || len(ids) != 1 {
		t.Fatalf("created=%d distinct orders=%d, want 1 and 1", created, len(ids))
	}
	if c := count(t, pool, `SELECT count(*) FROM orders WHERE idempotency_key = $1`, key); c != 1 {
		t.Fatalf("orders with key: %d", c)
	}
}

// If the outbox insert fails, the order and saga must not exist either:
// that's the whole point of writing them in one transaction.
func TestPlaceOrder_OutboxFailureRollsBackEverything(t *testing.T) {
	svc, pool := newOrchestrator(t)
	ctx := t.Context()

	// Make outbox inserts for one magic seat fail, only for this test.
	const poisonSeat = 987_654
	mustExec(t, pool, `
		CREATE OR REPLACE FUNCTION it_fail_outbox() RETURNS trigger AS $$
		BEGIN
			IF (NEW.payload->'payload'->>'amount_cents')::bigint = 987654 THEN
				RAISE EXCEPTION 'injected outbox failure';
			END IF;
			RETURN NEW;
		END $$ LANGUAGE plpgsql`)
	mustExec(t, pool, `CREATE TRIGGER it_fail_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION it_fail_outbox()`)
	t.Cleanup(func() {
		mustExec(t, pool, `DROP TRIGGER IF EXISTS it_fail_outbox ON outbox`)
		mustExec(t, pool, `DROP FUNCTION IF EXISTS it_fail_outbox()`)
	})

	key := "it-rollback-" + uuid.NewString()
	_, _, err := svc.PlaceOrder(ctx, orderIn(poisonSeat, 987_654, key))
	if err == nil || !strings.Contains(err.Error(), "injected outbox failure") {
		t.Fatalf("want injected failure, got %v", err)
	}
	if c := count(t, pool, `SELECT count(*) FROM orders WHERE idempotency_key = $1`, key); c != 0 {
		t.Fatalf("order survived a failed outbox write: %d rows", c)
	}
	if c := count(t, pool, `SELECT count(*) FROM sagas WHERE seat_id = $1`, poisonSeat); c != 0 {
		t.Fatalf("saga survived a failed outbox write: %d rows", c)
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	svc, _ := newOrchestrator(t)
	if _, err := svc.GetOrder(t.Context(), uuid.New()); !errors.Is(err, orchestrator.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	// Cleanup runs after t.Context() is cancelled: use a fresh context.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %v", strings.TrimSpace(sql), err)
	}
}

func orderIn(seat int, amountCents int64, key string) orchestrator.PlaceOrder {
	b := newBuyer()
	return orchestrator.PlaceOrder{SeatID: seat, AmountCents: amountCents, CustomerID: b.customer,
		CardHash: "hash-" + b.card, IP: b.ip, Country: b.country, IdempotencyKey: key}
}
