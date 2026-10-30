//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// TestInfra checks the harness itself: schemas migrated, topics created.
func TestInfra(t *testing.T) {
	ctx := context.Background()

	for svc, dsn := range env.DSN {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		var shards int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM outbox_shard_lease").Scan(&shards); err != nil {
			t.Fatalf("%s: %v", svc, err)
		}
		_ = db.Close()
		if shards != 16 {
			t.Errorf("%s: %d shard leases, want 16", svc, shards)
		}
	}

	cl, err := kafka.NewClient(env.Brokers, "it-check")
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	topics, err := kadm.NewClient(cl).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range messaging.Topics(3, 1) {
		if !topics.Has(spec.Name) {
			t.Errorf("topic %s missing", spec.Name)
		}
	}
}

// Full-flow scenarios: the whole system in-process (HTTP API, 3 relays, 3
// workers) in its own namespace with fresh databases. Assertions are on the
// final state of all three databases and on the DLQ topics.

func TestFlow_HappyPath(t *testing.T) {
	s := startSystem(t, defaultOpts())

	traceID := trace.TraceID(uuid.New())
	tp := fmt.Sprintf("00-%s-0102030405060708-01", traceID)
	o := s.placeOrder(7, 25_00, tp)

	got := s.waitDone(30*time.Second, o.ID)[o.ID]
	if got.Status != orchestrator.OrderConfirmed || got.SagaState != orchestrator.StateCompleted || got.FailureReason != "" {
		t.Fatalf("order = %s / %s %q", got.Status, got.SagaState, got.FailureReason)
	}
	if st := one[string](t, s.invDB, `SELECT status FROM seats WHERE seat_id = 7`); st != "RESERVED" {
		t.Errorf("seat 7 = %s", st)
	}
	if st := one[string](t, s.invDB, `SELECT status FROM reservations WHERE saga_id = $1`, o.SagaID); st != "HELD" {
		t.Errorf("reservation = %s", st)
	}
	if st := one[string](t, s.payDB, `SELECT status FROM payments WHERE saga_id = $1 AND amount_cents = 2500`, o.SagaID); st != "CHARGED" {
		t.Errorf("payment = %s", st)
	}

	// One trace across all services: every message of this saga, in every
	// database's outbox, carries the trace of the original HTTP request.
	for name, db := range map[string]*pgxpool.Pool{"orchestrator": s.orchDB, "inventory": s.invDB, "payment": s.payDB} {
		rows := outboxRowsForKey(t, db, o.SagaID.String())
		if len(rows) == 0 {
			t.Errorf("%s: no outbox rows for the saga", name)
		}
		for _, r := range rows {
			if !strings.Contains(r.Headers["traceparent"], traceID.String()) {
				t.Errorf("%s %s: traceparent %q lost the request's trace", name, r.Type, r.Headers["traceparent"])
			}
		}
	}
	s.assertNoDLQ()
}

func TestFlow_SeatTaken_Aborts(t *testing.T) {
	s := startSystem(t, defaultOpts())

	a := s.placeOrder(11, 10_00, "")
	b := s.placeOrder(11, 10_00, "")
	done := s.waitDone(30*time.Second, a.ID, b.ID)

	var winner, loser orchestrator.Order
	switch {
	case done[a.ID].Status == orchestrator.OrderConfirmed && done[b.ID].Status == orchestrator.OrderAborted:
		winner, loser = done[a.ID], done[b.ID]
	case done[b.ID].Status == orchestrator.OrderConfirmed && done[a.ID].Status == orchestrator.OrderAborted:
		winner, loser = done[b.ID], done[a.ID]
	default:
		t.Fatalf("want exactly one CONFIRMED and one ABORTED, got %s and %s", done[a.ID].Status, done[b.ID].Status)
	}
	if loser.FailureReason != "seat already taken" {
		t.Errorf("loser reason = %q", loser.FailureReason)
	}
	if n := count(t, s.payDB, `SELECT count(*) FROM payments`); n != 1 {
		t.Errorf("payments = %d, want 1 (only the winner is charged)", n)
	}
	if who := one[uuid.UUID](t, s.invDB, `SELECT saga_id FROM reservations WHERE seat_id = 11 AND status = 'HELD'`); who != winner.SagaID {
		t.Errorf("seat held by %s, want the winner %s", who, winner.SagaID)
	}
	s.assertNoDLQ()
}

func TestFlow_PaymentDeclined_Compensates(t *testing.T) {
	s := startSystem(t, defaultOpts())

	o := s.placeOrder(21, 5_000_00, "") // above the 1000.00 limit
	got := s.waitDone(30*time.Second, o.ID)[o.ID]

	if got.Status != orchestrator.OrderAborted || got.SagaState != orchestrator.StateAborted {
		t.Fatalf("order = %s / %s", got.Status, got.SagaState)
	}
	if got.FailureReason != "amount exceeds the payment limit" {
		t.Errorf("reason = %q", got.FailureReason)
	}
	// Compensation really happened: seat back to FREE, reservation RELEASED.
	if st := one[string](t, s.invDB, `SELECT status FROM seats WHERE seat_id = 21`); st != "FREE" {
		t.Errorf("seat 21 = %s, want FREE", st)
	}
	if st := one[string](t, s.invDB, `SELECT status FROM reservations WHERE saga_id = $1`, o.SagaID); st != "RELEASED" {
		t.Errorf("reservation = %s, want RELEASED", st)
	}
	if st := one[string](t, s.payDB, `SELECT status FROM payments WHERE saga_id = $1`, o.SagaID); st != "DECLINED" {
		t.Errorf("payment = %s, want DECLINED", st)
	}
	var types []string
	for _, r := range outboxRowsForKey(t, s.orchDB, o.SagaID.String()) {
		types = append(types, r.Type)
	}
	if want := []string{messaging.CmdReserveSeat, messaging.CmdCharge, messaging.CmdReleaseSeat}; strings.Join(types, ",") != strings.Join(want, ",") {
		t.Errorf("orchestrator commands = %v, want %v", types, want)
	}
	s.assertNoDLQ()
}

// Every message of finished sagas is delivered a second time: nothing changes.
func TestFlow_DuplicateDelivery_IsIdempotent(t *testing.T) {
	s := startSystem(t, defaultOpts())

	var ids []uuid.UUID
	for i := range 5 {
		ids = append(ids, s.placeOrder(31+i, 10_00, "").ID)
	}
	ids = append(ids, s.placeOrder(40, 9_999_00, "").ID) // one compensated saga too
	s.waitDone(30*time.Second, ids...)

	before := snapshot(t, s)
	resent := 0
	for _, db := range []*pgxpool.Pool{s.orchDB, s.invDB, s.payDB} {
		resent += resendOutbox(t, db, s.ns)
	}
	// Wait until every consumer group has consumed the re-sent messages.
	eventually(t, 30*time.Second, "duplicates consumed", func() bool {
		return lag(t, s.group("orchestrator"), s.ns.Topic(messaging.TopicOrchestratorReplies)) == 0 &&
			lag(t, s.group("inventory"), s.ns.Topic(messaging.TopicInventoryCommands)) == 0 &&
			lag(t, s.group("payment"), s.ns.Topic(messaging.TopicPaymentCommands)) == 0
	})
	after := snapshot(t, s)

	if resent < 20 {
		t.Fatalf("only %d messages re-sent; scenario too weak", resent)
	}
	if before != after {
		t.Fatalf("state changed after duplicate delivery:\nbefore %s\nafter  %s", before, after)
	}
	s.assertNoDLQ()
}

func TestFlow_TransientFailures_RetriedNotDeadLettered(t *testing.T) {
	opts := defaultOpts()
	opts.inventoryFaults = faults.Config{TransientRate: 0.3}
	opts.paymentFaults = faults.Config{TransientRate: 0.3}
	// 0.3^8 ≈ 7e-5 per record: exhausting retries (-> DLQ) is practically impossible.
	opts.retry = retry.Policy{MaxAttempts: 8, Initial: 10 * time.Millisecond, Max: 100 * time.Millisecond, Multiplier: 2}
	s := startSystem(t, opts)

	var ids []uuid.UUID
	for i := range 20 {
		ids = append(ids, s.placeOrder(100+i, 10_00, "").ID)
	}
	for id, o := range s.waitDone(60*time.Second, ids...) {
		if o.Status != orchestrator.OrderConfirmed {
			t.Errorf("order %s = %s (%s)", id, o.Status, o.FailureReason)
		}
	}
	s.assertNoDLQ()
}

func TestFlow_PoisonMessage_GoesToDLQ_BatchContinues(t *testing.T) {
	s := startSystem(t, defaultOpts())

	var ids []uuid.UUID
	for i := range 5 {
		ids = append(ids, s.placeOrder(200+i, 10_00, "").ID)
	}
	send(t, s.ns.Topic(messaging.TopicInventoryCommands),
		produced{env: message.Envelope{ID: uuid.New(), SagaID: uuid.New()}, value: []byte(`{"garbage":`)})
	for i := range 5 {
		ids = append(ids, s.placeOrder(210+i, 10_00, "").ID)
	}

	for id, o := range s.waitDone(30*time.Second, ids...) {
		if o.Status != orchestrator.OrderConfirmed {
			t.Errorf("order %s = %s", id, o.Status)
		}
	}
	if n := s.dlqCount(messaging.TopicInventoryCommands); n != 1 {
		t.Fatalf("inventory DLQ = %d, want exactly the garbage record", n)
	}
}

// A relay replica dies without releasing its leases while orders flow.
func TestFlow_RelayCrash_NoLossNoReorder(t *testing.T) {
	s := startSystem(t, defaultOpts())
	crash := startCrashableRelay(t, s.orchDB, s.ns, "relay-orch-doomed") // a 2nd orchestrator relay

	var ids []uuid.UUID
	for i := range 20 {
		ids = append(ids, s.placeOrder(300+i, 10_00, "").ID)
	}
	eventually(t, 10*time.Second, "both orchestrator relays own shards", func() bool {
		return len(leaseOwners(t, s.orchDB)) == 2
	})
	crash()
	for i := range 20 {
		ids = append(ids, s.placeOrder(320+i, 10_00, "").ID)
	}

	// Leases of the dead relay expire after LeaseTTL (3s); the survivor takes over.
	for id, o := range s.waitDone(60*time.Second, ids...) {
		if o.Status != orchestrator.OrderConfirmed {
			t.Errorf("order %s = %s", id, o.Status)
		}
	}
	// Reordering would make sagas receive replies in the wrong state; the
	// state machine ignores those and the saga would be stuck, not done.
	if n := count(t, s.orchDB, `SELECT count(*) FROM outbox WHERE type = $1`, messaging.CmdCharge); n != len(ids) {
		t.Errorf("charge commands = %d, want %d", n, len(ids))
	}
	s.assertNoDLQ()
}

// Inventory workers are added while orders flow.
func TestFlow_ScaleOut_RebalancesWithoutLoss(t *testing.T) {
	s := startSystem(t, defaultOpts())

	var ids []uuid.UUID
	for i := range 30 {
		ids = append(ids, s.placeOrder(400+i, 10_00, "").ID)
	}
	s.addInventoryWorker()
	s.addInventoryWorker()
	for i := range 30 {
		ids = append(ids, s.placeOrder(430+i, 10_00, "").ID)
	}

	for id, o := range s.waitDone(60*time.Second, ids...) {
		if o.Status != orchestrator.OrderConfirmed {
			t.Errorf("order %s = %s", id, o.Status)
		}
	}
	if n := count(t, s.invDB, `SELECT count(*) FROM reservations WHERE status = 'HELD'`); n != len(ids) {
		t.Errorf("held reservations = %d, want %d (one per saga, none twice)", n, len(ids))
	}
	if n := count(t, s.payDB, `SELECT count(*) FROM payments WHERE status = 'CHARGED'`); n != len(ids) {
		t.Errorf("payments = %d, want %d", n, len(ids))
	}
	s.assertNoDLQ()
}

// ─── helpers ─────────────────────────────────────────────────────────────

func one[T any](t *testing.T, pool *pgxpool.Pool, sql string, args ...any) T {
	t.Helper()
	var v T
	if err := pool.QueryRow(t.Context(), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

// snapshot summarizes the business state of all three databases.
func snapshot(t *testing.T, s *system) string {
	t.Helper()
	q := func(db *pgxpool.Pool, sql string) string {
		var v string
		if err := db.QueryRow(t.Context(), sql).Scan(&v); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return v
	}
	return strings.Join([]string{
		q(s.orchDB, `SELECT string_agg(status || '/' || coalesce(failure_reason,''), ',' ORDER BY order_id) FROM orders`),
		q(s.orchDB, `SELECT string_agg(state, ',' ORDER BY saga_id) FROM sagas`),
		q(s.orchDB, `SELECT count(*)::text FROM outbox`),
		q(s.invDB, `SELECT string_agg(seat_id || ':' || status, ',' ORDER BY saga_id) FROM reservations`),
		q(s.invDB, `SELECT count(*)::text FROM seats WHERE status = 'RESERVED'`),
		q(s.invDB, `SELECT count(*)::text FROM outbox`),
		q(s.payDB, `SELECT string_agg(status, ',' ORDER BY saga_id) FROM payments`),
		q(s.payDB, `SELECT count(*)::text FROM outbox`),
	}, " | ")
}

// resendOutbox produces every row of a service's outbox again (same message
// ids, same keys) and returns how many.
func resendOutbox(t *testing.T, db *pgxpool.Pool, ns kafka.Namespace) int {
	t.Helper()
	rows, err := db.Query(t.Context(), `SELECT topic, key, payload, headers FROM outbox ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var recs []*kgo.Record
	for rows.Next() {
		var (
			topic, key string
			payload    []byte
			headers    map[string]string
		)
		if err := rows.Scan(&topic, &key, &payload, &headers); err != nil {
			t.Fatal(err)
		}
		r := &kgo.Record{Topic: ns.Topic(topic), Key: []byte(key), Value: payload}
		for k, v := range headers {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs = append(recs, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return len(recs)
}
