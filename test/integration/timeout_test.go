//go:build integration

package integration

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/platform/message"
)

// waitState waits until the order's saga is in state.
func (s *system) waitState(id uuid.UUID, state orchestrator.State, timeout time.Duration) {
	s.t.Helper()
	eventually(s.t, timeout, "saga reaches "+string(state), func() bool {
		o, err := s.svc.GetOrder(s.t.Context(), id)
		return err == nil && o.SagaState == state
	})
}

// Inventory is down when the order comes in. The sweeper compensates after
// the step timeout; when inventory comes back it processes reserve and then
// release (same partition, same order), so the seat ends up FREE.
func TestTimeout_InventoryDown_SeatEndsUpFree(t *testing.T) {
	opts := defaultOpts()
	opts.noInventory = true
	opts.stepTimeout = time.Second
	s := startSystem(t, opts)

	o := s.placeOrder(50, 10_00, "")
	s.waitState(o.ID, orchestrator.StateCompensating, 15*time.Second)

	s.addInventoryWorker() // inventory recovers
	got := s.waitDone(30*time.Second, o.ID)[o.ID]

	if got.Status != orchestrator.OrderAborted || got.FailureReason != orchestrator.ReasonInventoryTimeout {
		t.Fatalf("order = %s %q", got.Status, got.FailureReason)
	}
	if st := one[string](t, s.invDB, `SELECT status FROM seats WHERE seat_id = 50`); st != "FREE" {
		t.Errorf("seat 50 = %s, want FREE (reserved late, then released)", st)
	}
	if n := count(t, s.payDB, `SELECT count(*) FROM payments`); n != 0 {
		t.Errorf("payments = %d: a timed-out order must never reach payment", n)
	}
	s.assertNoDLQ()
}

// Payment is down while the saga is CHARGING. The sweeper moves it to
// REFUNDING; when payment comes back it charges (late) and then refunds, so
// the customer ends up with their money back and the seat is released.
func TestTimeout_PaymentDown_RefundsThenReleases(t *testing.T) {
	opts := defaultOpts()
	opts.noPayment = true
	opts.stepTimeout = time.Second
	s := startSystem(t, opts)

	o := s.placeOrder(60, 42_00, "")
	s.waitState(o.ID, orchestrator.StateRefunding, 15*time.Second)

	s.addPaymentWorker() // payment recovers
	got := s.waitDone(30*time.Second, o.ID)[o.ID]

	if got.Status != orchestrator.OrderAborted || got.FailureReason != orchestrator.ReasonPaymentTimeout {
		t.Fatalf("order = %s %q", got.Status, got.FailureReason)
	}
	if st := one[string](t, s.payDB, `SELECT status FROM payments WHERE saga_id = $1`, o.SagaID); st != "REFUNDED" {
		t.Errorf("payment = %s, want REFUNDED (charged late, then refunded)", st)
	}
	if st := one[string](t, s.invDB, `SELECT status FROM seats WHERE seat_id = 60`); st != "FREE" {
		t.Errorf("seat 60 = %s, want FREE", st)
	}
	s.assertNoDLQ()
}

// A compensation that arrives BEFORE its action (the action was stuck, e.g.
// in a DLQ, and replayed later) must leave a tombstone that makes the late
// action fail, instead of reserving a seat / charging for an aborted order.
func TestTombstones_LateActionsAfterCompensationAreRefused(t *testing.T) {
	s := startSystem(t, defaultOpts())
	saga := uuid.New() // not an orchestrator saga: we drive participants directly

	cmd := func(typ string, payload any) produced {
		e, err := message.New(typ, saga, payload)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(e)
		return produced{env: e, value: raw}
	}
	send(t, s.ns.Topic(messaging.TopicInventoryCommands),
		cmd(messaging.CmdReleaseSeat, messaging.ReleaseSeat{SeatID: 70}),
		cmd(messaging.CmdReserveSeat, messaging.ReserveSeat{SeatID: 70}))
	send(t, s.ns.Topic(messaging.TopicPaymentCommands),
		cmd(messaging.CmdRefund, messaging.Refund{}),
		cmd(messaging.CmdCharge, messaging.Charge{AmountCents: 500}))

	eventually(t, 20*time.Second, "participants handled the commands", func() bool {
		return count(t, s.invDB, `SELECT count(*) FROM inbox`) == 2 && count(t, s.payDB, `SELECT count(*) FROM inbox`) == 2
	})
	if st := one[string](t, s.invDB, `SELECT status FROM seats WHERE seat_id = 70`); st != "FREE" {
		t.Errorf("seat 70 = %s: a reserve after its release must not take the seat", st)
	}
	if st := one[string](t, s.payDB, `SELECT status FROM payments WHERE saga_id = $1`, saga); st != "CANCELLED" {
		t.Errorf("payment = %s: a charge after its refund must not charge", st)
	}
	var replies []string
	for _, db := range []string{"inventory", "payment"} {
		pool := s.invDB
		if db == "payment" {
			pool = s.payDB
		}
		for _, r := range outboxRowsForKey(t, pool, saga.String()) {
			replies = append(replies, r.Type)
		}
	}
	want := map[string]bool{messaging.EvtSeatReleased: true, messaging.EvtSeatReservationFailed: true,
		messaging.EvtPaymentRefunded: true, messaging.EvtPaymentFailed: true}
	if len(replies) != 4 {
		t.Fatalf("replies = %v, want 4", replies)
	}
	for _, r := range replies {
		if !want[r] {
			t.Errorf("unexpected reply %s (want released, reservation_failed, refunded, failed)", r)
		}
	}
	// The orchestrator never started this saga: its replies are dead-lettered
	// as "unknown saga", visibly, not silently dropped.
	eventually(t, 20*time.Second, "unknown-saga replies dead-lettered", func() bool {
		return s.dlqCount(messaging.TopicOrchestratorReplies) == 4
	})
}

// Several sweepers (one per orchestrator-worker replica) split overdue sagas
// between them: each saga is compensated exactly once per timeout.
func TestSweeper_ConcurrentSweepersHandleEachSagaOnce(t *testing.T) {
	opts := defaultOpts()
	opts.noInventory, opts.noSweeper = true, true
	opts.stepTimeout = 200 * time.Millisecond
	s := startSystem(t, opts)

	var ids []uuid.UUID
	for i := range 30 {
		ids = append(ids, s.placeOrder(500+i, 10_00, "").ID)
	}
	time.Sleep(400 * time.Millisecond) // all deadlines passed

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	for range 3 {
		sw := orchestrator.NewSweeper(s.orchDB, s.svc, opts.stepTimeout, time.Hour, 10, env.Log)
		wg.Go(func() {
			for {
				n, err := sw.SweepOnce(t.Context())
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				total += n
				mu.Unlock()
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()

	if total != len(ids) {
		t.Fatalf("sweepers handled %d sagas, want %d (each exactly once)", total, len(ids))
	}
	if n := count(t, s.orchDB, `SELECT count(*) FROM outbox WHERE type = $1`, messaging.CmdReleaseSeat); n != len(ids) {
		t.Errorf("release commands = %d, want one per saga (%d)", n, len(ids))
	}
	if n := count(t, s.orchDB, `SELECT count(*) FROM sagas WHERE state = 'COMPENSATING'`); n != len(ids) {
		t.Errorf("sagas in COMPENSATING = %d, want %d", n, len(ids))
	}
}

// The step timeout belongs to the sweeper alone and counts from the saga's
// last transition (D-604). Before, the API stamped the first deadline with
// ITS config, so a longer worker-side timeout was silently ignored.
func TestSweeper_TimeoutIsTheSweepersAndCountsFromLastTransition(t *testing.T) {
	opts := defaultOpts()
	opts.noInventory = true
	opts.stepTimeout = 3 * time.Second
	s := startSystem(t, opts)

	o := s.placeOrder(900, 10_00, "")
	time.Sleep(1500 * time.Millisecond)
	if got, _ := s.svc.GetOrder(t.Context(), o.ID); got.SagaState != orchestrator.StateReserving {
		t.Fatalf("compensated after 1.5s with a 3s step timeout: %s", got.SagaState)
	}
	start := time.Now()
	s.waitState(o.ID, orchestrator.StateCompensating, 10*time.Second)
	if waited := time.Since(start) + 1500*time.Millisecond; waited < 3*time.Second || waited > 5*time.Second {
		t.Fatalf("compensated after %s, want ~3s (timeout) + up to one sweep interval", waited)
	}
}
