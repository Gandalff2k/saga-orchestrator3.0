//go:build integration

package integration

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/risk"
)

func reviewOpts() sysOpts {
	opts := defaultOpts()
	opts.riskCfg.ReviewLimitCents = 100_00
	return opts
}

// Six small payments from one card at once, scored by three workers in
// parallel: the per-card lock makes the count exact, so exactly the 4th, 5th
// and 6th are blocked, whatever order they arrive in.
func TestRisk_CardTestingIsBlocked(t *testing.T) {
	opts := defaultOpts()
	opts.riskWorkers = 3
	s := startSystem(t, opts)

	b := newBuyer()
	var ids []uuid.UUID
	for i := range 6 {
		ids = append(ids, s.placeOrderAs(b, 600+i, 1_00, "").ID)
	}
	done := s.waitDone(30*time.Second, ids...)

	confirmed, blocked := 0, 0
	for _, o := range done {
		switch {
		case o.Status == orchestrator.OrderConfirmed:
			confirmed++
		case strings.HasPrefix(o.FailureReason, risk.ReasonBlockedPrefix+"card_testing"):
			blocked++
		default:
			t.Errorf("order %s: %s %q", o.ID, o.Status, o.FailureReason)
		}
	}
	if confirmed != 3 || blocked != 3 {
		t.Fatalf("confirmed=%d blocked=%d, want 3 and 3", confirmed, blocked)
	}
	if n := count(t, s.invDB, `SELECT count(*) FROM reservations`); n != 3 {
		t.Errorf("reservations = %d: a blocked order must never touch inventory", n)
	}
	s.assertNoDLQ()
}

func TestRisk_OverLimitWaitsForReview_ThenApproved(t *testing.T) {
	s := startSystem(t, reviewOpts())

	o := s.placeOrder(610, 150_00, "")
	s.waitState(o.ID, orchestrator.StateInReview, 20*time.Second)

	c, err := risk.GetCase(t.Context(), s.riskDB, o.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	if c.Status != "PENDING" || c.AmountCents != 150_00 || len(c.Hits) != 1 || c.Hits[0].Rule != "over_limit" {
		t.Fatalf("case = %+v", c)
	}
	if n := count(t, s.invDB, `SELECT count(*) FROM reservations`); n != 0 {
		t.Fatalf("seat reserved while the order is in review")
	}

	if err := risk.DecideCase(t.Context(), s.riskDB, o.SagaID, true, "alice", "known customer"); err != nil {
		t.Fatal(err)
	}
	got := s.waitDone(30*time.Second, o.ID)[o.ID]
	if got.Status != orchestrator.OrderConfirmed || got.FailureReason != "" {
		t.Fatalf("order = %s %q", got.Status, got.FailureReason)
	}
	if err := risk.DecideCase(t.Context(), s.riskDB, o.SagaID, false, "bob", ""); !errors.Is(err, risk.ErrCaseClosed) {
		t.Fatalf("second decision: %v, want ErrCaseClosed", err)
	}
	s.assertNoDLQ()
}

func TestRisk_ReviewRejected_NothingReservedOrCharged(t *testing.T) {
	s := startSystem(t, reviewOpts())

	o := s.placeOrder(620, 150_00, "")
	s.waitState(o.ID, orchestrator.StateInReview, 20*time.Second)
	if err := risk.DecideCase(t.Context(), s.riskDB, o.SagaID, false, "alice", "stolen card"); err != nil {
		t.Fatal(err)
	}

	got := s.waitDone(30*time.Second, o.ID)[o.ID]
	if got.Status != orchestrator.OrderAborted || got.FailureReason != risk.ReasonReviewRejected {
		t.Fatalf("order = %s %q", got.Status, got.FailureReason)
	}
	if n := count(t, s.invDB, `SELECT count(*) FROM reservations`) + count(t, s.payDB, `SELECT count(*) FROM payments`); n != 0 {
		t.Fatalf("rejected order reached inventory or payment: %d rows", n)
	}
}

// Nobody reviews in time: the saga aborts on its own and closes the case, so a
// late reviewer can no longer approve an order the customer was told failed.
func TestRisk_ReviewTimeout_AbortsAndClosesTheCase(t *testing.T) {
	opts := reviewOpts()
	opts.stepTimeout, opts.reviewTimeout = 2*time.Second, 2*time.Second
	s := startSystem(t, opts)

	o := s.placeOrder(630, 150_00, "")
	got := s.waitDone(30*time.Second, o.ID)[o.ID]
	if got.Status != orchestrator.OrderAborted || got.FailureReason != orchestrator.ReasonReviewTimeout {
		t.Fatalf("order = %s %q", got.Status, got.FailureReason)
	}
	eventually(t, 20*time.Second, "case expired", func() bool {
		c, err := risk.GetCase(t.Context(), s.riskDB, o.SagaID)
		return err == nil && c.Status == "EXPIRED"
	})
	if err := risk.DecideCase(t.Context(), s.riskDB, o.SagaID, true, "late", ""); !errors.Is(err, risk.ErrCaseClosed) {
		t.Fatalf("late approval: %v, want ErrCaseClosed", err)
	}
}

// Fail closed: with the risk service down, no order gets through unchecked.
func TestRisk_ServiceDown_FailsClosed(t *testing.T) {
	opts := defaultOpts()
	opts.noRisk = true
	opts.stepTimeout = time.Second
	s := startSystem(t, opts)

	o := s.placeOrder(640, 10_00, "")
	got := s.waitDone(30*time.Second, o.ID)[o.ID]
	if got.Status != orchestrator.OrderAborted || got.FailureReason != orchestrator.ReasonRiskTimeout {
		t.Fatalf("order = %s %q", got.Status, got.FailureReason)
	}
	if n := count(t, s.invDB, `SELECT count(*) FROM reservations`); n != 0 {
		t.Fatalf("seat reserved without a risk decision")
	}
}

// The same score command delivered again with a NEW message id (a re-sent
// command, so the inbox can't catch it) replays the stored decision instead
// of scoring again: a second scoring would count the order against itself.
func TestRisk_RepeatedScoreCommand_ReplaysTheDecision(t *testing.T) {
	s := startSystem(t, defaultOpts())
	saga := uuid.New()
	b := newBuyer()
	score := messaging.Score{CustomerID: b.customer, CardHash: "h-" + b.card, IP: b.ip, Country: b.country, AmountCents: 1_00}

	var msgs []produced
	for range 2 {
		e, err := message.New(messaging.CmdScore, saga, score)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(e)
		msgs = append(msgs, produced{env: e, value: raw})
	}
	send(t, s.ns.Topic(messaging.TopicRiskCommands), msgs...)

	eventually(t, 20*time.Second, "both commands handled", func() bool {
		return count(t, s.riskDB, `SELECT count(*) FROM inbox`) == 2
	})
	if n := count(t, s.riskDB, `SELECT count(*) FROM risk_events WHERE saga_id = $1`, saga); n != 1 {
		t.Fatalf("risk events = %d, want 1", n)
	}
	replies := outboxRowsForKey(t, s.riskDB, saga.String())
	if len(replies) != 2 || replies[0].Type != messaging.EvtRiskApproved || replies[1].Type != replies[0].Type {
		t.Fatalf("replies = %+v, want two identical approvals", replies)
	}
}
