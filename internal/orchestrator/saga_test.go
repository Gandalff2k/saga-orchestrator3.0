package orchestrator

import (
	"testing"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/messaging"
)

func TestStart(t *testing.T) {
	s, cmd := Start(uuid.New(), uuid.New(), 5, 100_00)

	if s.State != StateReserving {
		t.Fatalf("state = %s, want RESERVING", s.State)
	}
	if cmd.Topic != messaging.TopicInventoryCommands || cmd.Type != messaging.CmdReserveSeat {
		t.Fatalf("first command = %s/%s", cmd.Topic, cmd.Type)
	}
	if p := cmd.Payload.(messaging.ReserveSeat); p.SeatID != 5 {
		t.Fatalf("payload seat = %d", p.SeatID)
	}
}

func TestSaga_On(t *testing.T) {
	tests := []struct {
		name      string
		from      State
		reply     string
		wantNext  State
		wantOrder OrderStatus
		wantEmit  string // message type of the single emitted command, "" = none
		ignored   bool
	}{
		// happy path
		{"reserved -> charge", StateReserving, messaging.EvtSeatReserved, StateCharging, "", messaging.CmdCharge, false},
		{"charged -> completed", StateCharging, messaging.EvtPaymentCharged, StateCompleted, OrderConfirmed, "", false},

		// failure + compensation
		{"seat unavailable -> aborted", StateReserving, messaging.EvtSeatReservationFailed, StateAborted, OrderAborted, "", false},
		{"payment failed -> release seat", StateCharging, messaging.EvtPaymentFailed, StateCompensating, "", messaging.CmdReleaseSeat, false},
		{"released -> aborted", StateCompensating, messaging.EvtSeatReleased, StateAborted, OrderAborted, "", false},

		// refund path (after a payment timeout)
		{"refunded -> release seat", StateRefunding, messaging.EvtPaymentRefunded, StateCompensating, "", messaging.CmdReleaseSeat, false},
		{"late charge while refunding is ignored", StateRefunding, messaging.EvtPaymentCharged, StateRefunding, "", "", true},
		{"late failure while refunding is ignored", StateRefunding, messaging.EvtPaymentFailed, StateRefunding, "", "", true},
		{"refunded outside refunding is ignored", StateCharging, messaging.EvtPaymentRefunded, StateCharging, "", "", true},

		// out-of-order / late / duplicate replies are ignored, never crash the saga
		{"late reserved while charging", StateCharging, messaging.EvtSeatReserved, StateCharging, "", "", true},
		{"charged after completion", StateCompleted, messaging.EvtPaymentCharged, StateCompleted, "", "", true},
		{"anything after abort", StateAborted, messaging.EvtSeatReleased, StateAborted, "", "", true},
		{"unknown reply type", StateReserving, "something.else.v1", StateReserving, "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Saga{ID: uuid.New(), OrderID: uuid.New(), State: tt.from, SeatID: 3, AmountCents: 42_00}
			tr := s.On(tt.reply)

			if tr.Next != tt.wantNext {
				t.Errorf("next = %s, want %s", tr.Next, tt.wantNext)
			}
			if tr.Order != tt.wantOrder {
				t.Errorf("order = %q, want %q", tr.Order, tt.wantOrder)
			}
			if tr.Ignored != tt.ignored {
				t.Errorf("ignored = %v, want %v", tr.Ignored, tt.ignored)
			}
			switch {
			case tt.wantEmit == "" && len(tr.Emit) != 0:
				t.Errorf("emitted %d commands, want none", len(tr.Emit))
			case tt.wantEmit != "" && (len(tr.Emit) != 1 || tr.Emit[0].Type != tt.wantEmit):
				t.Errorf("emit = %+v, want one %s", tr.Emit, tt.wantEmit)
			}
		})
	}
}

func TestSaga_CompensationReleasesTheReservedSeat(t *testing.T) {
	s := Saga{State: StateCharging, SeatID: 11}
	tr := s.On(messaging.EvtPaymentFailed)
	if p := tr.Emit[0].Payload.(messaging.ReleaseSeat); p.SeatID != 11 {
		t.Fatalf("release seat %d, want 11", p.SeatID)
	}
}

func TestSaga_ChargeCarriesAmount(t *testing.T) {
	s := Saga{State: StateReserving, AmountCents: 999}
	tr := s.On(messaging.EvtSeatReserved)
	if p := tr.Emit[0].Payload.(messaging.Charge); p.AmountCents != 999 {
		t.Fatalf("charge %d, want 999", p.AmountCents)
	}
}

func TestState_Terminal(t *testing.T) {
	for _, s := range []State{StateReserving, StateCharging, StateCompensating} {
		if s.Terminal() {
			t.Errorf("%s must not be terminal", s)
		}
	}
	for _, s := range []State{StateCompleted, StateAborted} {
		if !s.Terminal() {
			t.Errorf("%s must be terminal", s)
		}
	}
}

func TestSaga_OnTimeout(t *testing.T) {
	tests := []struct {
		from       State
		wantNext   State
		wantEmit   string
		wantReason string
		ignored    bool
	}{
		// "We don't know what happened" -> compensate what MAY have happened.
		{StateReserving, StateCompensating, messaging.CmdReleaseSeat, ReasonInventoryTimeout, false},
		{StateCharging, StateRefunding, messaging.CmdRefund, ReasonPaymentTimeout, false},
		// Compensation itself timed out: re-send it (it's idempotent), keep the reason.
		{StateRefunding, StateRefunding, messaging.CmdRefund, "", false},
		{StateCompensating, StateCompensating, messaging.CmdReleaseSeat, "", false},
		// Finished sagas never time out.
		{StateCompleted, StateCompleted, "", "", true},
		{StateAborted, StateAborted, "", "", true},
	}
	for _, tt := range tests {
		t.Run(string(tt.from), func(t *testing.T) {
			tr := Saga{State: tt.from, SeatID: 4}.OnTimeout()
			if tr.Next != tt.wantNext || tr.Ignored != tt.ignored || tr.Reason != tt.wantReason {
				t.Fatalf("got next=%s ignored=%v reason=%q", tr.Next, tr.Ignored, tr.Reason)
			}
			if tr.Order != "" {
				t.Errorf("a timeout never ends the order directly (compensation must finish first), got %s", tr.Order)
			}
			switch {
			case tt.wantEmit == "" && len(tr.Emit) != 0:
				t.Errorf("emitted %v", tr.Emit)
			case tt.wantEmit != "" && (len(tr.Emit) != 1 || tr.Emit[0].Type != tt.wantEmit):
				t.Errorf("emit = %+v, want one %s", tr.Emit, tt.wantEmit)
			}
		})
	}
}

func TestState_RefundingIsNotTerminal(t *testing.T) {
	if StateRefunding.Terminal() {
		t.Fatal("REFUNDING must not be terminal")
	}
}
