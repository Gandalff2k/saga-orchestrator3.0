// Package orchestrator runs the order saga: the pure state machine, the service, the HTTP API and the timeout sweeper.
package orchestrator

import (
	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/messaging"
)

// State is the saga's position in the state machine (persisted in sagas.state).
type State string

// Saga states; COMPLETED and ABORTED are terminal.
const (
	StateReserving    State = "RESERVING"
	StateCharging     State = "CHARGING"
	StateRefunding    State = "REFUNDING"
	StateCompensating State = "COMPENSATING"
	StateCompleted    State = "COMPLETED"
	StateAborted      State = "ABORTED"
)

// Terminal reports whether no further transitions are possible.
func (s State) Terminal() bool { return s == StateCompleted || s == StateAborted }

// OrderStatus is what the customer sees.
type OrderStatus string

// Order statuses.
const (
	OrderPending   OrderStatus = "PENDING"
	OrderConfirmed OrderStatus = "CONFIRMED"
	OrderAborted   OrderStatus = "ABORTED"
)

// Abort reasons written by the timeout sweeper.
const (
	ReasonInventoryTimeout = "timed out waiting for inventory"
	ReasonPaymentTimeout   = "timed out waiting for payment"
)

// Saga is the orchestrator's record of one order's distributed transaction.
type Saga struct {
	ID          uuid.UUID
	OrderID     uuid.UUID
	State       State
	SeatID      int
	AmountCents int64
}

// Outgoing is a command the saga wants sent.
type Outgoing struct {
	Topic   string
	Type    string
	Payload any
}

// Transition is the result of feeding one reply into the saga.
type Transition struct {
	Next    State
	Order   OrderStatus
	Reason  string
	Emit    []Outgoing
	Ignored bool
}

// Start returns a new saga in RESERVING and the first command to send.
func Start(sagaID, orderID uuid.UUID, seatID int, amountCents int64) (Saga, Outgoing) {
	s := Saga{ID: sagaID, OrderID: orderID, State: StateReserving, SeatID: seatID, AmountCents: amountCents}
	return s, Outgoing{
		Topic:   messaging.TopicInventoryCommands,
		Type:    messaging.CmdReserveSeat,
		Payload: messaging.ReserveSeat{SeatID: seatID},
	}
}

// On computes the transition for a reply of the given message type.
func (s Saga) On(replyType string) Transition {
	switch {
	case s.State == StateReserving && replyType == messaging.EvtSeatReserved:
		return Transition{Next: StateCharging, Emit: []Outgoing{s.charge()}}

	case s.State == StateReserving && replyType == messaging.EvtSeatReservationFailed:
		return Transition{Next: StateAborted, Order: OrderAborted}

	case s.State == StateCharging && replyType == messaging.EvtPaymentCharged:
		return Transition{Next: StateCompleted, Order: OrderConfirmed}

	case s.State == StateCharging && replyType == messaging.EvtPaymentFailed:
		return Transition{Next: StateCompensating, Emit: []Outgoing{s.release()}}

	case s.State == StateRefunding && replyType == messaging.EvtPaymentRefunded:
		return Transition{Next: StateCompensating, Emit: []Outgoing{s.release()}}

	case s.State == StateCompensating && replyType == messaging.EvtSeatReleased:
		return Transition{Next: StateAborted, Order: OrderAborted}
	}

	// Includes late replies after a timeout: e.g. payment.charged while
	// REFUNDING — the refund already in flight undoes it.
	return Transition{Next: s.State, Ignored: true}
}

// OnTimeout computes what to do when the saga waited too long for a reply.
func (s Saga) OnTimeout() Transition {
	switch s.State {
	case StateReserving:
		return Transition{Next: StateCompensating, Reason: ReasonInventoryTimeout, Emit: []Outgoing{s.release()}}
	case StateCharging:
		return Transition{Next: StateRefunding, Reason: ReasonPaymentTimeout, Emit: []Outgoing{s.refund()}}
	case StateRefunding:
		return Transition{Next: StateRefunding, Emit: []Outgoing{s.refund()}} // re-send; refund is idempotent
	case StateCompensating:
		return Transition{Next: StateCompensating, Emit: []Outgoing{s.release()}} // re-send; release is idempotent
	}
	return Transition{Next: s.State, Ignored: true}
}

func (s Saga) charge() Outgoing {
	return Outgoing{Topic: messaging.TopicPaymentCommands, Type: messaging.CmdCharge,
		Payload: messaging.Charge{AmountCents: s.AmountCents}}
}

func (s Saga) refund() Outgoing {
	return Outgoing{Topic: messaging.TopicPaymentCommands, Type: messaging.CmdRefund, Payload: messaging.Refund{}}
}

func (s Saga) release() Outgoing {
	return Outgoing{Topic: messaging.TopicInventoryCommands, Type: messaging.CmdReleaseSeat,
		Payload: messaging.ReleaseSeat{SeatID: s.SeatID}}
}
