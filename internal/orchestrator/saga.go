// Package orchestrator owns orders and drives the order saga.
//
// saga.go is the PURE state machine: no I/O, no clock, no ids generated
// inside transitions. Everything else in the package (store, service, http)
// is plumbing around it. Keep it that way — it's what the unit tests cover.
//
//	RESERVING ──seat_reserved──────► CHARGING ──payment_charged──► COMPLETED (order CONFIRMED)
//	    │                               │
//	    └─seat_reservation_failed─►     └─payment_failed──► COMPENSATING ──seat_released──► ABORTED
//	          ABORTED                       (emit release_seat)                         (order ABORTED)
package orchestrator

import (
	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/messaging"
)

// State is the saga's position in the state machine (persisted in sagas.state).
type State string

// Saga states; see the diagram in the package doc.
const (
	StateReserving    State = "RESERVING"
	StateCharging     State = "CHARGING"
	StateCompensating State = "COMPENSATING"
	StateCompleted    State = "COMPLETED"
	StateAborted      State = "ABORTED"
)

// Terminal reports whether no further transitions are possible.
func (s State) Terminal() bool { return s == StateCompleted || s == StateAborted }

// OrderStatus is what the customer sees. It's coarser than State: an order
// stays PENDING through every intermediate saga step.
type OrderStatus string

// Order statuses (persisted in orders.status).
const (
	OrderPending   OrderStatus = "PENDING"
	OrderConfirmed OrderStatus = "CONFIRMED"
	OrderAborted   OrderStatus = "ABORTED"
)

// Saga is the orchestrator's record of one order's distributed transaction.
type Saga struct {
	ID          uuid.UUID
	OrderID     uuid.UUID
	State       State
	SeatID      int
	AmountCents int64
}

// Outgoing is a command the saga wants sent. The service turns it into an
// Envelope and writes it to the outbox in the same transaction as the state change.
type Outgoing struct {
	Topic   string
	Type    string
	Payload any
}

// Transition is the result of feeding one reply into the saga.
type Transition struct {
	Next  State
	Order OrderStatus // "" = order status unchanged
	Emit  []Outgoing
	// Ignored means the reply doesn't apply to the current state (late,
	// duplicated with a different id, or for a finished saga). It is not an
	// error: the service records it in the inbox and moves on.
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
		return Transition{
			Next: StateCharging,
			Emit: []Outgoing{{
				Topic:   messaging.TopicPaymentCommands,
				Type:    messaging.CmdCharge,
				Payload: messaging.Charge{AmountCents: s.AmountCents},
			}},
		}

	case s.State == StateReserving && replyType == messaging.EvtSeatReservationFailed:
		return Transition{Next: StateAborted, Order: OrderAborted}

	case s.State == StateCharging && replyType == messaging.EvtPaymentCharged:
		return Transition{Next: StateCompleted, Order: OrderConfirmed}

	case s.State == StateCharging && replyType == messaging.EvtPaymentFailed:
		return Transition{
			Next: StateCompensating,
			Emit: []Outgoing{{
				Topic:   messaging.TopicInventoryCommands,
				Type:    messaging.CmdReleaseSeat,
				Payload: messaging.ReleaseSeat{SeatID: s.SeatID},
			}},
		}

	case s.State == StateCompensating && replyType == messaging.EvtSeatReleased:
		return Transition{Next: StateAborted, Order: OrderAborted}
	}

	return Transition{Next: s.State, Ignored: true}
}
