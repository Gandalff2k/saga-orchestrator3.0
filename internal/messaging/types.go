// Package messaging is the contract between services: topics, message types and payloads.
package messaging

import (
	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/platform/message"
)

// Commands: orchestrator -> participant.
const (
	CmdReserveSeat = "inventory.reserve_seat.v1"
	CmdReleaseSeat = "inventory.release_seat.v1"
	CmdCharge      = "payment.charge.v1"
	CmdRefund      = "payment.refund.v1" // compensation for CmdCharge (sent on timeout)
)

// Replies: participant -> orchestrator (all on TopicOrchestratorReplies).
const (
	EvtSeatReserved          = "inventory.seat_reserved.v1"
	EvtSeatReservationFailed = "inventory.seat_reservation_failed.v1"
	EvtSeatReleased          = "inventory.seat_released.v1"
	EvtPaymentCharged        = "payment.charged.v1"
	EvtPaymentFailed         = "payment.failed.v1"
	EvtPaymentRefunded       = "payment.refunded.v1"
)

// ReserveSeat is the payload of CmdReserveSeat.
type ReserveSeat struct {
	SeatID int `json:"seat_id"`
}

// ReleaseSeat is the payload of CmdReleaseSeat (compensation for ReserveSeat).
type ReleaseSeat struct {
	SeatID int `json:"seat_id"`
}

// Charge is the payload of CmdCharge. Money is integer cents, never float.
type Charge struct {
	AmountCents int64 `json:"amount_cents"`
}

// Refund is the payload of CmdRefund. The payment is identified by the saga
// id (the envelope's), so there's nothing else to carry.
type Refund struct{}

// Reply is the payload of every participant reply. Reason is set on failures.
type Reply struct {
	Reason string `json:"reason,omitempty"`
}

// NewReply builds a participant's reply to the orchestrator. Every reply of
// one saga uses the saga id as Kafka key (outbox.Write enforces it), so
// replies stay ordered with the commands that caused them.
func NewReply(replyType string, sagaID uuid.UUID, reason string) (message.Envelope, error) {
	return message.New(replyType, sagaID, Reply{Reason: reason})
}
