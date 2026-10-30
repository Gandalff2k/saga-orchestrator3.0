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
)

// Replies: participant -> orchestrator (all on TopicOrchestratorReplies).
const (
	EvtSeatReserved          = "inventory.seat_reserved.v1"
	EvtSeatReservationFailed = "inventory.seat_reservation_failed.v1"
	EvtSeatReleased          = "inventory.seat_released.v1"
	EvtPaymentCharged        = "payment.charged.v1"
	EvtPaymentFailed         = "payment.failed.v1"
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
