package messaging

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

type ReserveSeat struct {
	SeatID int `json:"seat_id"`
}

type ReleaseSeat struct {
	SeatID int `json:"seat_id"`
}

type Charge struct {
	AmountCents int64 `json:"amount_cents"`
}

// Reply is the payload of every participant reply. Reason is set on failures.
type Reply struct {
	Reason string `json:"reason,omitempty"`
}
