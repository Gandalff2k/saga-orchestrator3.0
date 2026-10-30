// Package inventory is the saga participant that reserves and releases seats.
package inventory

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// Service handles inventory commands. It is stateless: all state is in the DB
// and reached through the inbox tx passed to Handle.
type Service struct {
	faults faults.Config
	log    *slog.Logger
}

// NewService builds the service; f injects failures for demos (zero value = off).
func NewService(f faults.Config, log *slog.Logger) *Service {
	return &Service{faults: f, log: log}
}

// Handle is the inbox.Handler for inventory.commands.
func (s *Service) Handle(ctx context.Context, tx pgx.Tx, env message.Envelope) error {
	if err := s.faults.Maybe(env.Type); err != nil {
		return err
	}
	switch env.Type {
	case messaging.CmdReserveSeat:
		var cmd messaging.ReserveSeat
		if err := env.DecodePayload(&cmd); err != nil {
			return retry.Permanent(err)
		}
		return s.reserve(ctx, tx, env, cmd)
	case messaging.CmdReleaseSeat:
		var cmd messaging.ReleaseSeat
		if err := env.DecodePayload(&cmd); err != nil {
			return retry.Permanent(err)
		}
		return s.release(ctx, tx, env, cmd)
	default:
		return retry.Permanent(fmt.Errorf("inventory: unknown message type %q", env.Type))
	}
}

// Failure reasons sent back to the orchestrator (shown to the customer).
const (
	reasonSeatTaken       = "seat already taken"
	reasonUnknownSeat     = "unknown seat"
	reasonAlreadyReleased = "reservation already released"
)

// reserve holds the seat for the saga and replies seat_reserved or
// seat_reservation_failed. "Seat taken" is a business outcome (a reply), not
// an error: the saga aborts, nothing is retried.
//
// Idempotent per saga, not just per message: the inbox drops a redelivered
// message, but the orchestrator may also send a NEW reserve command for the
// same saga (e.g. a timeout re-send). A saga that already holds its seat must
// get "reserved" again, not "taken" by itself.
func (s *Service) reserve(ctx context.Context, tx pgx.Tx, env message.Envelope, cmd messaging.ReserveSeat) error {
	heldSeat, status, found, err := reservationBySaga(ctx, tx, env.SagaID)
	if err != nil {
		return err
	}
	if found {
		if status == reservationHeld && heldSeat == cmd.SeatID {
			return s.reply(ctx, tx, env, messaging.EvtSeatReserved, "")
		}
		// Released (compensated) or a different seat: this saga's reservation
		// is over; a late command must not resurrect it.
		return s.reply(ctx, tx, env, messaging.EvtSeatReservationFailed, reasonAlreadyReleased)
	}

	taken, err := takeSeat(ctx, tx, cmd.SeatID)
	if err != nil {
		return err
	}
	if !taken {
		reason := reasonSeatTaken
		if exists, err := seatExists(ctx, tx, cmd.SeatID); err != nil {
			return err
		} else if !exists {
			reason = reasonUnknownSeat
		}
		s.log.InfoContext(ctx, "seat reservation failed", "saga_id", env.SagaID, "seat_id", cmd.SeatID, "reason", reason)
		return s.reply(ctx, tx, env, messaging.EvtSeatReservationFailed, reason)
	}

	if err := insertReservation(ctx, tx, env.SagaID, cmd.SeatID); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "seat reserved", "saga_id", env.SagaID, "seat_id", cmd.SeatID)
	return s.reply(ctx, tx, env, messaging.EvtSeatReserved, "")
}

// release is the compensation: free the seat this saga holds. It ALWAYS
// replies seat_released, even if there was nothing to release (never
// reserved, already released): a compensation that can fail on "nothing to
// undo" would leave the saga stuck in COMPENSATING forever.
//
// The seat comes from the reservation, not from the command: the
// reservation is the source of truth for what this saga holds.
func (s *Service) release(ctx context.Context, tx pgx.Tx, env message.Envelope, _ messaging.ReleaseSeat) error {
	seatID, released, err := releaseReservation(ctx, tx, env.SagaID)
	if err != nil {
		return err
	}
	if released {
		if err := freeSeat(ctx, tx, seatID); err != nil {
			return err
		}
		s.log.InfoContext(ctx, "seat released", "saga_id", env.SagaID, "seat_id", seatID)
	}
	return s.reply(ctx, tx, env, messaging.EvtSeatReleased, "")
}

// reply writes the answer to the orchestrator into the outbox, in the same
// transaction as the reservation change.
func (s *Service) reply(ctx context.Context, tx pgx.Tx, cause message.Envelope, replyType, reason string) error {
	r, err := messaging.NewReply(replyType, cause.SagaID, reason)
	if err != nil {
		return err
	}
	return outbox.Write(ctx, tx, messaging.TopicOrchestratorReplies, r)
}
