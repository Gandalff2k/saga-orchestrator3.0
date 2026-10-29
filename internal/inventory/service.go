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
	"github.com/Gandalff2k/saga3/internal/platform/retry"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
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

// reserve holds the seat for the saga and replies seat_reserved / seat_reservation_failed.
//
// TODO(phase-4):
//
//	existing reservation for saga_id (HELD) -> reply seat_reserved again (idempotent)
//	UPDATE seats SET status='RESERVED' WHERE seat_id=$1 AND status='FREE'
//	  1 row -> INSERT reservations (HELD), reply EvtSeatReserved
//	  0 rows (taken or unknown seat) -> reply EvtSeatReservationFailed{reason}
//	unique violation on reservations_one_held_per_seat -> lost a race -> reservation failed
//	reply = outbox.Write(tx, TopicOrchestratorReplies, saga_id, ...)
func (s *Service) reserve(ctx context.Context, tx pgx.Tx, env message.Envelope, cmd messaging.ReserveSeat) error {
	return todo.Phase(4, "inventory reserve")
}

// release is the compensation: free the seat held by this saga (no-op if none).
//
// TODO(phase-4): UPDATE reservations SET status='RELEASED' WHERE saga_id=$1 AND status='HELD'
// RETURNING seat_id -> UPDATE seats SET status='FREE'; ALWAYS reply EvtSeatReleased
// (compensations must be idempotent and must not fail on "nothing to undo").
func (s *Service) release(ctx context.Context, tx pgx.Tx, env message.Envelope, cmd messaging.ReleaseSeat) error {
	return todo.Phase(4, "inventory release")
}
