package orchestrator

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)


type Sweeper struct {
	pool     *pgxpool.Pool
	svc      *Service
	timeout  time.Duration
	interval time.Duration
	batch    int
	log      *slog.Logger
}


func NewSweeper(pool *pgxpool.Pool, svc *Service, stepTimeout, interval time.Duration, batch int, log *slog.Logger) *Sweeper {
	return &Sweeper{pool: pool, svc: svc, timeout: stepTimeout, interval: interval, batch: max(1, batch), log: log}
}


func (w *Sweeper) Run(ctx context.Context) error {
	w.svc.m.zeroTimeouts(ctx)
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		for ctx.Err() == nil { 
			n, err := w.SweepOnce(ctx)
			if err != nil {
				if ctx.Err() == nil {
					w.log.WarnContext(ctx, "saga sweep failed", "err", err)
				}
				break
			}
			if n < w.batch {
				break
			}
		}
	}
}


func (w *Sweeper) SweepOnce(ctx context.Context) (int, error) {
	var n int
	err := postgres.WithTx(ctx, w.pool, func(tx pgx.Tx) error {
		sagas, started, err := overdueForUpdate(ctx, tx, w.timeout, w.batch)
		if err != nil {
			return err
		}
		for i, saga := range sagas {
			tr := saga.OnTimeout()
			if tr.Ignored {
				continue
			}
			if err := w.svc.apply(ctx, tx, saga, tr, started[i]); err != nil {
				return err
			}
			w.svc.m.timedOut(ctx, saga.State)
			w.log.WarnContext(ctx, "saga timed out", "saga_id", saga.ID, "state", saga.State, "next", tr.Next)
			n++
		}
		return nil
	})
	return n, err
}
