// Command risk-worker consumes risk.commands: scores orders and closes review cases.
package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/worker"
	"github.com/Gandalff2k/saga3/internal/risk"
)

type cfg struct {
	worker.Config
	Risk               risk.Config
	Faults             faults.Config
	QueueStatsInterval time.Duration `env:"RISK_QUEUE_STATS_INTERVAL" envDefault:"15s"`
}

func main() {
	app.Main(func(ctx context.Context, a *app.App) error {
		c, err := config.Load[cfg]()
		if err != nil {
			return err
		}
		svc, err := risk.NewService(c.Risk, c.Faults, a.Log)
		if err != nil {
			return err
		}
		return worker.Run(ctx, a, c.Config, func(pool *pgxpool.Pool) (worker.Service, error) {
			queue, err := risk.NewQueueMonitor(pool, c.QueueStatsInterval, a.Log)
			if err != nil {
				return worker.Service{}, err
			}
			return worker.Service{Handle: svc.Handle, Background: []func(context.Context) error{queue.Run}}, nil
		})
	})
}
