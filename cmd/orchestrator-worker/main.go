// Command orchestrator-worker consumes orchestrator.replies, advances sagas and runs the timeout sweeper.
package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/worker"
)

type cfg struct {
	worker.Config
	// SagaStepTimeout: a saga that hasn't moved this long is compensated by
	// the sweeper. Only this deployment reads it (D-604).
	SagaStepTimeout time.Duration `env:"SAGA_STEP_TIMEOUT" envDefault:"30s"`
	SweepInterval   time.Duration `env:"SAGA_SWEEP_INTERVAL" envDefault:"5s"`
	SweepBatch      int           `env:"SAGA_SWEEP_BATCH" envDefault:"100"`
}

func main() {
	app.Main(func(ctx context.Context, a *app.App) error {
		c, err := config.Load[cfg]()
		if err != nil {
			return err
		}
		return worker.Run(ctx, a, c.Config, func(pool *pgxpool.Pool) (worker.Service, error) {
			svc, err := orchestrator.NewService(pool, a.Log)
			if err != nil {
				return worker.Service{}, err
			}
			sweeper := orchestrator.NewSweeper(pool, svc, c.SagaStepTimeout, c.SweepInterval, c.SweepBatch, a.Log)
			return worker.Service{Handle: svc.HandleReply, Background: []func(context.Context) error{sweeper.Run}}, nil
		})
	})
}
