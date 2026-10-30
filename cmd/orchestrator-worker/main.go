// orchestrator-worker: consumes orchestrator.replies and advances sagas.
package main

import (
	"context"
	"time"

	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/worker"
)

type cfg struct {
	worker.Config
	SagaStepTimeout time.Duration `env:"SAGA_STEP_TIMEOUT" envDefault:"30s"`
}

func main() {
	app.Main(func(ctx context.Context, a *app.App) error {
		c, err := config.Load[cfg]()
		if err != nil {
			return err
		}
		// HandleReply runs inside the inbox tx; the pool is only needed by the API side.
		svc, err := orchestrator.NewService(nil, c.SagaStepTimeout, a.Log)
		if err != nil {
			return err
		}
		return worker.Run(ctx, a, c.Config, svc.HandleReply)
	})
}
