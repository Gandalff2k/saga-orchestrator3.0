// inventory-worker: consumes inventory.commands (reserve / release seat).
package main

import (
	"context"

	"github.com/Gandalff2k/saga3/internal/inventory"
	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/worker"
)

type cfg struct {
	worker.Config
	Faults faults.Config
}

func main() {
	app.Main(func(ctx context.Context, a *app.App) error {
		c, err := config.Load[cfg]()
		if err != nil {
			return err
		}
		svc := inventory.NewService(c.Faults, a.Log)
		return worker.Run(ctx, a, c.Config, svc.Handle)
	})
}
