// Command inventory-worker consumes inventory.commands (reserve and release seat).
package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

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
		return worker.Run(ctx, a, c.Config, func(*pgxpool.Pool) (worker.Service, error) {
			return worker.Service{Handle: inventory.NewService(c.Faults, a.Log).Handle}, nil
		})
	})
}
