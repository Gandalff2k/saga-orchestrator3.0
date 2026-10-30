package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gandalff2k/saga3/internal/payment"
	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/worker"
)

type cfg struct {
	worker.Config
	Payment payment.Config
}

func main() {
	app.Main(func(ctx context.Context, a *app.App) error {
		c, err := config.Load[cfg]()
		if err != nil {
			return err
		}
		return worker.Run(ctx, a, c.Config, func(*pgxpool.Pool) (worker.Service, error) {
			return worker.Service{Handle: payment.NewService(c.Payment, a.Log).Handle}, nil
		})
	})
}
