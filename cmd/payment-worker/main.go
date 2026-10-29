// payment-worker: consumes payment.commands (charge).
package main

import (
	"context"

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
		svc := payment.NewService(c.Payment, a.Log)
		return worker.Run(ctx, a, c.Config, svc.Handle)
	})
}
