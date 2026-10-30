// orchestrator-api: public HTTP API. Writes orders + sagas + the first command
// to the orchestrator DB (outbox). Stateless, scale with HPA on CPU/RPS.
package main

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/httpserver"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)

type cfg struct {
	config.Postgres
	HTTPAddr        string        `env:"HTTP_ADDR" envDefault:":8080"`
	SagaStepTimeout time.Duration `env:"SAGA_STEP_TIMEOUT" envDefault:"30s"`
}

func main() {
	app.Main(func(ctx context.Context, a *app.App) error {
		c, err := config.Load[cfg]()
		if err != nil {
			return err
		}

		pool, err := postgres.NewPool(ctx, c.DSN, c.MaxConns)
		if err != nil {
			return err
		}
		defer pool.Close()

		svc, err := orchestrator.NewService(pool, c.SagaStepTimeout, a.Log)
		if err != nil {
			return err
		}
		api := &http.Server{
			Addr:              c.HTTPAddr,
			Handler:           orchestrator.NewHTTPHandler(svc, a.Log),
			ReadHeaderTimeout: httpserver.DefaultReadHeaderTimeout,
		}

		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error { return a.ServeOps(ctx, httpserver.Check{Name: "postgres", Fn: pool.Ping}) })
		g.Go(func() error { return httpserver.Run(ctx, api, a.Config.ShutdownTimeout, a.Log) })
		return g.Wait()
	})
}
