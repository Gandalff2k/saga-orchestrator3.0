// outbox-relay: publishes one service DB's outbox to Kafka. Deploy one
// Deployment per service DB, 2+ replicas each (shards are split between them).
package main

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/httpserver"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)

type cfg struct {
	config.Postgres
	config.Kafka
	config.Relay
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

		client, err := kafka.NewClient(c.Brokers, a.Config.ServiceName, kafka.ProducerOpts()...)
		if err != nil {
			return err
		}
		defer client.Close()

		relay := outbox.NewRelay(pool, client, c.Relay, a.Config.InstanceID, a.Log)

		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return a.ServeOps(ctx,
				httpserver.Check{Name: "postgres", Fn: pool.Ping},
				httpserver.Check{Name: "kafka", Fn: kafka.Ping(client)},
			)
		})
		g.Go(func() error { return relay.Run(ctx) })
		return g.Wait()
	})
}
