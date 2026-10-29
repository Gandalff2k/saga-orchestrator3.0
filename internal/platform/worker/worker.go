// Package worker wires a consumer process: postgres pool, kafka client,
// inbox processor, batch consumer with DLQ, and the ops server.
// Every *-worker binary is "load config, build service, worker.Run".
package worker

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/httpserver"
	"github.com/Gandalff2k/saga3/internal/platform/inbox"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)

// Config is everything a worker needs; embed it in the binary's own config.
type Config struct {
	config.Postgres
	config.Kafka
	config.Consumer
}

// Run blocks until ctx is cancelled or a component fails.
func Run(ctx context.Context, a *app.App, cfg Config, handle inbox.Handler) error {
	pool, err := postgres.NewPool(ctx, cfg.DSN, cfg.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	// One client both consumes and produces to the DLQ.
	opts := append(kafka.ProducerOpts(), kafka.ConsumerOpts(cfg.Consumer)...)
	client, err := kafka.NewClient(cfg.Brokers, a.Config.ServiceName, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	proc := inbox.NewProcessor(pool, cfg.Group, handle, a.Log)
	dlq := kafka.NewDLQProducer(client, cfg.Group)
	consumer := kafka.NewConsumer(client, proc, dlq, cfg.Consumer, a.Log)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return a.ServeOps(ctx,
			httpserver.Check{Name: "postgres", Fn: pool.Ping},
			httpserver.Check{Name: "kafka", Fn: kafka.Ping(client)},
		)
	})
	g.Go(func() error {
		if err := consumer.Run(ctx); err != nil {
			return fmt.Errorf("consumer: %w", err)
		}
		return nil
	})
	return g.Wait()
}
