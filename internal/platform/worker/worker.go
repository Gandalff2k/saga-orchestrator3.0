// Package worker wires a consumer process: pool, inbox, batch consumer, DLQ, janitor, ops server.
package worker

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/jackc/pgx/v5/pgxpool"

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
	config.Inbox
}

// Service is what a worker binary provides: its message handler and optional background loops.
type Service struct {
	Handle     inbox.Handler
	Background []func(ctx context.Context) error
}

// Run blocks until ctx is cancelled or a component fails.
func Run(ctx context.Context, a *app.App, cfg Config, build func(pool *pgxpool.Pool) (Service, error)) error {
	pool, err := postgres.NewPool(ctx, cfg.DSN, cfg.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	svc, err := build(pool)
	if err != nil {
		return err
	}
	proc, err := inbox.NewProcessor(pool, cfg.Group, svc.Handle, a.Log)
	if err != nil {
		return err
	}
	consumerCfg := cfg.Consumer
	consumerCfg.Topics = kafka.Namespace(cfg.TopicPrefix).Topics(cfg.Topics)
	consumer, err := kafka.NewConsumer(cfg.Brokers, a.Config.ServiceName, consumerCfg, proc, a.Log)
	if err != nil {
		return err
	}
	janitor := inbox.NewJanitor(pool, cfg.Inbox, a.Log)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return a.ServeOps(ctx,
			httpserver.Check{Name: "postgres", Fn: pool.Ping},
			httpserver.Check{Name: "kafka", Fn: consumer.Ping},
		)
	})
	g.Go(func() error {
		if err := consumer.Run(ctx); err != nil {
			return fmt.Errorf("consumer: %w", err)
		}
		return nil
	})
	g.Go(func() error { return janitor.Run(ctx) })
	for _, bg := range svc.Background {
		g.Go(func() error { return bg(ctx) })
	}
	return g.Wait()
}
