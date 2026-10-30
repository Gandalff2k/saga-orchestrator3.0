// Package worker wires a consumer process: postgres pool, inbox processor,
// batch consumer with DLQ, inbox janitor and the ops server.
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
	config.Inbox
}

// Run blocks until ctx is cancelled or a component fails. A consumer stopping
// with kafka.ErrDependencyDown ends the process with an error on purpose: the
// orchestrator (k8s) restarts it with backoff, and the uncommitted records are
// redelivered then.
func Run(ctx context.Context, a *app.App, cfg Config, handle inbox.Handler) error {
	pool, err := postgres.NewPool(ctx, cfg.DSN, cfg.MaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	proc, err := inbox.NewProcessor(pool, cfg.Group, handle, a.Log)
	if err != nil {
		return err
	}
	consumer, err := kafka.NewConsumer(cfg.Brokers, a.Config.ServiceName, cfg.Consumer, proc, a.Log)
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
	return g.Wait()
}
