// Command risk-agent investigates manual review cases with an LLM and stores a recommendation for the reviewer.
package main

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/Gandalff2k/saga3/internal/agent"
	"github.com/Gandalff2k/saga3/internal/llm"
	"github.com/Gandalff2k/saga3/internal/platform/app"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/httpserver"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)

type cfg struct {
	config.Postgres
	LLM    llm.Config
	Limits agent.Limits
	Runner agent.RunnerConfig
}

func main() {
	app.Main(func(ctx context.Context, a *app.App) error {
		c, err := config.Load[cfg]()
		if err != nil {
			return err
		}
		client, err := llm.NewOpenAI(c.LLM)
		if err != nil {
			return err
		}
		pool, err := postgres.NewPool(ctx, c.DSN, c.MaxConns)
		if err != nil {
			return err
		}
		defer pool.Close()

		inv := agent.NewInvestigator(client, agent.DBEvidence{Pool: pool}, c.Limits)
		runner, err := agent.NewRunner(pool, inv, c.Runner, a.Config.InstanceID, a.Log)
		if err != nil {
			return err
		}
		a.Log.Info("agent ready", "model", c.LLM.Model, "base_url", c.LLM.BaseURL)

		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error { return a.ServeOps(ctx, httpserver.Check{Name: "postgres", Fn: pool.Ping}) })
		g.Go(func() error { return runner.Run(ctx) })
		return g.Wait()
	})
}
