// Package app is the shared main(): config, logger, telemetry, signal
// handling and exit code. Every cmd/* is `app.Main(func(ctx, a) error {...})`.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/httpserver"
	"github.com/Gandalff2k/saga3/internal/platform/logging"
	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
)

// Version is set at build time: -ldflags "-X github.com/Gandalff2k/saga3/internal/platform/app.Version=..."
var Version = "dev"

// App is what Main hands to a binary: its common config and the configured logger.
type App struct {
	Config config.Common
	Log    *slog.Logger
}

// Main runs fn and exits the process. ctx is cancelled on SIGINT/SIGTERM;
// fn must then stop accepting work, finish in-flight work and return.
func Main(fn func(ctx context.Context, a *App) error) {
	os.Exit(run(fn))
}

func run(fn func(ctx context.Context, a *App) error) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load[config.Common]()
	if err != nil {
		slog.Error("load config", "err", err)
		return 2
	}

	log := logging.New(os.Stdout, cfg.LogLevel, cfg.LogFormat, cfg.ServiceName, Version)
	slog.SetDefault(log)

	shutdownTelemetry, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName: cfg.ServiceName,
		Version:     Version,
		Environment: cfg.Environment,
		InstanceID:  cfg.InstanceID,
		Endpoint:    cfg.OTLPEndpoint,
	})
	if err != nil {
		log.Error("init telemetry", "err", err)
		return 2
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := shutdownTelemetry(sctx); err != nil {
			log.Warn("telemetry shutdown", "err", err)
		}
	}()

	log.Info("starting", "instance", cfg.InstanceID, "env", cfg.Environment)
	err = fn(ctx, &App{Config: cfg, Log: log})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("exited with error", "err", err)
		return 1
	}
	log.Info("stopped")
	return 0
}

// ServeOps runs the ops server (/healthz, /readyz, pprof) until ctx is done.
func (a *App) ServeOps(ctx context.Context, checks ...httpserver.Check) error {
	srv := &http.Server{
		Addr:              a.Config.OpsAddr,
		Handler:           httpserver.OpsHandler(a.Config.Environment != "production", checks...),
		ReadHeaderTimeout: httpserver.DefaultReadHeaderTimeout,
	}
	return httpserver.Run(ctx, srv, a.Config.ShutdownTimeout, a.Log)
}
