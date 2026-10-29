// Package httpserver runs HTTP servers with graceful shutdown and provides
// the ops endpoints (/healthz, /readyz) used by k8s probes.
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"
)

// DefaultReadHeaderTimeout guards against slowloris; set it on every http.Server.
const DefaultReadHeaderTimeout = 5 * time.Second

// Run serves srv until ctx is cancelled, then shuts it down gracefully.
func Run(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration, log *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server %s: %w", srv.Addr, err)
	case <-ctx.Done():
	}

	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(sctx)
}

// Check is a named readiness dependency (postgres ping, kafka metadata, ...).
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// OpsHandler serves:
//
//	/healthz  liveness: the process is up (never checks dependencies, or a DB
//	          blip would make k8s restart every pod at once)
//	/readyz   readiness: all checks pass; failing = pod removed from Service endpoints
//	/debug/pprof/*  when enablePprof
func OpsHandler(enablePprof bool, checks ...Check) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		status, failed := http.StatusOK, map[string]string{}
		for _, c := range checks {
			if err := c.Fn(ctx); err != nil {
				status, failed[c.Name] = http.StatusServiceUnavailable, err.Error()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": status == http.StatusOK, "failed": failed})
	})
	if enablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}
