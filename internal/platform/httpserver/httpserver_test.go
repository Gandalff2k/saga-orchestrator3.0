package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOps_HealthzNeverChecksDependencies(t *testing.T) {
	called := false
	h := OpsHandler(false, Check{Name: "db", Fn: func(context.Context) error { called = true; return errors.New("down") }})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || called {
		t.Fatalf("liveness = %d, dependency checked = %v; liveness must not depend on the DB", rec.Code, called)
	}
}

func TestOps_Readyz(t *testing.T) {
	ok := Check{Name: "kafka", Fn: func(context.Context) error { return nil }}
	down := Check{Name: "postgres", Fn: func(context.Context) error { return errors.New("connection refused") }}

	tests := []struct {
		name   string
		checks []Check
		want   int
		failed map[string]string
	}{
		{"all ready", []Check{ok}, http.StatusOK, map[string]string{}},
		{"one down", []Check{ok, down}, http.StatusServiceUnavailable, map[string]string{"postgres": "connection refused"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			OpsHandler(false, tt.checks...).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
			var body struct {
				Ready  bool              `json:"ready"`
				Failed map[string]string `json:"failed"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if rec.Code != tt.want || len(body.Failed) != len(tt.failed) || body.Failed["postgres"] != tt.failed["postgres"] {
				t.Fatalf("readyz = %d %+v, want %d %v", rec.Code, body, tt.want, tt.failed)
			}
		})
	}
}

func TestOps_PprofOnlyWhenEnabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		rec := httptest.NewRecorder()
		OpsHandler(enabled).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/debug/pprof/", nil))
		if got := rec.Code == http.StatusOK; got != enabled {
			t.Errorf("pprof enabled=%v -> status %d", enabled, rec.Code)
		}
	}
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{Addr: addr, Handler: OpsHandler(false), ReadHeaderTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, srv, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil))) }()

	waitUp(t, "http://"+addr+"/healthz")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil after graceful shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

func waitUp(t *testing.T, url string) {
	t.Helper()
	for range 50 {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never came up", url)
}
