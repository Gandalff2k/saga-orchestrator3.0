package orchestrator

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestHTTP_SpanNamedByRoute(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	h := NewHTTPHandler(&fakeService{order: Order{ID: uuid.New()}}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, tc := range []struct{ method, path, want string }{
		{http.MethodGet, "/orders/" + uuid.NewString(), "GET /orders/{id}"},
		{http.MethodGet, "/nope", "GET unmatched"},
	} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, nil))

		spans := rec.Ended()
		if got := spans[len(spans)-1].Name(); got != tc.want {
			t.Errorf("%s %s: span name %q, want %q", tc.method, tc.path, got, tc.want)
		}
	}
}
