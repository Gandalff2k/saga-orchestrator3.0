package outbox

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Gandalff2k/saga3/internal/platform/message"
)

func TestHeaders(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	env := message.Envelope{ID: uuid.New(), Type: "x.y.v1", SagaID: uuid.New()}

	t.Run("without span: ids only", func(t *testing.T) {
		h := Headers(context.Background(), env)
		if h[message.HeaderID] != env.ID.String() || h[message.HeaderType] != "x.y.v1" {
			t.Fatalf("headers = %v", h)
		}
		if _, ok := h["traceparent"]; ok {
			t.Fatalf("unexpected traceparent without a span: %v", h)
		}
	})

	t.Run("with span: traceparent carries the caller's trace", func(t *testing.T) {
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
			TraceFlags: trace.FlagsSampled,
		})
		ctx := trace.ContextWithSpanContext(context.Background(), sc)

		h := Headers(ctx, env)
		want := "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
		if h["traceparent"] != want {
			t.Fatalf("traceparent = %q, want %q", h["traceparent"], want)
		}
	})
}

func TestWrite_RejectsIncompleteEnvelope(t *testing.T) {
	// Validation happens before touching tx, so a nil tx is fine here.
	cases := map[string]struct {
		topic string
		env   message.Envelope
	}{
		"no topic": {"", message.Envelope{ID: uuid.New(), Type: "t", SagaID: uuid.New()}},
		"no id":    {"t", message.Envelope{Type: "t", SagaID: uuid.New()}},
		"no saga":  {"t", message.Envelope{ID: uuid.New(), Type: "t"}},
		"no type":  {"t", message.Envelope{ID: uuid.New(), SagaID: uuid.New()}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Write(context.Background(), nil, c.topic, c.env); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
