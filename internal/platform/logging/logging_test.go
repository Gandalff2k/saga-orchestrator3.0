package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestLogger_AddsTraceIDsFromContext(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "debug", "json", "inventory-worker", "v1")

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}, TraceFlags: trace.FlagsSampled,
	})
	log.InfoContext(trace.ContextWithSpanContext(context.Background(), sc), "seat reserved", "seat_id", 7)
	log.InfoContext(context.Background(), "no span")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("got %d lines", len(lines))
	}
	var withSpan, without map[string]any
	_ = json.Unmarshal(lines[0], &withSpan)
	_ = json.Unmarshal(lines[1], &without)

	if withSpan["trace_id"] != "0102030405060708090a0b0c0d0e0f10" || withSpan["span_id"] != "0102030405060708" {
		t.Errorf("trace ids missing: %v", withSpan)
	}
	if withSpan["service"] != "inventory-worker" || withSpan["version"] != "v1" || withSpan["seat_id"] != float64(7) {
		t.Errorf("fields missing: %v", withSpan)
	}
	if _, ok := without["trace_id"]; ok {
		t.Errorf("trace_id on a line without a span: %v", without)
	}
}

func TestLogger_LevelAndFormat(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "warn", "text", "svc", "dev")
	log.Info("hidden")
	log.Warn("shown")
	if out := buf.String(); bytes.Contains([]byte(out), []byte("hidden")) || !bytes.Contains([]byte(out), []byte("level=WARN msg=shown")) {
		t.Fatalf("output = %q", out)
	}

	buf.Reset()
	New(&buf, "nonsense", "json", "svc", "dev").Debug("dropped")
	if buf.Len() != 0 {
		t.Fatal("unknown level must fall back to info, not debug")
	}
}
