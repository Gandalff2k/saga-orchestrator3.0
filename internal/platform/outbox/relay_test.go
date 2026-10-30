package outbox

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
)

func TestAcknowledgedPrefix(t *testing.T) {
	rows := []row{{Seq: 10, Topic: "t"}, {Seq: 11, Topic: "t"}, {Seq: 12, Topic: "t"}, {Seq: 13, Topic: "t"}}
	recs := make([]*kgo.Record, len(rows))
	for i := range recs {
		recs[i] = &kgo.Record{}
	}
	boom := errors.New("boom")

	results := func(errs map[int]error) kgo.ProduceResults {
		var out kgo.ProduceResults
		for i := len(recs) - 1; i >= 0; i-- {
			if err, skip := errs[i]; skip && err == nil {
				continue
			}
			out = append(out, kgo.ProduceResult{Record: recs[i], Err: errs[i]})
		}
		return out
	}

	tests := []struct {
		name    string
		errs    map[int]error
		want    []int64
		wantErr bool
	}{
		{"all acknowledged", nil, []int64{10, 11, 12, 13}, false},
		{"first fails: nothing marked", map[int]error{0: boom}, []int64{}, true},
		{"middle fails: later successes NOT marked (no reorder)", map[int]error{2: boom}, []int64{10, 11}, true},
		{"missing result counts as failure", map[int]error{1: nil}, []int64{10}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := acknowledgedPrefix(rows, recs, results(tt.errs))
			if !slices.Equal(got, tt.want) || (err != nil) != tt.wantErr {
				t.Fatalf("got %v, err %v; want %v, err=%v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestToRecord(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	const traceparent = "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
	rec := toRecord(context.Background(), row{
		Seq: 1, Topic: "inventory.commands", Key: "saga-1", Payload: []byte(`{}`),
		Headers: map[string]string{"message-type": "x", "message-id": "id-1", "traceparent": traceparent},
	}, kafka.Namespace("staging-"))

	if rec.Topic != "staging-inventory.commands" || string(rec.Key) != "saga-1" {
		t.Fatalf("record = %+v", rec)
	}
	var keys []string
	for _, h := range rec.Headers {
		keys = append(keys, h.Key)
	}
	if !slices.Equal(keys, []string{"message-id", "message-type", "traceparent"}) {
		t.Errorf("header keys = %v, want sorted and complete", keys)
	}
	sc := trace.SpanContextFromContext(rec.Context)
	if sc.TraceID().String() != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("record context trace = %v, want the stored one", sc.TraceID())
	}
}

func TestRelayConfigValidate(t *testing.T) {
	ok := config.Relay{BatchSize: 1, Concurrency: 1, PollInterval: 1, JanitorInterval: 1,
		LeaseTTL: 15e9, PublishTimeout: 4e9}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := ok
	bad.PublishTimeout = 5e9 
	if err := bad.Validate(); err == nil {
		t.Fatal("PublishTimeout >= LeaseTTL/3 must be rejected")
	}
}
