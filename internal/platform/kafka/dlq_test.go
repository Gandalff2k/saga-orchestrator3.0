package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDLQRecord(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	orig := &kgo.Record{
		Topic: "inventory.commands", Partition: 4, Offset: 99,
		Key: []byte("saga-1"), Value: []byte(`{"x":1}`),
		Headers: []kgo.RecordHeader{
			{Key: "traceparent", Value: []byte("00-abc")},
			{Key: "dlq-error", Value: []byte("an earlier failure")}, // replayed record that failed again
		},
	}

	dl := dlqRecord(orig, "inventory", errors.New(strings.Repeat("x", 5000)), 3, now)

	if dl.Topic != "inventory.commands.dlq" || string(dl.Key) != "saga-1" || string(dl.Value) != `{"x":1}` {
		t.Fatalf("record = %s %s %s", dl.Topic, dl.Key, dl.Value)
	}
	h := map[string][]string{}
	for _, x := range dl.Headers {
		h[x.Key] = append(h[x.Key], string(x.Value))
	}
	want := map[string]string{
		"traceparent":           "00-abc",
		HeaderDLQOriginalTopic:  "inventory.commands",
		HeaderDLQOriginalPart:   "4",
		HeaderDLQOriginalOffset: "99",
		HeaderDLQConsumerGroup:  "inventory",
		HeaderDLQAttempts:       "3",
		HeaderDLQFailedAt:       "2026-10-30T12:00:00Z",
	}
	for k, v := range want {
		if len(h[k]) != 1 || h[k][0] != v {
			t.Errorf("header %s = %v, want [%s]", k, h[k], v)
		}
	}
	if len(h[HeaderDLQError]) != 1 {
		t.Fatalf("dlq-error must be replaced, not duplicated: %d values", len(h[HeaderDLQError]))
	}
	if n := len(h[HeaderDLQError][0]); n > maxDLQErrorLen+4 {
		t.Errorf("dlq-error not truncated: %d bytes", n)
	}
}
