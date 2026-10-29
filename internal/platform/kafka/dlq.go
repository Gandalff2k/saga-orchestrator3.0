package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

// DLQProducer publishes failed records to "<topic>.dlq".
//
// The DLQ record keeps the original key, value and headers (incl. traceparent,
// so the failure shows up in the original trace) and adds the
// HeaderDLQ* headers: error, original topic/partition/offset,
// consumer group, attempts, failed-at. `sagactl dlq replay` uses them to
// re-publish records to the original topic after a fix.
type DLQProducer struct {
	client *kgo.Client
	group  string
}

// NewDLQProducer builds a DLQProducer. group is recorded on every dead letter,
// so a replay knows which consumer gave up on it.
func NewDLQProducer(client *kgo.Client, group string) *DLQProducer {
	return &DLQProducer{client: client, group: group}
}

// DeadLetter produces synchronously (acks=all): the caller commits the
// original offset right after, so the DLQ write must be durable first.
//
// TODO(phase-3): build record (DLQTopic(rec.Topic), same key), ProduceSync,
// increment saga_dlq_records_total{topic,group}, mark the current span as error.
func (d *DLQProducer) DeadLetter(ctx context.Context, rec *kgo.Record, cause error, attempts int) error {
	_, _, _ = rec, cause, attempts
	return todo.Phase(3, "dead-letter producer")
}
