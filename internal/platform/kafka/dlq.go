package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const maxDLQErrorLen = 1024

// DLQProducer publishes failed records to "<topic>.dlq".
type DLQProducer struct {
	client *kgo.Client
	group  string
}

// NewDLQProducer builds a DLQProducer.
func NewDLQProducer(client *kgo.Client, group string) *DLQProducer {
	return &DLQProducer{client: client, group: group}
}

// DeadLetter produces synchronously with acks=all, so the caller can commit the offset right after.
func (d *DLQProducer) DeadLetter(ctx context.Context, rec *kgo.Record, cause error, attempts int) error {
	dl := dlqRecord(rec, d.group, cause, attempts, time.Now())
	if err := d.client.ProduceSync(ctx, dl).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", dl.Topic, err)
	}
	return nil
}

func dlqRecord(rec *kgo.Record, group string, cause error, attempts int, now time.Time) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers)+7)
	for _, h := range rec.Headers {
		if !strings.HasPrefix(h.Key, "dlq-") {
			headers = append(headers, h)
		}
	}

	msg := cause.Error()
	if len(msg) > maxDLQErrorLen {
		msg = msg[:maxDLQErrorLen] + "…"
	}
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(msg)},
		kgo.RecordHeader{Key: HeaderDLQOriginalTopic, Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: HeaderDLQOriginalPart, Value: []byte(strconv.Itoa(int(rec.Partition)))},
		kgo.RecordHeader{Key: HeaderDLQOriginalOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		kgo.RecordHeader{Key: HeaderDLQConsumerGroup, Value: []byte(group)},
		kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
		kgo.RecordHeader{Key: HeaderDLQFailedAt, Value: []byte(now.UTC().Format(time.RFC3339Nano))},
	)
	return &kgo.Record{
		Topic:   DLQTopic(rec.Topic),
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: headers,
		Context: rec.Context,
	}
}
