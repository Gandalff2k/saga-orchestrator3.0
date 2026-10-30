package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Gandalff2k/saga3/internal/platform/message"
)

// ReplayGroup is the consumer group whose offsets mark how far a DLQ was replayed.
const ReplayGroup = "sagactl-dlq-replay"

// HeaderReplayedFrom marks a replayed record with the DLQ position it came from.
const HeaderReplayedFrom = "replayed-from"

// DLQEntry is one dead-lettered record as shown by sagactl dlq list.
type DLQEntry struct {
	Partition     int32  `json:"partition"`
	Offset        int64  `json:"offset"`
	Replayed      bool   `json:"replayed"`
	Key           string `json:"key"`
	Type          string `json:"type"`
	Error         string `json:"error"`
	OriginalTopic string `json:"original_topic"`
	Group         string `json:"consumer_group"`
	Attempts      string `json:"attempts"`
	FailedAt      string `json:"failed_at"`
}

// ListDLQ reads up to limit records of a DLQ topic and marks the ones already replayed.
func ListDLQ(ctx context.Context, brokers []string, dlqTopic string, limit int) ([]DLQEntry, error) {
	cl, adm, err := dlqClients(brokers)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	replayed, err := committed(ctx, adm, dlqTopic)
	if err != nil {
		return nil, err
	}
	recs, err := readRange(ctx, adm, brokers, dlqTopic, nil, limit)
	if err != nil {
		return nil, err
	}
	out := make([]DLQEntry, len(recs))
	for i, r := range recs {
		out[i] = DLQEntry{
			Partition: r.Partition, Offset: r.Offset,
			Replayed:      r.Offset < replayed[r.Partition],
			Key:           string(r.Key),
			Type:          headerValue(r, message.HeaderType),
			Error:         headerValue(r, HeaderDLQError),
			OriginalTopic: headerValue(r, HeaderDLQOriginalTopic),
			Group:         headerValue(r, HeaderDLQConsumerGroup),
			Attempts:      headerValue(r, HeaderDLQAttempts),
			FailedAt:      headerValue(r, HeaderDLQFailedAt),
		}
	}
	return out, nil
}

// ReplayDLQ republishes not yet replayed DLQ records to their original topic and returns how many.
func ReplayDLQ(ctx context.Context, brokers []string, dlqTopic string, limit int, dryRun bool) (int, error) {
	if !strings.HasSuffix(dlqTopic, DLQSuffix) {
		return 0, fmt.Errorf("%q is not a DLQ topic (must end with %s)", dlqTopic, DLQSuffix)
	}
	cl, adm, err := dlqClients(brokers)
	if err != nil {
		return 0, err
	}
	defer cl.Close()

	from, err := committed(ctx, adm, dlqTopic)
	if err != nil {
		return 0, err
	}
	recs, err := readRange(ctx, adm, brokers, dlqTopic, from, limit)
	if err != nil || len(recs) == 0 || dryRun {
		return len(recs), err
	}

	out := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		target := headerValue(r, HeaderDLQOriginalTopic)
		if target == "" {
			return 0, fmt.Errorf("%s@%d:%d has no %s header", dlqTopic, r.Partition, r.Offset, HeaderDLQOriginalTopic)
		}
		rr := &kgo.Record{Topic: target, Key: r.Key, Value: r.Value}
		for _, h := range r.Headers {
			if !strings.HasPrefix(h.Key, "dlq-") && h.Key != HeaderReplayedFrom {
				rr.Headers = append(rr.Headers, h)
			}
		}
		rr.Headers = append(rr.Headers, kgo.RecordHeader{Key: HeaderReplayedFrom,
			Value: []byte(fmt.Sprintf("%s@%d:%d", dlqTopic, r.Partition, r.Offset))})
		out = append(out, rr)
	}
	if err := cl.ProduceSync(ctx, out...).FirstErr(); err != nil {
		return 0, fmt.Errorf("replay produce: %w", err)
	}

	offsets := make(kadm.Offsets)
	for _, r := range recs {
		offsets.Add(kadm.Offset{Topic: r.Topic, Partition: r.Partition, At: r.Offset + 1, LeaderEpoch: -1})
	}
	resp, err := adm.CommitOffsets(ctx, ReplayGroup, offsets)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		return len(recs), fmt.Errorf("records replayed but progress not saved (next replay re-sends them): %w", err)
	}
	return len(recs), nil
}

func dlqClients(brokers []string) (*kgo.Client, *kadm.Client, error) {
	cl, err := NewClient(brokers, "sagactl", ProducerOpts()...)
	if err != nil {
		return nil, nil, err
	}
	return cl, kadm.NewClient(cl), nil
}

func committed(ctx context.Context, adm *kadm.Client, topic string) (map[int32]int64, error) {
	resp, err := adm.FetchOffsets(ctx, ReplayGroup)
	if err != nil && !errors.Is(err, kadm.ErrEmpty) {
		return nil, fmt.Errorf("fetch replay progress: %w", err)
	}
	out := map[int32]int64{}
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Err == nil && o.At >= 0 {
			out[o.Partition] = o.At
		}
	})
	return out, nil
}

func readRange(ctx context.Context, adm *kadm.Client, brokers []string, topic string, from map[int32]int64, limit int) ([]*kgo.Record, error) {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("end offsets: %w", err)
	}
	starts := map[int32]kgo.Offset{}
	pending := map[int32]int64{}
	ends.Each(func(lo kadm.ListedOffset) {
		start := from[lo.Partition]
		if lo.Err == nil && lo.Offset > start {
			starts[lo.Partition] = kgo.NewOffset().At(start)
			pending[lo.Partition] = lo.Offset
		}
	})
	if len(pending) == 0 {
		return nil, nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: starts}))
	if err != nil {
		return nil, fmt.Errorf("reader: %w", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(pending) > 0 && (limit <= 0 || len(out) < limit) {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("reading %s: %w (got %d records)", topic, err, len(out))
		}
		fetches.EachRecord(func(r *kgo.Record) {
			end, ok := pending[r.Partition]
			if !ok || r.Offset >= end || (limit > 0 && len(out) >= limit) {
				return
			}
			out = append(out, r)
			if r.Offset+1 >= end {
				delete(pending, r.Partition)
			}
		})
	}
	return out, nil
}

func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
