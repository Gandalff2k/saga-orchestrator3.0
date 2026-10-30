package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// EnsureTopics creates missing topics.
func EnsureTopics(ctx context.Context, cl *kgo.Client, specs []TopicSpec, log *slog.Logger) error {
	adm := kadm.NewClient(cl)
	for _, s := range specs {
		resp, err := adm.CreateTopic(ctx, s.Partitions, s.ReplicationFactor, s.Configs, s.Name)
		if err == nil {
			err = resp.Err
		}
		switch {
		case err == nil:
			log.Info("topic created", "topic", s.Name, "partitions", s.Partitions, "rf", s.ReplicationFactor)
		case errors.Is(err, kerr.TopicAlreadyExists):
			log.Info("topic exists", "topic", s.Name)
		default:
			return fmt.Errorf("create topic %s: %w", s.Name, err)
		}
	}
	return waitReady(ctx, cl, specs)
}

const topicReadyTimeout = 30 * time.Second

func waitReady(ctx context.Context, cl *kgo.Client, specs []TopicSpec) error {
	ctx, cancel := context.WithTimeout(ctx, topicReadyTimeout)
	defer cancel()

	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		missing, err := notReady(ctx, cl, names)
		if err == nil && len(missing) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("topics not ready %v: %w", missing, errors.Join(ctx.Err(), err))
		case <-tick.C:
		}
	}
}

func notReady(ctx context.Context, cl *kgo.Client, names []string) ([]string, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	for _, n := range names {
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(n)
		req.Topics = append(req.Topics, t)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return names, fmt.Errorf("metadata: %w", err)
	}

	ready := map[string]bool{}
	for _, t := range resp.Topics {
		if t.Topic == nil || t.ErrorCode != 0 || len(t.Partitions) == 0 {
			continue
		}
		ok := true
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 || p.Leader < 0 {
				ok = false
				break
			}
		}
		ready[*t.Topic] = ok
	}
	var missing []string
	for _, n := range names {
		if !ready[n] {
			missing = append(missing, n)
		}
	}
	return missing, nil
}
