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

// EnsureTopics creates missing topics. Existing topics are left untouched
// (partition count can only grow, and growing it remaps keys -> breaks
// per-saga ordering for in-flight sagas; do it deliberately, not here).
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

// topicReadyTimeout bounds waitReady; metadata normally converges in well under a second.
const topicReadyTimeout = 30 * time.Second

// waitReady blocks until every topic is visible in metadata with a leader for
// every partition. CreateTopics returns once the CONTROLLER has the topic;
// brokers learn about it asynchronously, so a producer/consumer started right
// after setup could otherwise hit UNKNOWN_TOPIC_OR_PARTITION.
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

// notReady asks a broker directly. Not kadm.ListTopics: kadm serves metadata
// from the client's cache (fresh for MetadataMinAge = 5s), so polling it every
// 200ms would see the same stale "topic missing" answer for 5 seconds.
func notReady(ctx context.Context, cl *kgo.Client, names []string) ([]string, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false // checking must never create a topic
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
