package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Gandalff2k/saga3/internal/messaging"
)

// EnsureTopics creates missing topics. Existing topics are left untouched
// (partition count can only grow, and growing it remaps keys -> breaks
// per-saga ordering for in-flight sagas; do it deliberately, not here).
func EnsureTopics(ctx context.Context, cl *kgo.Client, specs []messaging.TopicSpec, log *slog.Logger) error {
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
	return nil
}
