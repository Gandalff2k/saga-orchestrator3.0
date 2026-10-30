// Package kafka wraps franz-go (kgo): client construction with sane
// production defaults, topic administration, the batch consumer and the DLQ.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"

	"github.com/Gandalff2k/saga3/internal/platform/config"
)

// NewClient builds a traced, metered kgo client. Combine with ProducerOpts.
// Consumer clients are built by NewConsumer only (it must own the rebalance hooks).
func NewClient(brokers []string, clientID string, opts ...kgo.Opt) (*kgo.Client, error) {
	k := kotel.NewKotel(
		kotel.WithTracer(kotel.NewTracer()), // global provider + W3C propagator via headers
		kotel.WithMeter(kotel.NewMeter()),
	)

	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.WithHooks(k.Hooks()...),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return cl, nil
}

// ProducerOpts returns options for durable producing, ordered per key.
//   - acks=all and idempotence (kgo default): no loss, no duplicates from producer retries
//   - with min.insync.replicas=2 on RF=3 topics, a write survives one broker loss
//   - default partitioner = murmur2 of the key, same as the Java client, so
//     saga_id -> partition mapping is stable across languages
func ProducerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.RecordRetries(10),
	}
}

// consumerOpts returns consumer-group options: manual commits, rebalance
// blocked while a poll is being processed, cooperative-sticky assignment
// (scale out/in without stop-the-world rebalances).
func consumerOpts(cfg config.Consumer) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500 * time.Millisecond),
	}
}

// Ping checks broker connectivity (used by /readyz).
func Ping(cl *kgo.Client) func(ctx context.Context) error {
	return func(ctx context.Context) error { return cl.Ping(ctx) }
}
