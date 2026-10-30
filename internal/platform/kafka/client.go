package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"

	"github.com/Gandalff2k/saga3/internal/platform/config"
)


func NewClient(brokers []string, clientID string, opts ...kgo.Opt) (*kgo.Client, error) {
	k := kotel.NewKotel(
		kotel.WithTracer(kotel.NewTracer()), 
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

func ProducerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.RecordRetries(10),
	}
}


func consumerOpts(cfg config.Consumer) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500 * time.Millisecond),
	}
	if cfg.SessionTimeout > 0 { 
		opts = append(opts,
			kgo.SessionTimeout(cfg.SessionTimeout),
			kgo.HeartbeatInterval(cfg.SessionTimeout/5),
		)
	}
	return opts
}

func Ping(cl *kgo.Client) func(ctx context.Context) error {
	return func(ctx context.Context) error { return cl.Ping(ctx) }
}
