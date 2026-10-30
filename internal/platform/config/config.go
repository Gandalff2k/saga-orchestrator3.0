
package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

func Load[T any]() (T, error) {
	var cfg T
	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

type Common struct {
	ServiceName string `env:"SERVICE_NAME,required"`
	Environment string `env:"APP_ENV" envDefault:"local"`
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat   string `env:"LOG_FORMAT" envDefault:"json"` 
	InstanceID string `env:"HOSTNAME" envDefault:"local"`
	OpsAddr         string        `env:"OPS_ADDR" envDefault:":8081"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"25s"`
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
}


type Postgres struct {
	DSN      string `env:"POSTGRES_DSN,required"`
	MaxConns int32  `env:"POSTGRES_MAX_CONNS" envDefault:"10"`
}

type Kafka struct {
	Brokers []string `env:"KAFKA_BROKERS,required" envSeparator:","`
	TopicPrefix string `env:"KAFKA_TOPIC_PREFIX"`
}

type Consumer struct {
	Group  string   `env:"CONSUMER_GROUP,required"`
	Topics []string `env:"CONSUMER_TOPICS,required" envSeparator:","`
	MaxPollRecords int `env:"CONSUMER_MAX_POLL_RECORDS" envDefault:"500"`
	Concurrency int `env:"CONSUMER_CONCURRENCY" envDefault:"8"`
	SessionTimeout time.Duration `env:"CONSUMER_SESSION_TIMEOUT" envDefault:"15s"`
	Retry retry.Policy `envPrefix:"CONSUMER_RETRY_"`
}

type Inbox struct {
	Retention       time.Duration `env:"INBOX_RETENTION" envDefault:"336h"` // 14d
	JanitorInterval time.Duration `env:"INBOX_JANITOR_INTERVAL" envDefault:"10m"`
}

type Relay struct {
	BatchSize int `env:"RELAY_BATCH_SIZE" envDefault:"500"`
	PollInterval time.Duration `env:"RELAY_POLL_INTERVAL" envDefault:"200ms"`
	LeaseTTL time.Duration `env:"RELAY_LEASE_TTL" envDefault:"15s"`
	PublishTimeout time.Duration `env:"RELAY_PUBLISH_TIMEOUT" envDefault:"4s"`
	Concurrency int `env:"RELAY_CONCURRENCY" envDefault:"4"`
	Retention       time.Duration `env:"RELAY_RETENTION" envDefault:"24h"`
	JanitorInterval time.Duration `env:"RELAY_JANITOR_INTERVAL" envDefault:"1m"`
	Retry retry.Policy `envPrefix:"RELAY_RETRY_"`
}

func (r Relay) Validate() error {
	switch {
	case r.BatchSize <= 0 || r.Concurrency <= 0:
		return errors.New("relay: RELAY_BATCH_SIZE and RELAY_CONCURRENCY must be positive")
	case r.PollInterval <= 0 || r.JanitorInterval <= 0:
		return errors.New("relay: intervals must be positive")
	case r.PublishTimeout <= 0 || r.PublishTimeout >= r.LeaseTTL/3:
		return fmt.Errorf("relay: RELAY_PUBLISH_TIMEOUT (%s) must be in (0, RELAY_LEASE_TTL/3 = %s)",
			r.PublishTimeout, r.LeaseTTL/3)
	}
	return nil
}

type Topics struct {
	Partitions        int32 `env:"TOPIC_PARTITIONS" envDefault:"12"`
	ReplicationFactor int16 `env:"TOPIC_REPLICATION_FACTOR" envDefault:"3"`
}
