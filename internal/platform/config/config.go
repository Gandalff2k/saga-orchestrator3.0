// Package config loads 12-factor configuration from environment variables.
// Each binary composes the blocks it needs into its own struct and calls Load.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// Load parses environment variables into a new T.
func Load[T any]() (T, error) {
	var cfg T
	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

// Common is shared by every binary.
type Common struct {
	ServiceName string `env:"SERVICE_NAME,required"`
	Environment string `env:"APP_ENV" envDefault:"local"`
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat   string `env:"LOG_FORMAT" envDefault:"json"` // json | text
	// InstanceID identifies this replica (pod name in k8s, container hostname in compose).
	InstanceID string `env:"HOSTNAME" envDefault:"local"`
	// OpsAddr serves /healthz and /readyz (and pprof outside production).
	OpsAddr         string        `env:"OPS_ADDR" envDefault:":8081"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"25s"` // keep < k8s terminationGracePeriodSeconds
	// OTLPEndpoint empty = telemetry disabled (no-op providers). The OTel SDK
	// also reads the standard OTEL_* variables itself.
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
}

// Postgres configures the connection pool. Size MaxConns so that
// replicas × MaxConns stays below the server's max_connections.
type Postgres struct {
	DSN      string `env:"POSTGRES_DSN,required"`
	MaxConns int32  `env:"POSTGRES_MAX_CONNS" envDefault:"10"`
}

// Kafka lists seed brokers; the client discovers the rest of the cluster.
type Kafka struct {
	Brokers []string `env:"KAFKA_BROKERS,required" envSeparator:","`
}

// Consumer configures the batch consumer (internal/platform/kafka).
type Consumer struct {
	Group  string   `env:"CONSUMER_GROUP,required"`
	Topics []string `env:"CONSUMER_TOPICS,required" envSeparator:","`
	// MaxPollRecords bounds one poll across all assigned partitions.
	MaxPollRecords int `env:"CONSUMER_MAX_POLL_RECORDS" envDefault:"500"`
	// Concurrency is how many partitions of one poll are processed in parallel.
	Concurrency int `env:"CONSUMER_CONCURRENCY" envDefault:"8"`
	// Retry applies to transient failures before a record is dead-lettered.
	Retry retry.Policy `envPrefix:"CONSUMER_RETRY_"`
}

// Relay configures the outbox relay (internal/platform/outbox).
type Relay struct {
	BatchSize    int           `env:"RELAY_BATCH_SIZE" envDefault:"500"`
	PollInterval time.Duration `env:"RELAY_POLL_INTERVAL" envDefault:"200ms"`
	LeaseTTL     time.Duration `env:"RELAY_LEASE_TTL" envDefault:"15s"`
	// Retention of published rows before the janitor deletes them.
	Retention time.Duration `env:"RELAY_RETENTION" envDefault:"24h"`
	Retry     retry.Policy  `envPrefix:"RELAY_RETRY_"`
}

// Topics sizes topics created by `sagactl topics`.
type Topics struct {
	Partitions        int32 `env:"TOPIC_PARTITIONS" envDefault:"12"`
	ReplicationFactor int16 `env:"TOPIC_REPLICATION_FACTOR" envDefault:"3"`
}
