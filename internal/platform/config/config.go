// Package config loads 12-factor configuration from environment variables.
// Each binary composes the blocks it needs into its own struct and calls Load.
package config

import (
	"errors"
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

// Inbox configures inbox cleanup (internal/platform/inbox).
type Inbox struct {
	// Retention must exceed the longest time a message can be redelivered:
	// topic retention (7d) plus any DLQ replay window. Deleting an id earlier
	// turns a late redelivery into a second execution.
	Retention       time.Duration `env:"INBOX_RETENTION" envDefault:"336h"` // 14d
	JanitorInterval time.Duration `env:"INBOX_JANITOR_INTERVAL" envDefault:"10m"`
}

// Relay configures the outbox relay (internal/platform/outbox).
type Relay struct {
	BatchSize    int           `env:"RELAY_BATCH_SIZE" envDefault:"500"`
	PollInterval time.Duration `env:"RELAY_POLL_INTERVAL" envDefault:"200ms"`
	// LeaseTTL: a crashed replica's shards are taken over after at most this long.
	// Leases are renewed every LeaseTTL/3.
	LeaseTTL time.Duration `env:"RELAY_LEASE_TTL" envDefault:"15s"`
	// PublishTimeout bounds one batch (produce + mark). Must be < LeaseTTL/3 so
	// a batch never outlives the lease it started under (Validate enforces it).
	PublishTimeout time.Duration `env:"RELAY_PUBLISH_TIMEOUT" envDefault:"4s"`
	// Concurrency: owned shards published in parallel by one replica.
	Concurrency int `env:"RELAY_CONCURRENCY" envDefault:"4"`
	// Retention of published rows before the janitor deletes them.
	Retention       time.Duration `env:"RELAY_RETENTION" envDefault:"24h"`
	JanitorInterval time.Duration `env:"RELAY_JANITOR_INTERVAL" envDefault:"1m"`
	// Retry is the per-shard backoff after a failed batch (MaxAttempts is
	// ignored: the relay never gives up, a row must not be skipped).
	Retry retry.Policy `envPrefix:"RELAY_RETRY_"`
}

// Validate rejects combinations that would break the relay's safety margins.
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

// Topics sizes topics created by `sagactl topics`.
type Topics struct {
	Partitions        int32 `env:"TOPIC_PARTITIONS" envDefault:"12"`
	ReplicationFactor int16 `env:"TOPIC_REPLICATION_FACTOR" envDefault:"3"`
}
