//go:build integration

// Package integration runs the system against real Postgres and Kafka started
// by testcontainers. All components run in-process, so a test can kill/restart
// a relay or a worker and inspect DB state directly.
//
//	go test -tags=integration -count=1 ./test/integration/...
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/migrations"
)

// env is shared by all tests in the package: containers start once in TestMain.
var env *Env

type Env struct {
	DSN     map[string]string // service -> DSN, migrated
	Brokers []string          // topics created (RF=1, 3 partitions)
	Log     *slog.Logger
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("IT_VERBOSE") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	pg, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithUsername("saga"), tcpostgres.WithPassword("saga"), tcpostgres.WithDatabase("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer testcontainers.TerminateContainer(pg)

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0", tckafka.WithClusterID("saga-it"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		return 1
	}
	defer testcontainers.TerminateContainer(kc)

	adminDSN, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	dsns, err := createAndMigrate(ctx, adminDSN, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := createTopics(ctx, brokers, log); err != nil {
		fmt.Fprintln(os.Stderr, "topics:", err)
		return 1
	}

	env = &Env{DSN: dsns, Brokers: brokers, Log: log}
	return m.Run()
}

// createAndMigrate creates one database per service (like deploy/compose/postgres-init.sql)
// and applies migrations with the same code `sagactl migrate` uses.
func createAndMigrate(ctx context.Context, adminDSN string, log *slog.Logger) (map[string]string, error) {
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return nil, err
	}
	defer admin.Close()

	out := map[string]string{}
	for _, svc := range migrations.Services {
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+svc); err != nil {
			return nil, err
		}
		u, err := url.Parse(adminDSN)
		if err != nil {
			return nil, err
		}
		u.Path = "/" + svc
		out[svc] = u.String()

		db, err := sql.Open("pgx", out[svc])
		if err != nil {
			return nil, err
		}
		err = migrations.Up(ctx, db, svc, log)
		_ = db.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func createTopics(ctx context.Context, brokers []string, log *slog.Logger) error {
	cl, err := kafka.NewClient(brokers, "it-admin")
	if err != nil {
		return err
	}
	defer cl.Close()
	return kafka.EnsureTopics(ctx, cl, messaging.Topics(3, 1), log)
}
