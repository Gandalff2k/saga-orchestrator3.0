//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
	"github.com/Gandalff2k/saga3/migrations"
)

// env is shared by all tests in the package: containers start once in TestMain.
var env *Env

type Env struct {
	DSN      map[string]string // service -> DSN, migrated
	Brokers  []string          // topics created (RF=1, 3 partitions)
	Log      *slog.Logger
	adminDSN string
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

	// Same as production minus exporting: installs the W3C propagator, so trace
	// context flows into outbox headers exactly like in a real process.
	if _, err := telemetry.Init(ctx, telemetry.Config{}); err != nil {
		fmt.Fprintln(os.Stderr, "telemetry:", err)
		return 1
	}

	pg, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithUsername("saga"), tcpostgres.WithPassword("saga"), tcpostgres.WithDatabase("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer terminate("postgres", pg)

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0", tckafka.WithClusterID("saga-it"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		return 1
	}
	defer terminate("kafka", kc)

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

	env = &Env{DSN: dsns, Brokers: brokers, Log: log, adminDSN: adminDSN}
	return m.Run()
}

// Pool opens a pgx pool to a service DB, closed when the test ends.
func (e *Env) Pool(t *testing.T, service string) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(t.Context(), e.DSN[service], 10)
	if err != nil {
		t.Fatalf("pool %s: %v", service, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// FreshDB creates an isolated, migrated database for one test (its own
// outbox, leases and relay members) and returns a pool to it. The database is
// dropped when the test ends.
func (e *Env) FreshDB(t *testing.T, service string) *pgxpool.Pool {
	t.Helper()
	name := "it_" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	dsn, err := createDB(t.Context(), e.adminDSN, name, service, e.Log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin, err := sql.Open("pgx", e.adminDSN)
		if err != nil {
			return
		}
		defer func() { _ = admin.Close() }()
		_, _ = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	pool, err := postgres.NewPool(t.Context(), dsn, 20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close) // LIFO: runs before the DROP above
	return pool
}

// FreshTopic creates a uniquely named topic (and its DLQ) for one test.
func (e *Env) FreshTopic(t *testing.T, partitions int32) string {
	t.Helper()
	name := "it-" + uuid.NewString()
	cl, err := kafka.NewClient(e.Brokers, "it-admin")
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	specs := []kafka.TopicSpec{
		{Name: name, Partitions: partitions, ReplicationFactor: 1},
		{Name: kafka.DLQTopic(name), Partitions: partitions, ReplicationFactor: 1},
	}
	if err := kafka.EnsureTopics(t.Context(), cl, specs, e.Log); err != nil {
		t.Fatal(err)
	}
	return name
}

// Admin runs a statement as the superuser on the maintenance database
// (e.g. to cut a test database off and simulate an outage).
func (e *Env) Admin(t *testing.T, stmt string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", e.adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}

// terminate reports (not fails on) cleanup errors: Ryuk reaps leftovers anyway,
// but a container that won't stop is worth seeing in the output.
func terminate(name string, c testcontainers.Container) {
	if err := testcontainers.TerminateContainer(c); err != nil {
		fmt.Fprintf(os.Stderr, "terminate %s: %v\n", name, err)
	}
}

// createAndMigrate creates one database per service (like deploy/compose/postgres-init.sql)
// and applies migrations with the same code `sagactl migrate` uses.
func createAndMigrate(ctx context.Context, adminDSN string, log *slog.Logger) (map[string]string, error) {
	out := map[string]string{}
	for _, svc := range migrations.Services {
		dsn, err := createDB(ctx, adminDSN, svc, svc, log)
		if err != nil {
			return nil, err
		}
		out[svc] = dsn
	}
	return out, nil
}

// createDB creates database name, applies the platform + service migrations
// and returns its DSN.
func createDB(ctx context.Context, adminDSN, name, service string, log *slog.Logger) (string, error) {
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return "", err
	}
	defer func() { _ = admin.Close() }() // statements already ran; a close error changes nothing
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		return "", fmt.Errorf("create database %s: %w", name, err)
	}

	u, err := url.Parse(adminDSN)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	dsn := u.String()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()
	if err := migrations.Up(ctx, db, service, log); err != nil {
		return "", err
	}
	return dsn, nil
}

func createTopics(ctx context.Context, brokers []string, log *slog.Logger) error {
	cl, err := kafka.NewClient(brokers, "it-admin")
	if err != nil {
		return err
	}
	defer cl.Close()
	return kafka.EnsureTopics(ctx, cl, messaging.Topics(3, 1), log)
}
