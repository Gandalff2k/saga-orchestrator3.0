// sagactl: operational CLI. Runs as one-shot Jobs (compose / k8s) or by hand.
//
//	sagactl migrate            apply migrations to every DB whose <SERVICE>_DSN is set
//	sagactl topics             create Kafka topics + DLQs (KAFKA_BROKERS, TOPIC_*)
//	sagactl dlq list   -topic inventory.commands.dlq
//	sagactl dlq replay -topic inventory.commands.dlq [-limit N]
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx" for goose

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
	"github.com/Gandalff2k/saga3/internal/platform/todo"
	"github.com/Gandalff2k/saga3/migrations"
)

// Dependencies may still be starting when a Job runs: wait for them.
var waitPolicy = retry.Policy{MaxAttempts: 30, Initial: time.Second, Max: 5 * time.Second, Multiplier: 1.5, Jitter: 0.1}

func main() {
	os.Exit(run()) // separate run() so its defers execute before the exit
}

func run() int {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(os.Args) < 2 {
		usage()
		return 2
	}

	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "migrate":
		err = migrate(ctx, log)
	case "topics":
		err = topics(ctx, log)
	case "dlq":
		err = dlq(ctx, args, log)
	default:
		usage()
		return 2
	}
	if err != nil {
		log.Error(os.Args[1]+" failed", "err", err)
		return 1
	}
	return 0
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: sagactl migrate | topics | dlq {list|replay} -topic <name.dlq>")
}

func migrate(ctx context.Context, log *slog.Logger) error {
	ran := 0
	for _, svc := range migrations.Services {
		dsn := os.Getenv(strings.ToUpper(svc) + "_DSN")
		if dsn == "" {
			continue
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return err
		}
		err = retry.Do(ctx, waitPolicy, func(ctx context.Context, attempt int) error {
			if err := db.PingContext(ctx); err != nil {
				log.Info("waiting for postgres", "service", svc, "attempt", attempt, "err", err)
				return err
			}
			return nil
		})
		if err == nil {
			err = migrations.Up(ctx, db, svc, log)
		}
		_ = db.Close()
		if err != nil {
			return err
		}
		ran++
	}
	if ran == 0 {
		return errors.New("no <SERVICE>_DSN set (ORCHESTRATOR_DSN, INVENTORY_DSN, PAYMENT_DSN)")
	}
	return nil
}

func topics(ctx context.Context, log *slog.Logger) error {
	type cfg struct {
		config.Kafka
		config.Topics
	}
	c, err := config.Load[cfg]()
	if err != nil {
		return err
	}
	client, err := kafka.NewClient(c.Brokers, "sagactl")
	if err != nil {
		return err
	}
	defer client.Close()

	specs := messaging.Topics(c.Partitions, c.ReplicationFactor)
	return retry.Do(ctx, waitPolicy, func(ctx context.Context, attempt int) error {
		err := kafka.EnsureTopics(ctx, client, specs, log)
		if err != nil {
			log.Info("waiting for kafka", "attempt", attempt, "err", err)
		}
		return err
	})
}

// dlq inspects and replays dead-lettered records.
//
// TODO(phase-5):
//
//	list:   consume <topic>.dlq from the start without a group, print key, type,
//	        dlq-error, original offset, failed-at (table or -json)
//	replay: re-produce selected records to dlq-original-topic with the original
//	        key/value/headers (minus dlq-*) + "replayed-from" header; commit a
//	        dedicated group ("sagactl-replay") so a record is replayed once.
//	        The original message-id is kept: if it was in fact processed, the
//	        inbox drops the replay.
func dlq(_ context.Context, args []string, _ *slog.Logger) error {
	fs := flag.NewFlagSet("dlq", flag.ExitOnError)
	topic := fs.String("topic", "", "DLQ topic, e.g. "+kafka.DLQTopic(messaging.TopicInventoryCommands))
	limit := fs.Int("limit", 0, "max records (0 = all)")
	if len(args) == 0 {
		return errors.New("dlq: expected list or replay")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	_, _ = topic, limit
	return todo.Phase(5, "dlq "+args[0])
}
