// sagactl: operational CLI. Runs as one-shot Jobs (compose / k8s) or by hand.
//
//	sagactl migrate            apply migrations to every DB whose <SERVICE>_DSN is set
//	sagactl topics             create Kafka topics + DLQs (KAFKA_BROKERS, TOPIC_*)
//	sagactl dlq list   -topic inventory.commands.dlq [-limit N] [-json]
//	sagactl dlq replay -topic inventory.commands.dlq [-limit N] [-dry-run]
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
	"github.com/Gandalff2k/saga3/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var waitPolicy = retry.Policy{MaxAttempts: 30, Initial: time.Second, Max: 5 * time.Second, Multiplier: 1.5, Jitter: 0.1}

func main() {
	os.Exit(run())
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

	specs := kafka.Namespace(c.TopicPrefix).Specs(messaging.Topics(c.Partitions, c.ReplicationFactor))
	return retry.Do(ctx, waitPolicy, func(ctx context.Context, attempt int) error {
		err := kafka.EnsureTopics(ctx, client, specs, log)
		if err != nil {
			log.Info("waiting for kafka", "attempt", attempt, "err", err)
		}
		return err
	})
}

func dlq(ctx context.Context, args []string, log *slog.Logger) error {
	if len(args) == 0 || (args[0] != "list" && args[0] != "replay") {
		return errors.New("dlq: expected list or replay")
	}
	fs := flag.NewFlagSet("dlq "+args[0], flag.ContinueOnError)
	topic := fs.String("topic", "", "DLQ topic, e.g. "+kafka.DLQTopic(messaging.TopicInventoryCommands))
	limit := fs.Int("limit", 0, "max records (0 = all)")
	asJSON := fs.Bool("json", false, "list: print JSON lines")
	dryRun := fs.Bool("dry-run", false, "replay: only count what would be replayed")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("dlq: -topic is required")
	}
	c, err := config.Load[config.Kafka]()
	if err != nil {
		return err
	}

	switch args[0] {
	case "list":
		entries, err := kafka.ListDLQ(ctx, c.Brokers, *topic, *limit)
		if err != nil {
			return err
		}
		return printDLQ(os.Stdout, entries, *asJSON)
	default:
		n, err := kafka.ReplayDLQ(ctx, c.Brokers, *topic, *limit, *dryRun)
		if err != nil {
			return err
		}
		log.Info("dlq replay", "topic", *topic, "records", n, "dry_run", *dryRun)
		return nil
	}
}

func printDLQ(w io.Writer, entries []kafka.DLQEntry, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		for _, e := range entries {
			if err := enc.Encode(e); err != nil {
				return err
			}
		}
		return nil
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PARTITION\tOFFSET\tREPLAYED\tTYPE\tKEY\tATTEMPTS\tFAILED AT\tERROR"); err != nil {
		return err
	}
	for _, e := range entries {
		errMsg := e.Error
		if len(errMsg) > 100 {
			errMsg = errMsg[:100] + "…"
		}
		if _, err := fmt.Fprintf(tw, "%d\t%d\t%v\t%s\t%s\t%s\t%s\t%s\n",
			e.Partition, e.Offset, e.Replayed, e.Type, e.Key, e.Attempts, e.FailedAt, errMsg); err != nil {
			return err
		}
	}
	return tw.Flush()
}
