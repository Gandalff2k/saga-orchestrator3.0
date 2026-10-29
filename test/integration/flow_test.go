//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
)

// TestInfra checks the harness itself: schemas migrated, topics created.
func TestInfra(t *testing.T) {
	ctx := context.Background()

	for svc, dsn := range env.DSN {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		var shards int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM outbox_shard_lease").Scan(&shards); err != nil {
			t.Fatalf("%s: %v", svc, err)
		}
		_ = db.Close()
		if shards != 16 {
			t.Errorf("%s: %d shard leases, want 16", svc, shards)
		}
	}

	cl, err := kafka.NewClient(env.Brokers, "it-check")
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	topics, err := kadm.NewClient(cl).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range messaging.Topics(3, 1) {
		if !topics.Has(spec.Name) {
			t.Errorf("topic %s missing", spec.Name)
		}
	}
}

// Full-flow scenarios. Each starts the whole system in-process (API handler via
// httptest, 3 relays, 3 workers as goroutines) on fresh data and asserts on
// the final DB state + Kafka topics. Unskip as the roadmap phases land.

func TestFlow_HappyPath(t *testing.T) {
	t.Skip("TODO(phase-4): POST /orders -> order CONFIRMED, seat RESERVED, payment CHARGED, one trace")
}

func TestFlow_SeatTaken_Aborts(t *testing.T) {
	t.Skip("TODO(phase-4): two orders for one seat -> exactly one CONFIRMED, the other ABORTED, no payment for it")
}

func TestFlow_PaymentDeclined_Compensates(t *testing.T) {
	t.Skip("TODO(phase-4): amount > PAYMENT_LIMIT_CENTS -> COMPENSATING -> ABORTED, seat FREE again")
}

func TestFlow_DuplicateDelivery_IsIdempotent(t *testing.T) {
	t.Skip("TODO(phase-4): re-produce every outbox message twice -> same final state, inbox has one row per id")
}

func TestFlow_TransientFailures_RetriedNotDeadLettered(t *testing.T) {
	t.Skip("TODO(phase-4): FAULT_TRANSIENT_RATE=0.5 -> all orders still CONFIRMED, DLQ empty, retries metric > 0")
}

func TestFlow_PoisonMessage_GoesToDLQ_BatchContinues(t *testing.T) {
	t.Skip("TODO(phase-4): garbage record amid valid ones -> garbage in inventory.commands.dlq with dlq-* headers, valid ones processed")
}

func TestFlow_RelayCrash_NoLossNoReorder(t *testing.T) {
	t.Skip("TODO(phase-4): kill a relay mid-batch, another takes its shards after LeaseTTL, every saga completes")
}

func TestFlow_ScaleOut_RebalancesWithoutLoss(t *testing.T) {
	t.Skip("TODO(phase-4): start 1 inventory worker, load 200 orders, add 2 workers mid-run -> all complete exactly once")
}
