//go:build integration

package integration

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/message"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// The operator workflow: a bug dead-letters some messages, the operator
// inspects the DLQ, ships a fix, replays, and replaying again is a no-op.
func TestDLQTool_ListFixReplay(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	setupEffects(t, pool)
	topic := env.FreshTopic(t, 3)
	dlqTopic := kafka.DLQTopic(topic)
	group := "g-" + uuid.NewString()

	var broken atomic.Bool
	broken.Store(true)
	bug := func(e message.Envelope) error { // step 99 hits a "bug" until it's fixed
		var p step
		if err := e.DecodePayload(&p); err == nil && p.Step == 99 && broken.Load() {
			return retry.Permanent(errStr("cannot handle step 99 yet"))
		}
		return nil
	}

	good := produce(t, topic, 4, 1)
	bad := produceSteps(t, topic, 99, 2)
	startConsumer(t, pool, consumerCfg(group, topic), effectsHandler(bug))
	eventually(t, 20*time.Second, "good applied, bad dead-lettered", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) == len(good) && len(dlqRecords(t, topic, 2)) == 2
	})

	// list
	entries, err := kafka.ListDLQ(t.Context(), env.Brokers, dlqTopic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("list = %d entries, want 2", len(entries))
	}
	for _, e := range entries {
		if e.Replayed || e.OriginalTopic != topic || e.Group != group || !strings.Contains(e.Error, "step 99") || e.Attempts != "1" {
			t.Errorf("entry = %+v", e)
		}
	}

	// dry run changes nothing
	if n, err := kafka.ReplayDLQ(t.Context(), env.Brokers, dlqTopic, 0, true); err != nil || n != 2 {
		t.Fatalf("dry run = %d, %v; want 2", n, err)
	}
	time.Sleep(time.Second)
	if n := count(t, pool, `SELECT count(*) FROM it_effects`); n != len(good) {
		t.Fatalf("dry run applied something: %d effects", n)
	}

	// fix, replay, everything applied
	broken.Store(false)
	if n, err := kafka.ReplayDLQ(t.Context(), env.Brokers, dlqTopic, 0, false); err != nil || n != 2 {
		t.Fatalf("replay = %d, %v; want 2", n, err)
	}
	eventually(t, 20*time.Second, "replayed records applied", func() bool {
		return count(t, pool, `SELECT count(*) FROM it_effects`) == len(good)+len(bad)
	})
	for _, b := range bad {
		if n := count(t, pool, `SELECT count(*) FROM it_effects WHERE message_id = $1`, b.env.ID); n != 1 {
			t.Errorf("message %s applied %d times (original id must be kept)", b.env.ID, n)
		}
	}

	// replay again: nothing left to send
	if n, err := kafka.ReplayDLQ(t.Context(), env.Brokers, dlqTopic, 0, false); err != nil || n != 0 {
		t.Fatalf("second replay = %d, %v; want 0", n, err)
	}
	entries, err = kafka.ListDLQ(t.Context(), env.Brokers, dlqTopic, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Replayed {
			t.Errorf("entry %d@%d not marked replayed", e.Partition, e.Offset)
		}
	}
}

func TestDLQTool_RejectsNonDLQTopic(t *testing.T) {
	if _, err := kafka.ReplayDLQ(t.Context(), env.Brokers, "inventory.commands", 0, true); err == nil {
		t.Fatal("replaying from a non-DLQ topic must be refused")
	}
}

// produceSteps sends n messages with the given step, each for a new saga.
func produceSteps(t *testing.T, topic string, stepNo, n int) []produced {
	t.Helper()
	var out []produced
	for range n {
		e, err := message.New(typeStep, uuid.New(), step{Step: stepNo})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, produced{env: e, value: raw})
	}
	send(t, topic, out...)
	return out
}

type errStr string

func (e errStr) Error() string { return string(e) }
