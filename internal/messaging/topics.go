// Package messaging is the business contract between services: topic names,
// message types and payloads. The wire format itself (Envelope, headers) is
// generic and lives in internal/platform/message.
// Changing anything here is a cross-service change — version message types
// (".v2") instead of mutating existing payloads.
package messaging

import (
	"strconv"

	"github.com/Gandalff2k/saga3/internal/platform/kafka"
)

// Topics. The record key is ALWAYS the saga id, so every message of one saga
// lands on the same partition and is processed in order.
const (
	TopicInventoryCommands   = "inventory.commands"
	TopicPaymentCommands     = "payment.commands"
	TopicOrchestratorReplies = "orchestrator.replies"
)

// Topics returns the full topology: every main topic plus its DLQ.
//
// partitions caps consumer parallelism: at most `partitions` consumers of one
// group do useful work, the rest sit idle. Size HPA/KEDA maxReplicas from it.
func Topics(partitions int32, replicationFactor int16) []kafka.TopicSpec {
	minISR := max(1, replicationFactor-1) // RF=3 -> min.insync=2: survive one broker loss without losing acks=all writes

	main := map[string]*string{
		"min.insync.replicas": ptr(strconv.Itoa(int(minISR))),
		"retention.ms":        ptr("604800000"), // 7d
		"cleanup.policy":      ptr("delete"),
	}
	dlq := map[string]*string{
		"min.insync.replicas": ptr(strconv.Itoa(int(minISR))),
		"retention.ms":        ptr("2592000000"), // 30d: DLQ is evidence, keep it longer
		"cleanup.policy":      ptr("delete"),
	}

	var out []kafka.TopicSpec
	for _, name := range []string{TopicInventoryCommands, TopicPaymentCommands, TopicOrchestratorReplies} {
		out = append(out,
			kafka.TopicSpec{Name: name, Partitions: partitions, ReplicationFactor: replicationFactor, Configs: main},
			kafka.TopicSpec{Name: kafka.DLQTopic(name), Partitions: partitions, ReplicationFactor: replicationFactor, Configs: dlq},
		)
	}
	return out
}

func ptr(s string) *string { return &s }
