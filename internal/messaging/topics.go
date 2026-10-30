package messaging

import (
	"strconv"

	"github.com/Gandalff2k/saga3/internal/platform/kafka"
)

// Logical topic names; kafka.Namespace adds the environment prefix.
const (
	TopicInventoryCommands   = "inventory.commands"
	TopicPaymentCommands     = "payment.commands"
	TopicOrchestratorReplies = "orchestrator.replies"
)

// Topics returns the full topology: every main topic plus its DLQ.
func Topics(partitions int32, replicationFactor int16) []kafka.TopicSpec {
	minISR := max(1, replicationFactor-1) // RF=3 -> min.insync=2: survive one broker loss without losing acks=all writes

	main := map[string]*string{
		"min.insync.replicas": ptr(strconv.Itoa(int(minISR))),
		"retention.ms":        ptr("604800000"), // 7d
		"cleanup.policy":      ptr("delete"),
	}
	dlq := map[string]*string{
		"min.insync.replicas": ptr(strconv.Itoa(int(minISR))),
		"retention.ms":        ptr("2592000000"), // 30d
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
