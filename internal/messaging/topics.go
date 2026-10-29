// Package messaging is the shared contract between services: topic names,
// message types, payloads and the envelope that travels on the wire.
// Changing anything here is a cross-service change — version message types
// (".v2") instead of mutating existing payloads.
package messaging

import "strconv"

// Topics. The record key is ALWAYS the saga id, so every message of one saga
// lands on the same partition and is processed in order.
const (
	TopicInventoryCommands   = "inventory.commands"
	TopicPaymentCommands     = "payment.commands"
	TopicOrchestratorReplies = "orchestrator.replies"
)

// DLQSuffix is appended to a topic name to get its dead-letter topic.
const DLQSuffix = ".dlq"

// DLQ returns the dead-letter topic for topic.
func DLQ(topic string) string { return topic + DLQSuffix }

// TopicSpec is everything needed to create a topic (see `sagactl topics`).
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]*string
}

// Topics returns the full topology: every main topic plus its DLQ.
//
// partitions caps consumer parallelism: at most `partitions` consumers of one
// group do useful work, the rest sit idle. Size HPA/KEDA maxReplicas from it.
func Topics(partitions int32, replicationFactor int16) []TopicSpec {
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

	var out []TopicSpec
	for _, name := range []string{TopicInventoryCommands, TopicPaymentCommands, TopicOrchestratorReplies} {
		out = append(out,
			TopicSpec{Name: name, Partitions: partitions, ReplicationFactor: replicationFactor, Configs: main},
			TopicSpec{Name: DLQ(name), Partitions: partitions, ReplicationFactor: replicationFactor, Configs: dlq},
		)
	}
	return out
}

func ptr(s string) *string { return &s }
