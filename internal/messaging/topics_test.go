package messaging

import (
	"testing"

	"github.com/Gandalff2k/saga3/internal/platform/kafka"
)

func TestTopics_EveryTopicHasDLQ(t *testing.T) {
	specs := Topics(12, 3)
	names := map[string]bool{}
	for _, s := range specs {
		names[s.Name] = true
		if s.Partitions != 12 || s.ReplicationFactor != 3 {
			t.Errorf("%s: partitions=%d rf=%d", s.Name, s.Partitions, s.ReplicationFactor)
		}
		if got := *s.Configs["min.insync.replicas"]; got != "2" {
			t.Errorf("%s: min.insync.replicas=%s, want 2", s.Name, got)
		}
	}
	for _, topic := range []string{TopicInventoryCommands, TopicPaymentCommands, TopicOrchestratorReplies} {
		if !names[topic] || !names[kafka.DLQTopic(topic)] {
			t.Errorf("missing %s or its DLQ", topic)
		}
	}
}

func TestTopics_SingleBrokerKeepsMinISRAtOne(t *testing.T) {
	for _, s := range Topics(3, 1) {
		if got := *s.Configs["min.insync.replicas"]; got != "1" {
			t.Fatalf("%s: min.insync.replicas=%s with RF=1 would reject every write", s.Name, got)
		}
	}
}
