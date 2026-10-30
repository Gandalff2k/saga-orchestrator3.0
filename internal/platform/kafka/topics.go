package kafka

// Namespace prefixes topic names so several environments (or test runs) can
// share one cluster. Business code only ever sees logical names
// ("inventory.commands"); the namespace is applied at the Kafka boundary:
// when the relay publishes, when a consumer subscribes, when topics are
// created. DLQ names derive from the physical topic, so they're namespaced too.
type Namespace string

// Topic returns the physical name of a logical topic.
func (n Namespace) Topic(name string) string { return string(n) + name }

// Topics maps Topic over names.
func (n Namespace) Topics(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = n.Topic(name)
	}
	return out
}

// Specs returns specs with namespaced names.
func (n Namespace) Specs(specs []TopicSpec) []TopicSpec {
	out := make([]TopicSpec, len(specs))
	for i, s := range specs {
		s.Name = n.Topic(s.Name)
		out[i] = s
	}
	return out
}

// DLQSuffix is appended to a topic name to get its dead-letter topic.
const DLQSuffix = ".dlq"

// DLQTopic returns the dead-letter topic for topic.
func DLQTopic(topic string) string { return topic + DLQSuffix }

// Headers added to records in a *.dlq topic (on top of the original headers).
const (
	HeaderDLQError          = "dlq-error"
	HeaderDLQOriginalTopic  = "dlq-original-topic"
	HeaderDLQOriginalPart   = "dlq-original-partition"
	HeaderDLQOriginalOffset = "dlq-original-offset"
	HeaderDLQConsumerGroup  = "dlq-consumer-group"
	HeaderDLQAttempts       = "dlq-attempts"
	HeaderDLQFailedAt       = "dlq-failed-at"
)

// TopicSpec is everything needed to create a topic (see EnsureTopics).
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]*string
}
