package kafka

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
