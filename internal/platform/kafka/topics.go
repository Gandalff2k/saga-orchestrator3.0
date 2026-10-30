package kafka


type Namespace string

func (n Namespace) Topic(name string) string { return string(n) + name }

func (n Namespace) Topics(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = n.Topic(name)
	}
	return out
}

func (n Namespace) Specs(specs []TopicSpec) []TopicSpec {
	out := make([]TopicSpec, len(specs))
	for i, s := range specs {
		s.Name = n.Topic(s.Name)
		out[i] = s
	}
	return out
}

const DLQSuffix = ".dlq"

func DLQTopic(topic string) string { return topic + DLQSuffix }

const (
	HeaderDLQError          = "dlq-error"
	HeaderDLQOriginalTopic  = "dlq-original-topic"
	HeaderDLQOriginalPart   = "dlq-original-partition"
	HeaderDLQOriginalOffset = "dlq-original-offset"
	HeaderDLQConsumerGroup  = "dlq-consumer-group"
	HeaderDLQAttempts       = "dlq-attempts"
	HeaderDLQFailedAt       = "dlq-failed-at"
)

type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]*string
}
