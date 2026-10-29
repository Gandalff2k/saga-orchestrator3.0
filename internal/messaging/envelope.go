package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Kafka record headers. Trace context travels in the W3C headers
// ("traceparent", "tracestate") injected by the OpenTelemetry propagator.
const (
	HeaderMessageID   = "message-id"
	HeaderMessageType = "message-type"

	// Set on records in a *.dlq topic.
	HeaderDLQError          = "dlq-error"
	HeaderDLQOriginalTopic  = "dlq-original-topic"
	HeaderDLQOriginalPart   = "dlq-original-partition"
	HeaderDLQOriginalOffset = "dlq-original-offset"
	HeaderDLQConsumerGroup  = "dlq-consumer-group"
	HeaderDLQAttempts       = "dlq-attempts"
	HeaderDLQFailedAt       = "dlq-failed-at"
)

// Envelope is the value of every Kafka record.
//
// ID is unique per message and is the idempotency key for the inbox: a
// redelivered message has the same ID and is skipped.
type Envelope struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"`
	SagaID     uuid.UUID       `json:"saga_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

// NewEnvelope wraps payload into an envelope with a fresh message id.
func NewEnvelope(msgType string, sagaID uuid.UUID, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", msgType, err)
	}
	return Envelope{
		ID:         uuid.New(),
		Type:       msgType,
		SagaID:     sagaID,
		OccurredAt: time.Now().UTC(),
		Payload:    raw,
	}, nil
}

var ErrInvalidEnvelope = errors.New("invalid envelope")

// Unmarshal parses and validates a record value. Errors wrap
// ErrInvalidEnvelope; callers should treat them as permanent (DLQ).
func Unmarshal(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	switch {
	case e.ID == uuid.Nil:
		return Envelope{}, fmt.Errorf("%w: missing id", ErrInvalidEnvelope)
	case e.Type == "":
		return Envelope{}, fmt.Errorf("%w: missing type", ErrInvalidEnvelope)
	case e.SagaID == uuid.Nil:
		return Envelope{}, fmt.Errorf("%w: missing saga_id", ErrInvalidEnvelope)
	}
	return e, nil
}

// DecodePayload unmarshals the payload into v.
func (e Envelope) DecodePayload(v any) error {
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("%w: %s payload: %v", ErrInvalidEnvelope, e.Type, err)
	}
	return nil
}
