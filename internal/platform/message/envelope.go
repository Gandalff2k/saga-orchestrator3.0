// Package message defines the wire format shared by the outbox, the relay and
// consumers: the Envelope (Kafka record value) and the standard headers.
// It knows nothing about concrete message types; those live in
// internal/messaging (the business contract).
package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Kafka record headers set on every message. Trace context travels in the W3C
// headers ("traceparent", "tracestate") injected by the OpenTelemetry propagator.
const (
	HeaderID   = "message-id"
	HeaderType = "message-type"
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

// New wraps payload into an envelope with a fresh message id.
//
// Ids are UUIDv7 (time-ordered): the inbox and outbox use them in btree
// indexes, and v7 appends at the right edge instead of splitting random pages.
func New(msgType string, sagaID uuid.UUID, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", msgType, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, fmt.Errorf("message id: %w", err)
	}
	return Envelope{
		ID:         id,
		Type:       msgType,
		SagaID:     sagaID,
		OccurredAt: time.Now().UTC(),
		Payload:    raw,
	}, nil
}

// ErrInvalid marks a record that can never be processed (bad JSON, missing
// fields): consumers wrap it in retry.Permanent and dead-letter it.
var ErrInvalid = errors.New("invalid envelope")

// Unmarshal parses and validates a record value. Errors wrap ErrInvalid.
func Unmarshal(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	switch {
	case e.ID == uuid.Nil:
		return Envelope{}, fmt.Errorf("%w: missing id", ErrInvalid)
	case e.Type == "":
		return Envelope{}, fmt.Errorf("%w: missing type", ErrInvalid)
	case e.SagaID == uuid.Nil:
		return Envelope{}, fmt.Errorf("%w: missing saga_id", ErrInvalid)
	}
	return e, nil
}

// DecodePayload unmarshals the payload into v. Errors wrap ErrInvalid.
func (e Envelope) DecodePayload(v any) error {
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("%w: %s payload: %w", ErrInvalid, e.Type, err)
	}
	return nil
}
