package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)


const (
	HeaderID   = "message-id"
	HeaderType = "message-type"
)


type Envelope struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"`
	SagaID     uuid.UUID       `json:"saga_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

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


var ErrInvalid = errors.New("invalid envelope")


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

func (e Envelope) DecodePayload(v any) error {
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("%w: %s payload: %w", ErrInvalid, e.Type, err)
	}
	return nil
}
