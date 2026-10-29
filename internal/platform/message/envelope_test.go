package message

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

type testPayload struct {
	N int `json:"n"`
}

func TestEnvelope_RoundTrip(t *testing.T) {
	saga := uuid.New()
	env, err := New("test.thing.v1", saga, testPayload{N: 7})
	if err != nil {
		t.Fatal(err)
	}
	if env.ID.Version() != 7 {
		t.Errorf("message id version = %d, want 7", env.ID.Version())
	}

	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != env.ID || got.Type != "test.thing.v1" || got.SagaID != saga {
		t.Fatalf("round trip mismatch: %+v vs %+v", got, env)
	}

	var p testPayload
	if err := got.DecodePayload(&p); err != nil || p.N != 7 {
		t.Fatalf("payload = %+v, err = %v", p, err)
	}
}

func TestUnmarshal_RejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"not json":     `{`,
		"missing id":   `{"type":"x","saga_id":"` + uuid.NewString() + `"}`,
		"missing type": `{"id":"` + uuid.NewString() + `","saga_id":"` + uuid.NewString() + `"}`,
		"missing saga": `{"id":"` + uuid.NewString() + `","type":"x"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Unmarshal([]byte(raw)); !errors.Is(err, ErrInvalid) {
				t.Fatalf("want ErrInvalid, got %v", err)
			}
		})
	}
}

func TestDecodePayload_InvalidIsErrInvalid(t *testing.T) {
	env := Envelope{Type: "x", Payload: json.RawMessage(`"not an object"`)}
	if err := env.DecodePayload(&testPayload{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}
