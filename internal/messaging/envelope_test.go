package messaging

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestEnvelope_RoundTrip(t *testing.T) {
	saga := uuid.New()
	env, err := NewEnvelope(CmdReserveSeat, saga, ReserveSeat{SeatID: 7})
	if err != nil {
		t.Fatal(err)
	}

	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != env.ID || got.Type != CmdReserveSeat || got.SagaID != saga {
		t.Fatalf("round trip mismatch: %+v vs %+v", got, env)
	}

	var p ReserveSeat
	if err := got.DecodePayload(&p); err != nil || p.SeatID != 7 {
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
			if _, err := Unmarshal([]byte(raw)); !errors.Is(err, ErrInvalidEnvelope) {
				t.Fatalf("want ErrInvalidEnvelope, got %v", err)
			}
		})
	}
}

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
		if !names[topic] || !names[DLQ(topic)] {
			t.Errorf("missing %s or its DLQ", topic)
		}
	}
}
