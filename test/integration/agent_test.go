//go:build integration

package integration

import (
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Gandalff2k/saga3/internal/agent"
	"github.com/Gandalff2k/saga3/internal/llm"
	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/risk"
)

func scripted(rule string) llm.Client {
	call := func(id, name, args string) llm.Response {
		return llm.Response{Message: llm.Message{Role: llm.RoleAssistant,
			ToolCalls: []llm.ToolCall{{ID: id, Name: name, Arguments: args}}}}
	}
	return llm.Script(
		call("1", "customer_history", "{}"),
		call("2", "submit_recommendation",
			`{"action":"approve","confidence":0.8,"summary":"First order, only the amount is high.","evidence":["rule:`+rule+`"]}`),
	)
}

func newRunner(t *testing.T, s *system, c llm.Client, owner string) *agent.Runner {
	t.Helper()
	inv := agent.NewInvestigator(c, agent.DBEvidence{Pool: s.riskDB}, agent.Limits{MaxSteps: 4, MaxTokens: 10000, HistoryLimit: 10, InvalidAnswer: 1})
	r, err := agent.NewRunner(s.riskDB, inv, agent.RunnerConfig{PollInterval: time.Second, CaseTimeout: 10 * time.Second, MaxAttempts: 2},
		owner, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The agent only advises: its recommendation lands on the case, the order
// stays in review until a person decides.
func TestAgent_RecommendsButDoesNotDecide(t *testing.T) {
	s := startSystem(t, reviewOpts())
	o := s.placeOrder(700, 150_00, "")
	s.waitState(o.ID, orchestrator.StateInReview, 20*time.Second)

	worked, err := newRunner(t, s, scripted("over_limit"), "agent-1").Once(t.Context())
	if err != nil || !worked {
		t.Fatalf("worked=%v err=%v", worked, err)
	}
	c, err := risk.GetCase(t.Context(), s.riskDB, o.SagaID)
	if err != nil {
		t.Fatal(err)
	}
	var rec agent.Recommendation
	if err := json.Unmarshal(c.Recommendation, &rec); err != nil || rec.Action != "approve" || rec.Steps != 2 {
		t.Fatalf("recommendation = %s (%v)", c.Recommendation, err)
	}
	if c.Status != "PENDING" {
		t.Fatalf("case %s: the agent must not decide", c.Status)
	}
	time.Sleep(time.Second)
	if got, _ := s.svc.GetOrder(t.Context(), o.ID); got.SagaState != orchestrator.StateInReview {
		t.Fatalf("order moved to %s without a reviewer", got.SagaState)
	}
	if worked, _ := newRunner(t, s, scripted("over_limit"), "agent-1").Once(t.Context()); worked {
		t.Fatal("a case with a recommendation was picked up again")
	}
}

// Two agent replicas never work on the same case.
func TestAgent_ReplicasSplitTheQueue(t *testing.T) {
	s := startSystem(t, reviewOpts())
	o := s.placeOrder(710, 150_00, "")
	s.waitState(o.ID, orchestrator.StateInReview, 20*time.Second)

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		worked int
	)
	for i := range 4 {
		r := newRunner(t, s, scripted("over_limit"), "agent-"+string(rune('a'+i)))
		wg.Go(func() {
			ok, err := r.Once(t.Context())
			if err != nil {
				t.Error(err)
			}
			if ok {
				mu.Lock()
				worked++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if worked != 1 {
		t.Fatalf("%d replicas worked on the one case, want 1", worked)
	}
}

// A model that can't produce a valid answer leaves the case to the reviewer,
// and after MaxAttempts the agent stops retrying it.
func TestAgent_FailureLeavesTheCaseToPeople(t *testing.T) {
	s := startSystem(t, reviewOpts())
	o := s.placeOrder(720, 150_00, "")
	s.waitState(o.ID, orchestrator.StateInReview, 20*time.Second)

	for attempt := range 3 {
		worked, _ := newRunner(t, s, scripted("no_such_rule"), "agent-1").Once(t.Context())
		if want := attempt < 2; worked != want {
			t.Fatalf("attempt %d: worked=%v, want %v", attempt+1, worked, want)
		}
	}
	c, _ := risk.GetCase(t.Context(), s.riskDB, o.SagaID)
	if c.Status != "PENDING" || len(c.Recommendation) != 0 {
		t.Fatalf("case = %s %s", c.Status, c.Recommendation)
	}
	if err := risk.DecideCase(t.Context(), s.riskDB, o.SagaID, true, "alice", ""); err != nil {
		t.Fatalf("a person can still decide: %v", err)
	}
}
