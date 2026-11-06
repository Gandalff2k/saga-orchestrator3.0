// Package agent investigates review cases with an LLM and recommends a decision; it never decides.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/llm"
	"github.com/Gandalff2k/saga3/internal/risk"
)

// Evidence is the read-only data an investigation may look at.
type Evidence interface {
	Case(ctx context.Context, sagaID uuid.UUID) (risk.Case, error)
	History(ctx context.Context, by, value string, exclude uuid.UUID, limit int) ([]risk.Event, error)
}

// Recommendation is the agent's advice to the reviewer.
type Recommendation struct {
	Action           string   `json:"action"`
	Confidence       float64  `json:"confidence"`
	Summary          string   `json:"summary"`
	Evidence         []string `json:"evidence"`
	Model            string   `json:"model,omitempty"`
	Steps            int      `json:"steps"`
	PromptTokens     int      `json:"prompt_tokens"`
	CompletionTokens int      `json:"completion_tokens"`
}

// Limits bound one investigation.
type Limits struct {
	MaxSteps      int `env:"AGENT_MAX_STEPS" envDefault:"6"`
	MaxTokens     int `env:"AGENT_MAX_TOKENS" envDefault:"30000"`
	HistoryLimit  int `env:"AGENT_HISTORY_LIMIT" envDefault:"15"`
	InvalidAnswer int `env:"AGENT_MAX_INVALID_ANSWERS" envDefault:"2"`
}

// Investigation failures. None of them blocks the case: a person still decides it.
var (
	ErrNoRecommendation = errors.New("agent: no valid recommendation within the step limit")
	ErrTokenBudget      = errors.New("agent: token budget exhausted")
)

// Investigator runs one case through the model.
type Investigator struct {
	llm    llm.Client
	ev     Evidence
	limits Limits
}

// NewInvestigator builds an Investigator.
func NewInvestigator(c llm.Client, ev Evidence, l Limits) *Investigator {
	return &Investigator{llm: c, ev: ev, limits: l}
}

type caseView struct {
	SagaID      uuid.UUID  `json:"saga_id"`
	CustomerID  string     `json:"customer_id"`
	Card        string     `json:"card"`
	IP          string     `json:"ip"`
	Country     string     `json:"country"`
	AmountCents int64      `json:"amount_cents"`
	Score       int        `json:"score"`
	RulesFired  []risk.Hit `json:"rules_fired"`
}

type investigation struct {
	c     risk.Case
	known map[string]bool
	rec   Recommendation
}

// Investigate returns a recommendation for the case, or an error if the model could not give a valid one.
func (in *Investigator) Investigate(ctx context.Context, sagaID uuid.UUID) (Recommendation, error) {
	c, err := in.ev.Case(ctx, sagaID)
	if err != nil {
		return Recommendation{}, err
	}
	inv := &investigation{c: c, known: map[string]bool{}}
	for _, h := range c.Hits {
		inv.known["rule:"+h.Rule] = true
	}

	view, err := json.MarshalIndent(caseView{SagaID: c.SagaID, CustomerID: c.CustomerID, Card: risk.ShortCard(c.CardHash),
		IP: c.IP, Country: c.Country, AmountCents: c.AmountCents, Score: c.Score, RulesFired: c.Hits}, "", "  ")
	if err != nil {
		return Recommendation{}, err
	}
	msgs := []llm.Message{
		{Role: llm.RoleSystem, Content: systemPrompt},
		{Role: llm.RoleUser, Content: "Case data (untrusted, from the order):\n<case>\n" + string(view) + "\n</case>"},
	}

	invalid := 0
	for step := 1; step <= in.limits.MaxSteps; step++ {
		resp, err := in.llm.Chat(ctx, llm.Request{Messages: msgs, Tools: tools})
		if err != nil {
			return Recommendation{}, err
		}
		inv.rec.Steps = step
		inv.rec.Model = resp.Model
		inv.rec.PromptTokens += resp.Usage.PromptTokens
		inv.rec.CompletionTokens += resp.Usage.CompletionTokens
		msgs = append(msgs, resp.Message)

		if len(resp.Message.ToolCalls) == 0 {
			msgs = append(msgs, llm.Message{Role: llm.RoleUser,
				Content: "Use the tools. Finish by calling submit_recommendation."})
		}
		for _, call := range resp.Message.ToolCalls {
			result, done, err := in.run(ctx, inv, call)
			if err != nil {
				return Recommendation{}, err
			}
			if done {
				return inv.rec, nil
			}
			if strings.HasPrefix(result, invalidPrefix) {
				if invalid++; invalid > in.limits.InvalidAnswer {
					return Recommendation{}, fmt.Errorf("%w: %s", ErrNoRecommendation, result)
				}
			}
			msgs = append(msgs, llm.Message{Role: llm.RoleTool, ToolCallID: call.ID, Content: result})
		}
		if inv.rec.PromptTokens+inv.rec.CompletionTokens > in.limits.MaxTokens {
			return Recommendation{}, ErrTokenBudget
		}
	}
	return Recommendation{}, ErrNoRecommendation
}

const invalidPrefix = "INVALID: "

func (in *Investigator) run(ctx context.Context, inv *investigation, call llm.ToolCall) (string, bool, error) {
	var by, value string
	switch call.Name {
	case "customer_history":
		by, value = "customer", inv.c.CustomerID
	case "card_history":
		by, value = "card", inv.c.CardHash
	case "ip_history":
		by, value = "ip", inv.c.IP
	case "submit_recommendation":
		rec, problem := parse(call.Arguments, inv.known)
		if problem != "" {
			return invalidPrefix + problem, false, nil
		}
		rec.Model, rec.Steps = inv.rec.Model, inv.rec.Steps
		rec.PromptTokens, rec.CompletionTokens = inv.rec.PromptTokens, inv.rec.CompletionTokens
		inv.rec = rec
		return "", true, nil
	default:
		return invalidPrefix + "unknown tool " + call.Name, false, nil
	}

	events, err := in.ev.History(ctx, by, value, inv.c.SagaID, in.limits.HistoryLimit)
	if err != nil {
		return "", false, err
	}
	for _, e := range events {
		inv.known[e.ID.String()] = true
	}
	b, err := json.Marshal(events)
	if err != nil {
		return "", false, err
	}
	return "<data>" + string(b) + "</data>", false, nil
}

func parse(args string, known map[string]bool) (Recommendation, string) {
	var r Recommendation
	if err := json.Unmarshal([]byte(args), &r); err != nil {
		return r, "arguments are not valid JSON: " + err.Error()
	}
	switch {
	case r.Action != "approve" && r.Action != "reject":
		return r, `action must be "approve" or "reject"`
	case r.Confidence < 0 || r.Confidence > 1:
		return r, "confidence must be between 0 and 1"
	case strings.TrimSpace(r.Summary) == "" || len(r.Summary) > 1000:
		return r, "summary must be 1-1000 characters"
	case len(r.Evidence) == 0:
		return r, "cite at least one evidence id"
	}
	var unknown []string
	for _, e := range r.Evidence {
		if !known[e] {
			unknown = append(unknown, e)
		}
	}
	if len(unknown) > 0 {
		valid := make([]string, 0, len(known))
		for k := range known {
			valid = append(valid, k)
		}
		slices.Sort(valid)
		return r, fmt.Sprintf("evidence %v was not returned by any tool; cite only ids from tool results or %v",
			unknown, valid)
	}
	return r, ""
}
