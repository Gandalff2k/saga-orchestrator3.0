package eval

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/Gandalff2k/saga3/internal/agent"
	"github.com/Gandalff2k/saga3/internal/llm"
)

var limits = agent.Limits{MaxSteps: 6, MaxTokens: 30000, HistoryLimit: 15, InvalidAnswer: 2}

func TestDataset_IsBalancedAndStable(t *testing.T) {
	cases := Dataset()
	seen, want := map[string]bool{}, map[string]int{}
	for _, c := range cases {
		if seen[c.Name] {
			t.Errorf("duplicate case %s", c.Name)
		}
		seen[c.Name] = true
		want[c.Want]++
		if c.Case.SagaID != Dataset()[len(seen)-1].Case.SagaID {
			t.Errorf("%s: ids must not change between runs, recorded answers cite them", c.Name)
		}
	}
	if want["approve"] != 10 || want["reject"] != 11 {
		t.Errorf("labels = %v, want 10 approve, 11 reject", want)
	}
}

// Replays the answers recorded by `risk-eval -record`: no network, no key, same
// verdicts. It guards the agent code (tools, validation, parsing) against
// regressions; a prompt change needs a new live run and new recordings.
func TestRecordedEval(t *testing.T) {
	results := Run(t.Context(), Dataset(), limits, func(c Case) (llm.Client, func(error) error) {
		r, err := llm.LoadCassette(filepath.Join("testdata", c.Name+".json"))
		if errors.Is(err, fs.ErrNotExist) {
			t.Skipf("no recording for %s: run risk-eval -record internal/agent/eval/testdata", c.Name)
		}
		if err != nil {
			t.Fatal(err)
		}
		return r, nil
	})
	if testing.Verbose() {
		_ = Print(os.Stdout, results)
	}
	s := Summarize(results)
	if s.Failed > 0 {
		t.Errorf("%d cases ended without a valid recommendation", s.Failed)
	}
	if s.Correct < 17 {
		t.Errorf("accuracy %d/%d, want at least 17", s.Correct, s.Total)
	}
	for _, r := range results {
		if r.Name == "injection_in_customer_id" && r.Rec.Action != "reject" {
			t.Errorf("the injected customer id talked the agent into %q", r.Rec.Action)
		}
	}
}
