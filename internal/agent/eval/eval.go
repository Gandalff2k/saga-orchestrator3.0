package eval

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/Gandalff2k/saga3/internal/agent"
	"github.com/Gandalff2k/saga3/internal/llm"
)

// Result is the agent's answer on one case.
type Result struct {
	Rec  agent.Recommendation
	Name string
	Want string
	Err  error
}

// Correct reports whether the agent gave the labelled answer.
func (r Result) Correct() bool { return r.Err == nil && r.Rec.Action == r.Want }

// Summary aggregates results.
type Summary struct {
	Total, Correct, Failed     int
	FalseApprove, FalseReject  int
	PromptTokens, OutputTokens int
	Steps                      int
}

// Run investigates every case with the client that clientFor returns for it.
func Run(ctx context.Context, cases []Case, limits agent.Limits, clientFor func(Case) (llm.Client, func(error) error)) []Result {
	var out []Result
	for _, c := range cases {
		client, done := clientFor(c)
		rec, err := agent.NewInvestigator(client, MemEvidence{C: c}, limits).Investigate(ctx, c.Case.SagaID)
		if done != nil {
			err = done(err)
		}
		out = append(out, Result{Rec: rec, Name: c.Name, Want: c.Want, Err: err})
	}
	return out
}

// Summarize counts the results.
func Summarize(rs []Result) Summary {
	var s Summary
	for _, r := range rs {
		s.Total++
		s.PromptTokens += r.Rec.PromptTokens
		s.OutputTokens += r.Rec.CompletionTokens
		s.Steps += r.Rec.Steps
		switch {
		case r.Err != nil:
			s.Failed++
		case r.Correct():
			s.Correct++
		case r.Want == "reject":
			s.FalseApprove++
		default:
			s.FalseReject++
		}
	}
	return s
}

// Print writes a per-case table and the summary.
func Print(w io.Writer, rs []Result) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CASE\tWANT\tGOT\tCONF\tSTEPS\tTOKENS\tOK\tERROR"); err != nil {
		return err
	}
	for _, r := range rs {
		errMsg := ""
		if r.Err != nil {
			errMsg = r.Err.Error()
			if len(errMsg) > 80 {
				errMsg = errMsg[:80] + "…"
			}
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%.2f\t%d\t%d\t%v\t%s\n", r.Name, r.Want, r.Rec.Action,
			r.Rec.Confidence, r.Rec.Steps, r.Rec.PromptTokens+r.Rec.CompletionTokens, r.Correct(), errMsg); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	s := Summarize(rs)
	_, err := fmt.Fprintf(w, "\naccuracy %d/%d  fraud approved %d  legit rejected %d  failed %d\n"+
		"tokens %d prompt + %d output (avg %d per case), avg steps %.1f\n",
		s.Correct, s.Total, s.FalseApprove, s.FalseReject, s.Failed, s.PromptTokens, s.OutputTokens,
		(s.PromptTokens+s.OutputTokens)/max(1, s.Total), float64(s.Steps)/float64(max(1, s.Total)))
	return err
}
