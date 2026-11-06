package fraudgen

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"
)

// Outcome is what the risk service did with one labelled order.
type Outcome struct {
	Scenario string
	Fraud    bool
	Decision string
}

// Row is one scenario's line in the report.
type Row struct {
	Scenario             string
	Total                int
	Allow, Review, Block int
	Unscored             int
}

// Report is precision and recall of "stopped" (review or block) against the labels.
type Report struct {
	Rows              []Row
	Precision         float64
	Recall            float64
	LegitReviewRate   float64
	LegitBlockRate    float64
	FraudPassed       int
	Unscored          int
	Fraud, Legit      int
	TruePos, FalsePos int
}

// Score builds the report.
func Score(outcomes []Outcome) Report {
	rows := map[string]*Row{}
	var r Report
	legitReview, legitBlock := 0, 0
	for _, o := range outcomes {
		row := rows[o.Scenario]
		if row == nil {
			row = &Row{Scenario: o.Scenario}
			rows[o.Scenario] = row
		}
		row.Total++
		stopped := false
		switch o.Decision {
		case "ALLOW":
			row.Allow++
		case "REVIEW":
			row.Review++
			stopped = true
		case "BLOCK":
			row.Block++
			stopped = true
		default:
			row.Unscored++
			r.Unscored++
			continue
		}
		if o.Fraud {
			r.Fraud++
		} else {
			r.Legit++
		}
		switch {
		case stopped && o.Fraud:
			r.TruePos++
		case stopped:
			r.FalsePos++
			if o.Decision == "BLOCK" {
				legitBlock++
			} else {
				legitReview++
			}
		case o.Fraud:
			r.FraudPassed++
		}
	}
	for _, row := range rows {
		r.Rows = append(r.Rows, *row)
	}
	slices.SortFunc(r.Rows, func(a, b Row) int { return cmp.Compare(a.Scenario, b.Scenario) })
	r.Precision = ratio(r.TruePos, r.TruePos+r.FalsePos)
	r.Recall = ratio(r.TruePos, r.Fraud)
	r.LegitReviewRate = ratio(legitReview, r.Legit)
	r.LegitBlockRate = ratio(legitBlock, r.Legit)
	return r
}

func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// Print writes the report as a table.
func (r Report) Print(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	if _, err := fmt.Fprintln(tw, "SCENARIO\tORDERS\tALLOW\tREVIEW\tBLOCK\tUNSCORED\t"); err != nil {
		return err
	}
	for _, row := range r.Rows {
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t\n", row.Scenario, row.Total, row.Allow, row.Review,
			row.Block, row.Unscored); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "\nstopped = review or block\nprecision %.1f%%  recall %.1f%%  fraud passed %d of %d\n"+
		"legit orders sent to review %.1f%%, blocked %.1f%%  unscored %d\n",
		100*r.Precision, 100*r.Recall, r.FraudPassed, r.Fraud, 100*r.LegitReviewRate, 100*r.LegitBlockRate, r.Unscored)
	return err
}
