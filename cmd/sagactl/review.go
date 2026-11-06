package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/platform/postgres"
	"github.com/Gandalff2k/saga3/internal/risk"
)

func review(ctx context.Context, args []string, w io.Writer) error {
	if len(args) == 0 {
		return errors.New("review: expected list, show, approve or reject")
	}
	dsn := os.Getenv("RISK_DSN")
	if dsn == "" {
		return errors.New("review: RISK_DSN is not set")
	}
	pool, err := postgres.NewPool(ctx, dsn, 2)
	if err != nil {
		return err
	}
	defer pool.Close()

	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("review "+sub, flag.ContinueOnError)
	status := fs.String("status", "PENDING", "list: PENDING, APPROVED, REJECTED or EXPIRED")
	limit := fs.Int("limit", 50, "list: max cases")
	asJSON := fs.Bool("json", false, "list/show: print JSON")
	by := fs.String("by", "", "approve/reject: reviewer name")
	note := fs.String("note", "", "approve/reject: why")

	var id uuid.UUID
	if sub != "list" {
		if len(rest) == 0 {
			return fmt.Errorf("review %s: saga id is required", sub)
		}
		if id, err = uuid.Parse(rest[0]); err != nil {
			return fmt.Errorf("review %s: %w", sub, err)
		}
		rest = rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}

	switch sub {
	case "list":
		cases, err := risk.ListCases(ctx, pool, *status, *limit)
		if err != nil {
			return err
		}
		if *asJSON {
			return writeJSONLines(w, cases)
		}
		return printCases(w, cases)
	case "show":
		c, err := risk.GetCase(ctx, pool, id)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(c)
	case "approve", "reject":
		if err := risk.DecideCase(ctx, pool, id, sub == "approve", *by, *note); err != nil {
			return err
		}
		_, err := fmt.Fprintf(w, "%s: %sd by %s\n", id, sub, *by)
		return err
	default:
		return fmt.Errorf("review: unknown command %q", sub)
	}
}

func writeJSONLines[T any](w io.Writer, items []T) error {
	enc := json.NewEncoder(w)
	for _, it := range items {
		if err := enc.Encode(it); err != nil {
			return err
		}
	}
	return nil
}

func printCases(w io.Writer, cases []risk.Case) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SAGA\tAGE\tAMOUNT\tCUSTOMER\tCOUNTRY\tSCORE\tRULES\tAGENT"); err != nil {
		return err
	}
	for _, c := range cases {
		rules := ""
		for i, h := range c.Hits {
			if i > 0 {
				rules += ","
			}
			rules += h.Rule
		}
		agent := "-"
		if len(c.Recommendation) > 0 {
			var rec struct {
				Action string `json:"action"`
			}
			if json.Unmarshal(c.Recommendation, &rec) == nil {
				agent = rec.Action
			}
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%.2f\t%s\t%s\t%d\t%s\t%s\n", c.SagaID,
			time.Since(c.CreatedAt).Round(time.Second), float64(c.AmountCents)/100, c.CustomerID, c.Country,
			c.Score, rules, agent); err != nil {
			return err
		}
	}
	return tw.Flush()
}
