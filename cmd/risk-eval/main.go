// risk-eval runs the review agent on the labelled dataset.
//
//	risk-eval                         live, against LLM_BASE_URL / LLM_MODEL / LLM_API_KEY
//	risk-eval -record internal/agent/eval/testdata   live, and save each case's answers as a cassette
//	risk-eval -only account_takeover  one case
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Gandalff2k/saga3/internal/agent"
	"github.com/Gandalff2k/saga3/internal/agent/eval"
	"github.com/Gandalff2k/saga3/internal/llm"
	"github.com/Gandalff2k/saga3/internal/platform/config"
)

type cfg struct {
	LLM    llm.Config
	Limits agent.Limits
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "risk-eval:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	record := flag.String("record", "", "directory to save cassettes into")
	only := flag.String("only", "", "run one case by name")
	flag.Parse()

	c, err := config.Load[cfg]()
	if err != nil {
		return err
	}
	if c.LLM.APIKey == "" {
		fmt.Fprintln(os.Stderr, "warning: LLM_API_KEY is empty")
	}
	client, err := llm.NewOpenAI(c.LLM)
	if err != nil {
		return err
	}

	var cases []eval.Case
	for _, cs := range eval.Dataset() {
		if *only == "" || cs.Name == *only {
			cases = append(cases, cs)
		}
	}
	if len(cases) == 0 {
		return fmt.Errorf("no case named %q", *only)
	}

	results := eval.Run(ctx, cases, c.Limits, func(cs eval.Case) (llm.Client, func(error) error) {
		if *record == "" {
			return client, nil
		}
		rec := &llm.Recorder{Next: client}
		return rec, func(err error) error {
			if err != nil {
				return err
			}
			return rec.Save(filepath.Join(*record, cs.Name+".json"))
		}
	})
	fmt.Printf("model %s\n\n", c.LLM.Model)
	return eval.Print(os.Stdout, results)
}
