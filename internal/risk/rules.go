// Package risk is the saga participant that scores orders for fraud and runs manual review.
package risk

import (
	"fmt"
	"strings"
)

// RulesVersion is stored with every decision, so an old decision can be explained by the rules that made it.
const RulesVersion = "v1"

// Decision is the outcome of scoring.
type Decision string

// Decisions.
const (
	Allow  Decision = "ALLOW"
	Review Decision = "REVIEW"
	Block  Decision = "BLOCK"
)

// Config is the risk worker's business configuration.
type Config struct {
	ReviewLimitCents int64 `env:"RISK_REVIEW_LIMIT_CENTS" envDefault:"10000"`
	ReviewScore      int   `env:"RISK_REVIEW_SCORE" envDefault:"30"`
	BlockScore       int   `env:"RISK_BLOCK_SCORE" envDefault:"70"`
}

// Features describe the order's history; every count includes the order itself.
type Features struct {
	CardSmallTxn10m     int
	CustomerCards24h    int
	CustomerPrior90d    int
	CustomerCountrySeen bool
	CustomerAvgCents    int64
	IPCustomers1h       int
}

// Hit is one rule that fired.
type Hit struct {
	Rule   string `json:"rule"`
	Points int    `json:"points"`
	Detail string `json:"detail"`
}

// Result is a decision with the rules behind it.
type Result struct {
	Decision Decision
	Score    int
	Hits     []Hit
}

const (
	smallAmountCents  = 500
	cardTestingCount  = 4
	manyCardsCount    = 3
	spikeMinHistory   = 3
	spikeFactor       = 5
	sharedIPCustomers = 5
)

// Evaluate scores one order. It is pure: same input, same result.
func Evaluate(amountCents int64, f Features, cfg Config) Result {
	var hits []Hit
	add := func(rule string, points int, detail string, args ...any) {
		hits = append(hits, Hit{Rule: rule, Points: points, Detail: fmt.Sprintf(detail, args...)})
	}

	if amountCents <= smallAmountCents && f.CardSmallTxn10m >= cardTestingCount {
		add("card_testing", 70, "%d payments of at most %d cents from this card in 10 minutes",
			f.CardSmallTxn10m, smallAmountCents)
	}
	if f.CustomerCards24h >= manyCardsCount {
		add("many_cards", 50, "%d different cards used by this customer in 24 hours", f.CustomerCards24h)
	}
	if f.CustomerPrior90d > 0 && !f.CustomerCountrySeen {
		add("new_country", 30, "first order from this country after %d earlier orders", f.CustomerPrior90d)
	}
	if f.CustomerPrior90d >= spikeMinHistory && amountCents > spikeFactor*f.CustomerAvgCents {
		add("amount_spike", 30, "%d cents is more than %dx the customer's average of %d",
			amountCents, spikeFactor, f.CustomerAvgCents)
	}
	if f.IPCustomers1h >= sharedIPCustomers {
		add("shared_ip", 40, "%d different customers from this IP in 1 hour", f.IPCustomers1h)
	}
	overLimit := amountCents > cfg.ReviewLimitCents
	if overLimit {
		add("over_limit", 0, "%d cents is above the %d cents review limit", amountCents, cfg.ReviewLimitCents)
	}

	score := 0
	for _, h := range hits {
		score += h.Points
	}

	r := Result{Decision: Allow, Score: score, Hits: hits}
	switch {
	case score >= cfg.BlockScore:
		r.Decision = Block
	case score >= cfg.ReviewScore || overLimit:
		r.Decision = Review
	}
	return r
}

// RuleNames lists the rules that fired, for logs and reply reasons.
func (r Result) RuleNames() string {
	names := make([]string, 0, len(r.Hits))
	for _, h := range r.Hits {
		names = append(names, h.Rule)
	}
	return strings.Join(names, ", ")
}
