package risk

import (
	"slices"
	"testing"
)

var cfg = Config{ReviewLimitCents: 100_00, ReviewScore: 30, BlockScore: 70}

func TestEvaluate(t *testing.T) {
	regular := Features{CardSmallTxn10m: 1, CustomerCards24h: 1, CustomerPrior90d: 5, CustomerCountrySeen: true,
		CustomerAvgCents: 15_00, IPCustomers1h: 1}

	tests := []struct {
		name      string
		amount    int64
		mutate    func(*Features)
		want      Decision
		wantRules []string
	}{
		{"regular customer", 25_00, nil, Allow, nil},
		{"first order ever", 25_00, func(f *Features) { *f = Features{CardSmallTxn10m: 1, CustomerCards24h: 1, IPCustomers1h: 1} }, Allow, nil},
		{"card testing", 1_00, func(f *Features) { f.CardSmallTxn10m = 4 }, Block, []string{"card_testing"}},
		{"many small payments but this one is big", 50_00, func(f *Features) { f.CardSmallTxn10m = 9 }, Allow, nil},
		{"many cards alone is review", 25_00, func(f *Features) { f.CustomerCards24h = 3 }, Review, []string{"many_cards"}},
		{"new country alone is review", 25_00, func(f *Features) { f.CustomerCountrySeen = false }, Review, []string{"new_country"}},
		{"account takeover", 99_00, func(f *Features) { f.CustomerCountrySeen = false }, Review, []string{"new_country", "amount_spike"}},
		{"takeover plus new cards", 99_00, func(f *Features) { f.CustomerCountrySeen = false; f.CustomerCards24h = 3 }, Block, []string{"many_cards", "new_country", "amount_spike"}},
		{"shared ip", 25_00, func(f *Features) { f.IPCustomers1h = 5 }, Review, []string{"shared_ip"}},
		{"exactly at the limit passes", 100_00, func(f *Features) { f.CustomerAvgCents = 90_00 }, Allow, nil},
		{"over the limit is never allowed automatically", 100_01, func(f *Features) { f.CustomerAvgCents = 90_00 }, Review, []string{"over_limit"}},
		{"over the limit does not soften a block", 150_00, func(f *Features) { f.CustomerAvgCents = 90_00; f.CustomerCards24h = 4; f.IPCustomers1h = 6 }, Block, []string{"many_cards", "shared_ip", "over_limit"}},
		{"spike needs history", 99_00, func(f *Features) { f.CustomerPrior90d = 2 }, Allow, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := regular
			if tt.mutate != nil {
				tt.mutate(&f)
			}
			r := Evaluate(tt.amount, f, cfg)
			if r.Decision != tt.want {
				t.Errorf("decision = %s (score %d, %s), want %s", r.Decision, r.Score, r.RuleNames(), tt.want)
			}
			var got []string
			for _, h := range r.Hits {
				got = append(got, h.Rule)
			}
			if !slices.Equal(got, tt.wantRules) {
				t.Errorf("rules = %v, want %v", got, tt.wantRules)
			}
		})
	}
}

func TestEvaluate_ScoreIsTheSumOfHits(t *testing.T) {
	f := Features{CardSmallTxn10m: 5, CustomerCards24h: 3, IPCustomers1h: 6}
	r := Evaluate(2_00, f, cfg)
	if r.Score != 70+50+40 {
		t.Fatalf("score = %d", r.Score)
	}
	for _, h := range r.Hits {
		if h.Detail == "" {
			t.Errorf("%s has no detail: a reviewer must see why it fired", h.Rule)
		}
	}
}
