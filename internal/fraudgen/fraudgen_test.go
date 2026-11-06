package fraudgen

import (
	"slices"
	"testing"
)

func TestPlan_IsDeterministic(t *testing.T) {
	h1, l1 := Plan(7, DefaultMix)
	h2, l2 := Plan(7, DefaultMix)
	if !slices.Equal(h1, h2) || !slices.Equal(l1, l2) {
		t.Fatal("same seed gave different plans")
	}
	_, l3 := Plan(8, DefaultMix)
	if slices.Equal(l1, l3) {
		t.Fatal("different seeds gave the same plan")
	}
}

func TestPlan_ScenariosHaveTheirShape(t *testing.T) {
	history, live := Plan(1, Mix{CardTesting: 1, StolenCards: 1, Takeover: 1, IPFarm: 1})

	by := map[string][]Order{}
	for _, o := range live {
		by[o.Scenario] = append(by[o.Scenario], o)
		if !o.Fraud {
			t.Errorf("%s order labelled legit", o.Scenario)
		}
	}

	cards := map[string]bool{}
	for _, o := range by[CardTesting] {
		cards[o.CardNumber] = true
		if o.AmountCents > 2_00 {
			t.Errorf("card testing amount %d", o.AmountCents)
		}
	}
	if len(by[CardTesting]) < 6 || len(cards) != 1 {
		t.Errorf("card testing: %d orders on %d cards, want >= 6 on one", len(by[CardTesting]), len(cards))
	}

	cards = map[string]bool{}
	for _, o := range by[StolenCards] {
		cards[o.CardNumber] = true
	}
	if len(cards) != 4 {
		t.Errorf("stolen cards: %d distinct cards, want 4", len(cards))
	}

	if len(history) != 4 || len(by[Takeover]) != 1 {
		t.Fatalf("takeover: %d history orders and %d attacks", len(history), len(by[Takeover]))
	}
	attack := by[Takeover][0]
	if attack.CustomerID != history[0].CustomerID || attack.Country == history[0].Country {
		t.Errorf("takeover must be the same customer from a new country: %+v vs %+v", attack, history[0])
	}

	ips, customers := map[string]bool{}, map[string]bool{}
	for _, o := range by[IPFarm] {
		ips[o.IP], customers[o.CustomerID] = true, true
	}
	if len(ips) != 1 || len(customers) != 6 {
		t.Errorf("ip farm: %d ips, %d customers", len(ips), len(customers))
	}
}

func TestScore(t *testing.T) {
	r := Score([]Outcome{
		{Normal, false, "ALLOW"},
		{Normal, false, "ALLOW"},
		{Normal, false, "REVIEW"},
		{BigSpender, false, "REVIEW"},
		{CardTesting, true, "ALLOW"},
		{CardTesting, true, "BLOCK"},
		{CardTesting, true, "BLOCK"},
		{Takeover, true, "REVIEW"},
		{IPFarm, true, ""},
	})
	if r.TruePos != 3 || r.FalsePos != 2 || r.FraudPassed != 1 || r.Unscored != 1 {
		t.Fatalf("report = %+v", r)
	}
	if r.Precision != 3.0/5 || r.Recall != 3.0/4 || r.LegitReviewRate != 2.0/4 || r.LegitBlockRate != 0 {
		t.Fatalf("precision %v recall %v review %v block %v", r.Precision, r.Recall, r.LegitReviewRate, r.LegitBlockRate)
	}
	if r.Rows[0].Scenario != Takeover {
		t.Errorf("rows not sorted: %v", r.Rows)
	}
}
