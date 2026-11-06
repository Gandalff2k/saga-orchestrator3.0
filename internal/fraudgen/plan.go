// Package fraudgen generates labelled synthetic traffic and scores the risk service against the labels.
package fraudgen

import (
	"fmt"
	"math/rand/v2"
)

// Scenarios.
const (
	Normal      = "normal"
	BigSpender  = "big_spender"
	CardTesting = "card_testing"
	StolenCards = "stolen_cards"
	Takeover    = "account_takeover"
	IPFarm      = "ip_farm"
)

// Order is one request the generator sends, with its ground-truth label.
type Order struct {
	Scenario    string `json:"scenario"`
	Fraud       bool   `json:"fraud"`
	CustomerID  string `json:"customer_id"`
	CardNumber  string `json:"-"`
	IP          string `json:"ip_address"`
	Country     string `json:"country"`
	AmountCents int64  `json:"amount_cents"`
}

// Mix says how many actors of each scenario to generate.
type Mix struct {
	Normal, BigSpender, CardTesting, StolenCards, Takeover, IPFarm int
}

// DefaultMix is roughly 15% fraud by orders.
var DefaultMix = Mix{Normal: 150, BigSpender: 10, CardTesting: 6, StolenCards: 6, Takeover: 8, IPFarm: 4}

var countries = []string{"UA", "PL", "DE", "GB", "US", "NL"}

type gen struct {
	r  *rand.Rand
	id int
}

// Plan returns history first (orders that must exist before the attack), then
// the rest shuffled. The same seed gives the same plan.
func Plan(seed uint64, m Mix) (history, live []Order) {
	g := &gen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))} //nolint:gosec // G404: synthetic data
	for range m.Normal {
		c := g.customer()
		for range 1 + g.r.IntN(3) {
			live = append(live, c.order(Normal, false, g.amount(5_00, 60_00)))
		}
	}
	for range m.BigSpender {
		c := g.customer()
		live = append(live, c.order(BigSpender, false, g.amount(120_00, 400_00)))
	}
	for range m.CardTesting {
		c := g.customer()
		for range 6 + g.r.IntN(5) {
			live = append(live, c.order(CardTesting, true, g.amount(50, 2_00)))
		}
	}
	for range m.StolenCards {
		c := g.customer()
		for i := range 4 {
			if i > 0 {
				c.card = g.card()
			}
			live = append(live, c.order(StolenCards, true, g.amount(20_00, 80_00)))
		}
	}
	for range m.Takeover {
		c := g.customer()
		for range 4 {
			history = append(history, c.order(Normal, false, g.amount(10_00, 20_00)))
		}
		c.country = g.otherCountry(c.country)
		c.ip = g.ip()
		live = append(live, c.order(Takeover, true, g.amount(90_00, 300_00)))
	}
	for range m.IPFarm {
		ip, country := g.ip(), g.country()
		for range 6 {
			c := g.customer()
			c.ip, c.country = ip, country
			live = append(live, c.order(IPFarm, true, g.amount(10_00, 30_00)))
		}
	}
	g.r.Shuffle(len(live), func(i, j int) { live[i], live[j] = live[j], live[i] })
	return history, live
}

type actor struct {
	id, card, ip, country string
}

func (a actor) order(scenario string, fraud bool, amount int64) Order {
	return Order{Scenario: scenario, Fraud: fraud, CustomerID: a.id, CardNumber: a.card, IP: a.ip,
		Country: a.country, AmountCents: amount}
}

func (g *gen) customer() actor {
	g.id++
	return actor{id: fmt.Sprintf("gen_%05d", g.id), card: g.card(), ip: g.ip(), country: g.country()}
}

func (g *gen) card() string {
	return fmt.Sprintf("4%015d", g.r.Int64N(1_000_000_000_000_000))
}

func (g *gen) ip() string {
	return fmt.Sprintf("%d.%d.%d.%d", 11+g.r.IntN(200), g.r.IntN(256), g.r.IntN(256), 1+g.r.IntN(254))
}

func (g *gen) country() string { return countries[g.r.IntN(len(countries))] }

func (g *gen) otherCountry(c string) string {
	for {
		if n := g.country(); n != c {
			return n
		}
	}
}

func (g *gen) amount(minCents, maxCents int64) int64 {
	return minCents + g.r.Int64N(maxCents-minCents+1)
}
