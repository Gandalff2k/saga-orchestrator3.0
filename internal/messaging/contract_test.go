package messaging

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/*.json golden files")

var contract = map[string]any{
	CmdReserveSeat:           ReserveSeat{SeatID: 12},
	CmdReleaseSeat:           ReleaseSeat{SeatID: 12},
	CmdCharge:                Charge{AmountCents: 12_345},
	CmdRefund:                Refund{},
	EvtSeatReserved:          Reply{},
	EvtSeatReservationFailed: Reply{Reason: "seat already taken"},
	EvtSeatReleased:          Reply{},
	EvtPaymentCharged:        Reply{},
	EvtPaymentFailed:         Reply{Reason: "amount exceeds the payment limit"},
	EvtPaymentRefunded:       Reply{},
}

func TestContract_PayloadsMatchGoldenFiles(t *testing.T) {
	for typ, payload := range contract {
		t.Run(typ, func(t *testing.T) {
			got, err := json.MarshalIndent(payload, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join("testdata", typ+".json")
			if *update {
				if err := os.WriteFile(path, append(got, '\n'), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("no golden file for %s (run with -update for a NEW type): %v", typ, err)
			}
			if string(append(got, '\n')) != string(want) {
				t.Errorf("wire format of %s changed:\n got: %s\nwant: %s\nAdd a new version (%s -> .v2) instead of changing this one.",
					typ, got, want, typ)
			}
		})
	}
}

func TestContract_UnknownFieldsAreIgnored(t *testing.T) {
	var c Charge
	if err := json.Unmarshal([]byte(`{"amount_cents": 100, "currency": "EUR"}`), &c); err != nil || c.AmountCents != 100 {
		t.Fatalf("v1 consumer broke on an added field: %+v, %v", c, err)
	}
}

var versioned = regexp.MustCompile(`^[a-z]+\.[a-z_]+\.v[0-9]+$`)

func TestContract_AllTypesCovered(t *testing.T) {
	all := []string{CmdReserveSeat, CmdReleaseSeat, CmdCharge, CmdRefund,
		EvtSeatReserved, EvtSeatReservationFailed, EvtSeatReleased,
		EvtPaymentCharged, EvtPaymentFailed, EvtPaymentRefunded}
	seen := map[string]bool{}
	for _, typ := range all {
		if !versioned.MatchString(typ) {
			t.Errorf("%q: message types are <service>.<name>.v<N>", typ)
		}
		if seen[typ] {
			t.Errorf("%q declared twice", typ)
		}
		seen[typ] = true
		if _, ok := contract[typ]; !ok {
			t.Errorf("%q has no golden payload in contract", typ)
		}
	}
	if len(contract) != len(all) {
		t.Errorf("contract has %d types, constants list %d", len(contract), len(all))
	}
}
