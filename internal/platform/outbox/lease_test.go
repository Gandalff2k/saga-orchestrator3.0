package outbox

import (
	"slices"
	"testing"
)

func TestFairShare(t *testing.T) {
	tests := []struct{ total, alive, want int }{
		{16, 1, 16},
		{16, 2, 8},
		{16, 3, 6}, 
		{16, 16, 1},
		{16, 20, 1}, 
		{16, 0, 16}, 
	}
	for _, tt := range tests {
		if got := fairShare(tt.total, tt.alive); got != tt.want {
			t.Errorf("fairShare(%d, %d) = %d, want %d", tt.total, tt.alive, got, tt.want)
		}
	}
}

func TestPlan(t *testing.T) {
	tests := []struct {
		name        string
		owned       []int
		total       int
		alive       int
		wantKeep    []int
		wantRelease []int
		wantClaim   int
	}{
		{"alone, owns nothing: claim all", nil, 16, 1, nil, nil, 16},
		{"alone, owns all: steady", seq(0, 16), 16, 1, seq(0, 16), nil, 0},
		{"second replica joined: give back half", seq(0, 16), 16, 2, seq(0, 8), seq(8, 16), 0},
		{"under fair share: claim the rest", []int{3, 1}, 16, 2, []int{1, 3}, nil, 6},
		{"unsorted input: release highest", []int{9, 2, 15, 4}, 16, 8, []int{2, 4}, []int{9, 15}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keep, release, claim := plan(tt.owned, tt.total, tt.alive)
			if !slices.Equal(keep, tt.wantKeep) || !slices.Equal(release, tt.wantRelease) || claim != tt.wantClaim {
				t.Fatalf("plan = keep %v release %v claim %d; want %v %v %d",
					keep, release, claim, tt.wantKeep, tt.wantRelease, tt.wantClaim)
			}
		})
	}
}

func seq(from, to int) []int {
	var out []int
	for i := from; i < to; i++ {
		out = append(out, i)
	}
	return out
}
