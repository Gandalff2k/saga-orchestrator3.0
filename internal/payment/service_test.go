package payment

import "testing"

func TestDecide(t *testing.T) {
	tests := []struct {
		amount, limit int64
		want          decision
	}{
		{100, 1000, decision{status: statusCharged}},
		{1000, 1000, decision{status: statusCharged}}, 
		{1001, 1000, decision{status: statusDeclined, reason: reasonLimitExceeded}},
	}
	for _, tt := range tests {
		if got := decide(tt.amount, tt.limit); got != tt.want {
			t.Errorf("decide(%d, %d) = %+v, want %+v", tt.amount, tt.limit, got, tt.want)
		}
	}
}
