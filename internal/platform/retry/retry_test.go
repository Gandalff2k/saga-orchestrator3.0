package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff_ExponentialAndCapped(t *testing.T) {
	p := Policy{Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2}

	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := p.Backoff(i + 1); got != w*time.Millisecond {
			t.Errorf("attempt %d: got %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
}

func TestBackoff_JitterStaysInBounds(t *testing.T) {
	p := Policy{Initial: time.Second, Multiplier: 1, Jitter: 0.2}
	for range 1000 {
		got := p.Backoff(1)
		if got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Fatalf("backoff %v outside [800ms, 1200ms]", got)
		}
	}
}

func TestDo(t *testing.T) {
	fast := Policy{MaxAttempts: 3, Initial: time.Microsecond, Multiplier: 1}
	boom := errors.New("boom")

	tests := []struct {
		name      string
		failures  int   // fn fails this many times before succeeding
		failWith  error // error returned on failure
		wantCalls int
		check     func(t *testing.T, err error)
	}{
		{
			name: "succeeds first time", failures: 0, failWith: boom, wantCalls: 1,
			check: func(t *testing.T, err error) { mustNil(t, err) },
		},
		{
			name: "recovers after transient failures", failures: 2, failWith: boom, wantCalls: 3,
			check: func(t *testing.T, err error) { mustNil(t, err) },
		},
		{
			name: "gives up after max attempts", failures: 10, failWith: boom, wantCalls: 3,
			check: func(t *testing.T, err error) {
				var ex *ExhaustedError
				if !errors.As(err, &ex) || ex.Attempts != 3 || !errors.Is(err, boom) {
					t.Fatalf("want ExhaustedError(3) wrapping boom, got %v", err)
				}
			},
		},
		{
			name: "permanent error stops immediately", failures: 10, failWith: Permanent(boom), wantCalls: 1,
			check: func(t *testing.T, err error) {
				if !IsPermanent(err) || !errors.Is(err, boom) {
					t.Fatalf("want permanent boom, got %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := Do(context.Background(), fast, func(context.Context, int) error {
				calls++
				if calls <= tt.failures {
					return tt.failWith
				}
				return nil
			})
			if calls != tt.wantCalls {
				t.Errorf("calls = %d, want %d", calls, tt.wantCalls)
			}
			tt.check(t, err)
		})
	}
}

func TestDo_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{Initial: time.Hour, Multiplier: 1} // would block forever without cancel

	err := Do(ctx, p, func(context.Context, int) error {
		cancel()
		return errors.New("transient")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestPermanent_Nil(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
}

func mustNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
