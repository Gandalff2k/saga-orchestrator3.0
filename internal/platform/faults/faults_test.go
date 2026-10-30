package faults

import (
	"errors"
	"testing"

	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

func TestMaybe(t *testing.T) {
	if err := (Config{}).Maybe("op"); err != nil {
		t.Fatalf("zero config must inject nothing, got %v", err)
	}

	err := Config{TransientRate: 1}.Maybe("op")
	if !errors.Is(err, ErrInjected) || retry.IsPermanent(err) {
		t.Fatalf("transient rate 1: %v", err)
	}

	err = Config{PermanentRate: 1}.Maybe("op")
	if !errors.Is(err, ErrInjected) || !retry.IsPermanent(err) {
		t.Fatalf("permanent rate 1: %v", err)
	}

	n := 0
	for range 10_000 {
		if (Config{TransientRate: 0.3}).Maybe("op") != nil {
			n++
		}
	}
	if n < 2_700 || n > 3_300 {
		t.Fatalf("30%% rate produced %d/10000 faults", n)
	}
}
