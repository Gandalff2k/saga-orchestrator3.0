// Package faults injects failures on purpose, so retries, backoff and the
// DLQ can be watched working in a local or k8s run. Off by default.
package faults

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

var ErrInjected = errors.New("injected fault")

type Config struct {
	// TransientRate: probability (0..1) that an operation fails with a
	// retryable error. 0.3 => most messages succeed after a retry or two.
	TransientRate float64 `env:"FAULT_TRANSIENT_RATE" envDefault:"0"`
	// PermanentRate: probability that an operation fails permanently -> DLQ.
	PermanentRate float64 `env:"FAULT_PERMANENT_RATE" envDefault:"0"`
}

// Maybe returns an injected error for op according to the configured rates.
func (c Config) Maybe(op string) error {
	switch r := rand.Float64(); {
	case r < c.PermanentRate:
		return retry.Permanent(fmt.Errorf("%w: permanent failure in %s", ErrInjected, op))
	case r < c.PermanentRate+c.TransientRate:
		return fmt.Errorf("%w: transient failure in %s", ErrInjected, op)
	}
	return nil
}
