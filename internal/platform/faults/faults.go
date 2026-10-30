// Package faults injects failures on purpose, so retries, backoff and the DLQ can be watched working.
package faults

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// ErrInjected is wrapped by every injected failure, so it can be told apart from a real one.
var ErrInjected = errors.New("injected fault")

// Config sets failure rates; the zero value injects nothing.
type Config struct {
	TransientRate float64 `env:"FAULT_TRANSIENT_RATE" envDefault:"0"`
	PermanentRate float64 `env:"FAULT_PERMANENT_RATE" envDefault:"0"`
}

// Maybe returns an injected error for op according to the configured rates.
func (c Config) Maybe(op string) error {
	switch r := rand.Float64(); { //nolint:gosec // G404: chaos testing, not security-sensitive
	case r < c.PermanentRate:
		return retry.Permanent(fmt.Errorf("%w: permanent failure in %s", ErrInjected, op))
	case r < c.PermanentRate+c.TransientRate:
		return fmt.Errorf("%w: transient failure in %s", ErrInjected, op)
	}
	return nil
}
