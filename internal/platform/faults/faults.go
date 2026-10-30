
package faults

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/Gandalff2k/saga3/internal/platform/retry"
)


var ErrInjected = errors.New("injected fault")

type Config struct {
	TransientRate float64 `env:"FAULT_TRANSIENT_RATE" envDefault:"0"`
	PermanentRate float64 `env:"FAULT_PERMANENT_RATE" envDefault:"0"`
}

func (c Config) Maybe(op string) error {
	switch r := rand.Float64(); { 
	case r < c.PermanentRate:
		return retry.Permanent(fmt.Errorf("%w: permanent failure in %s", ErrInjected, op))
	case r < c.PermanentRate+c.TransientRate:
		return fmt.Errorf("%w: transient failure in %s", ErrInjected, op)
	}
	return nil
}
