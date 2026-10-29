// Package todo marks scaffolding stubs. Every use of ErrNotImplemented is a
// piece of the roadmap in README.md; grep for "todo." to see what's left.
// Delete this package once the last phase is done.
package todo

import (
	"errors"
	"fmt"
)

// ErrNotImplemented is returned by every stub; the HTTP layer maps it to 501.
var ErrNotImplemented = errors.New("not implemented")

// Phase returns ErrNotImplemented annotated with the roadmap phase and feature.
func Phase(phase int, what string) error {
	return fmt.Errorf("%w (phase %d): %s", ErrNotImplemented, phase, what)
}
