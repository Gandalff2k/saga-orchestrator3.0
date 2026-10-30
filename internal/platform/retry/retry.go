package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

type Policy struct {
	MaxAttempts int           `env:"MAX_ATTEMPTS" envDefault:"5"` 
	Initial     time.Duration `env:"INITIAL_BACKOFF" envDefault:"200ms"`
	Max         time.Duration `env:"MAX_BACKOFF" envDefault:"10s"`
	Multiplier  float64       `env:"BACKOFF_MULTIPLIER" envDefault:"2"`
	Jitter      float64       `env:"BACKOFF_JITTER" envDefault:"0.2"`
}


func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.Initial) * math.Pow(p.Multiplier, float64(attempt-1))
	if p.Max > 0 && d > float64(p.Max) {
		d = float64(p.Max)
	}
	if p.Jitter > 0 {
		d += d * p.Jitter * (2*rand.Float64() - 1) 
	}
	return time.Duration(d)
}


func Do(ctx context.Context, p Policy, fn func(ctx context.Context, attempt int) error) error {
	for attempt := 1; ; attempt++ {
		err := fn(ctx, attempt)
		if err == nil {
			return nil
		}
		if IsPermanent(err) {
			return err
		}
		if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
			return &ExhaustedError{Attempts: attempt, Err: err}
		}

		t := time.NewTimer(p.Backoff(attempt))
		select {
		case <-ctx.Done():
			t.Stop()
			return errors.Join(ctx.Err(), err)
		case <-t.C:
		}
	}
}

type ExhaustedError struct {
	Attempts int
	Err      error
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("gave up after %d attempts: %v", e.Attempts, e.Err)
}

func (e *ExhaustedError) Unwrap() error { return e.Err }

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
