package resilience

import (
	"context"
	"errors"
)

var ErrBulkheadFull = errors.New("resilience: bulkhead capacity exceeded")

// Bulkhead limits how many concurrent calls to a given dependency are in
// flight, so a dependency that has gone merely *slow* (not yet failing
// enough to trip the CircuitBreaker) can't consume every goroutine/worker in
// vertex-core and starve unrelated lanes. Pair with CircuitBreaker: the
// breaker handles "is this dependency healthy", the bulkhead handles "how
// much of my own capacity am I willing to spend waiting on it".
type Bulkhead struct {
	sem chan struct{}
}

func NewBulkhead(maxConcurrent int) *Bulkhead {
	return &Bulkhead{sem: make(chan struct{}, maxConcurrent)}
}

// Execute runs fn if a slot is free, blocking up to ctx's deadline for one.
func (b *Bulkhead) Execute(ctx context.Context, fn func(context.Context) error) error {
	select {
	case b.sem <- struct{}{}:
		defer func() { <-b.sem }()
		return fn(ctx)
	case <-ctx.Done():
		return ErrBulkheadFull
	}
}

// InFlight returns the current number of occupied slots (for /metrics).
func (b *Bulkhead) InFlight() int { return len(b.sem) }

// Capacity returns the configured maximum concurrency.
func (b *Bulkhead) Capacity() int { return cap(b.sem) }
