package resilience

import (
	"context"
	"log"
	"sync"
	"time"
)

// Guard is the single object a service holds per downstream dependency,
// combining a CircuitBreaker and a Bulkhead. vertex-core keeps one Guard per
// dependent service (intervention, weight, coupon, visualverify, trilight)
// so each dependency fails independently.
type Guard struct {
	Name    string
	Breaker *CircuitBreaker
	Bulk    *Bulkhead
}

func NewGuard(name string, breakerCfg CircuitBreakerConfig, maxConcurrent int) *Guard {
	g := &Guard{
		Name:    name,
		Breaker: NewCircuitBreaker(name, breakerCfg),
		Bulk:    NewBulkhead(maxConcurrent),
	}
	g.Breaker.OnStateChange(func(name string, from, to State) {
		log.Printf("[resilience] guard %q circuit %s -> %s", name, from, to)
	})
	return g
}

// Call executes fn behind both the bulkhead and the circuit breaker, with a
// hard per-call timeout so a hung dependency can never block a checkout lane
// indefinitely — the caller (e.g. vertex-core) decides what to do when Call
// returns an error: fall back to degraded mode (domain.StateDegraded)
// rather than hang.
func (g *Guard) Call(parent context.Context, timeout time.Duration, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return g.Bulk.Execute(ctx, func(ctx context.Context) error {
		return g.Breaker.Execute(ctx, fn)
	})
}

// Registry keeps a set of named Guards so a service can look one up per
// downstream call site and expose their state on a /health or /metrics
// endpoint.
type Registry struct {
	mu     sync.RWMutex
	guards map[string]*Guard
}

func NewRegistry() *Registry { return &Registry{guards: make(map[string]*Guard)} }

func (r *Registry) Get(name string, breakerCfg CircuitBreakerConfig, maxConcurrent int) *Guard {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.guards[name]; ok {
		return g
	}
	g := NewGuard(name, breakerCfg, maxConcurrent)
	r.guards[name] = g
	return g
}

// Snapshot returns a name->state map for a status endpoint.
func (r *Registry) Snapshot() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(r.guards))
	for name, g := range r.guards {
		out[name] = g.Breaker.State().String()
	}
	return out
}
