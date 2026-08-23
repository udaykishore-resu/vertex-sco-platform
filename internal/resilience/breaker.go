// Package resilience provides circuit breaker and bulkhead primitives.
// vertex-core wraps every downstream call (to vertex-intervention,
// vertex-weight, vertex-coupon, etc.) through a CircuitBreaker + Bulkhead
// pair so a slow or failing dependent service degrades that one interaction
// instead of stalling the whole checkout lane (architecture review flaw #1
// and improvement "circuit breakers/bulkheads around scoxcoreservice's
// dependencies").
package resilience

import (
	"context"
	"errors"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	}
	return "UNKNOWN"
}

var ErrOpenCircuit = errors.New("resilience: circuit is open")

// CircuitBreakerConfig tunes trip/reset behavior.
type CircuitBreakerConfig struct {
	FailureThreshold int           // consecutive failures before tripping to OPEN
	OpenTimeout      time.Duration // how long to stay OPEN before trying HALF_OPEN
	HalfOpenMaxCalls int           // trial calls allowed while HALF_OPEN
}

func DefaultConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		OpenTimeout:      10 * time.Second,
		HalfOpenMaxCalls: 3,
	}
}

// CircuitBreaker is a small, dependency-free state machine: CLOSED -> OPEN
// on repeated failure, OPEN -> HALF_OPEN after a cooldown, HALF_OPEN -> CLOSED
// on trial success or back to OPEN on trial failure.
type CircuitBreaker struct {
	name string
	cfg  CircuitBreakerConfig

	mu               sync.Mutex
	state            State
	consecutiveFail  int
	openedAt         time.Time
	halfOpenInFlight int

	onStateChange func(name string, from, to State)
}

func NewCircuitBreaker(name string, cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{name: name, cfg: cfg, state: StateClosed}
}

// OnStateChange registers a callback (used by vertex-core to emit a metric /
// log line whenever a downstream dependency trips).
func (cb *CircuitBreaker) OnStateChange(fn func(name string, from, to State)) {
	cb.onStateChange = fn
}

func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *CircuitBreaker) setState(s State) {
	if cb.state == s {
		return
	}
	old := cb.state
	cb.state = s
	if cb.onStateChange != nil {
		go cb.onStateChange(cb.name, old, s)
	}
}

// Allow reports whether a call may proceed right now, transitioning
// OPEN -> HALF_OPEN once the cooldown has elapsed.
func (cb *CircuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.openedAt) >= cb.cfg.OpenTimeout {
			cb.setState(StateHalfOpen)
			cb.halfOpenInFlight = 0
			return cb.tryHalfOpenSlot()
		}
		return false
	case StateHalfOpen:
		return cb.tryHalfOpenSlot()
	}
	return false
}

// tryHalfOpenSlot must be called with cb.mu held.
func (cb *CircuitBreaker) tryHalfOpenSlot() bool {
	if cb.halfOpenInFlight >= cb.cfg.HalfOpenMaxCalls {
		return false
	}
	cb.halfOpenInFlight++
	return true
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFail = 0
	if cb.state == StateHalfOpen {
		cb.setState(StateClosed)
	}
}

func (cb *CircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFail++
	if cb.state == StateHalfOpen {
		cb.setState(StateOpen)
		cb.openedAt = time.Now()
		return
	}
	if cb.state == StateClosed && cb.consecutiveFail >= cb.cfg.FailureThreshold {
		cb.setState(StateOpen)
		cb.openedAt = time.Now()
	}
}

// Execute runs fn if the circuit allows it, tracking success/failure. It
// also respects ctx cancellation/deadline so a caller-imposed timeout always
// wins even if fn itself ignores ctx.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	if !cb.allow() {
		return ErrOpenCircuit
	}
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			cb.recordFailure()
			return err
		}
		cb.recordSuccess()
		return nil
	case <-ctx.Done():
		cb.recordFailure()
		return ctx.Err()
	}
}
