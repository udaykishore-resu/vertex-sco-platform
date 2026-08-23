package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerTripsAndRecovers(t *testing.T) {
	cb := NewCircuitBreaker("test", CircuitBreakerConfig{FailureThreshold: 3, OpenTimeout: 100 * time.Millisecond, HalfOpenMaxCalls: 1})
	boom := errors.New("boom")

	for i := 0; i < 3; i++ {
		err := cb.Execute(context.Background(), func(context.Context) error { return boom })
		if err != boom {
			t.Fatalf("call %d: expected boom, got %v", i, err)
		}
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected OPEN after 3 consecutive failures, got %s", cb.State())
	}

	// Immediately calling again should be rejected without invoking fn.
	called := false
	err := cb.Execute(context.Background(), func(context.Context) error { called = true; return nil })
	if err != ErrOpenCircuit || called {
		t.Fatalf("expected fast-fail while OPEN, got err=%v called=%v", err, called)
	}

	time.Sleep(150 * time.Millisecond) // let OpenTimeout elapse

	if err := cb.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("expected trial call to succeed in HALF_OPEN, got %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected CLOSED after successful trial call, got %s", cb.State())
	}
}

func TestBulkheadLimitsConcurrency(t *testing.T) {
	b := NewBulkhead(2)
	release := make(chan struct{})
	started := make(chan struct{}, 3)

	for i := 0; i < 2; i++ {
		go b.Execute(context.Background(), func(ctx context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		})
	}
	<-started
	<-started // both slots occupied

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := b.Execute(ctx, func(context.Context) error { return nil })
	if err != ErrBulkheadFull {
		t.Fatalf("expected bulkhead full error, got %v", err)
	}
	close(release)
}
