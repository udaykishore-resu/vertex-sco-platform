// Package statemachine implements the lane state machine hosted by
// vertex-core (renamed from scoxcoreservice). Compared to the original
// design, two things changed per the architecture review:
//
//  1. Transitions are emitted as domain events (published on the bus) after
//     they happen, rather than vertex-core synchronously calling out to
//     every dependent service to ask permission inline. Dependent services
//     (vertex-weight, vertex-coupon, vertex-visualverify, ...) subscribe and
//     react; vertex-core does not block on them for unrelated lane
//     operations. This is the "event-driven decoupling" / CQRS-flavored fix
//     for flaw #1.
//  2. There is an explicit StateDegraded state: if a required dependency
//     guard (see internal/resilience) is open/unavailable when a transition
//     needs its input, the machine moves to DEGRADED instead of hanging —
//     giving the lane (and the shopper-facing UI) a defined, visible state
//     rather than an indefinite stall.
package statemachine

import (
	"fmt"
	"sync"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
)

// transitions maps the allowed From -> {To...} moves. Anything not listed
// is rejected by Fire, which keeps the machine's behavior auditable in one
// place instead of scattered across services.
var transitions = map[domain.LaneState][]domain.LaneState{
	domain.StateIdle:         {domain.StateScanning, domain.StateDegraded},
	domain.StateScanning:     {domain.StateWeighing, domain.StateIntervention, domain.StatePayment, domain.StateDegraded, domain.StateIdle},
	domain.StateWeighing:     {domain.StateScanning, domain.StateIntervention, domain.StateDegraded},
	domain.StateIntervention: {domain.StateScanning, domain.StateWeighing, domain.StateIdle, domain.StateDegraded},
	domain.StatePayment:      {domain.StateComplete, domain.StateIntervention, domain.StateDegraded},
	domain.StateComplete:     {domain.StateIdle},
	domain.StateDegraded:     {domain.StateIdle, domain.StateScanning}, // recovery paths
}

// Listener is notified after every successful transition — vertex-core's
// main.go wires one that publishes domain.LaneStateTransitioned on the bus
// and records a trace span.
type Listener func(laneID string, from, to domain.LaneState, reason string)

// Machine is safe for concurrent use across multiple lanes: callers key
// their own per-lane instance (see cmd/vertex-core, one Machine per active
// lane ID).
type Machine struct {
	mu        sync.Mutex
	laneID    string
	state     domain.LaneState
	listeners []Listener
}

func New(laneID string) *Machine {
	return &Machine{laneID: laneID, state: domain.StateIdle}
}

func (m *Machine) State() domain.LaneState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *Machine) OnTransition(l Listener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, l)
}

// Fire attempts From(current) -> to. It returns an error (and leaves state
// unchanged) if the move isn't in the allowed transition table.
func (m *Machine) Fire(to domain.LaneState, reason string) error {
	m.mu.Lock()
	from := m.state
	allowed := transitions[from]
	ok := false
	for _, s := range allowed {
		if s == to {
			ok = true
			break
		}
	}
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("statemachine: illegal transition %s -> %s", from, to)
	}
	m.state = to
	listeners := append([]Listener(nil), m.listeners...)
	m.mu.Unlock()

	for _, l := range listeners {
		l(m.laneID, from, to, reason)
	}
	return nil
}

// Degrade force-moves the machine to StateDegraded regardless of the normal
// transition table — always legal, since every state can degrade, and is
// what a Guard/CircuitBreaker failure (internal/resilience) should trigger
// instead of leaving the lane hung on a dependency call.
func (m *Machine) Degrade(reason string) {
	m.mu.Lock()
	from := m.state
	if from == domain.StateDegraded {
		m.mu.Unlock()
		return
	}
	m.state = domain.StateDegraded
	listeners := append([]Listener(nil), m.listeners...)
	m.mu.Unlock()
	for _, l := range listeners {
		l(m.laneID, from, domain.StateDegraded, reason)
	}
}
