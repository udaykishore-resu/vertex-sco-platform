// Package domain defines the event contracts exchanged between Vertex SCO
// Platform services. All cross-service coordination happens by publishing
// and subscribing to these events over the event bus (see internal/eventbus) —
// this is what replaces the old point-to-point "everyone calls the core
// service directly" coupling with a decoupled, event-driven design.
package domain

import "time"

// EventType is a stable, versioned identifier for a domain event.
type EventType string

const (
	EventLaneStateTransitioned  EventType = "lane.state.transitioned.v1"
	EventInterventionRequested  EventType = "intervention.requested.v1"
	EventInterventionResolved   EventType = "intervention.resolved.v1"
	EventWeightObserved         EventType = "weight.observed.v1"
	EventWeightMismatchDetected EventType = "weight.mismatch_detected.v1"
	EventCouponLimitExceeded    EventType = "coupon.limit_exceeded.v1"
	EventVisualVerifyRequested  EventType = "visualverify.requested.v1"
	EventConfigPublished        EventType = "config.published.v1"
	EventConfigPromoted         EventType = "config.promoted.v1"
	EventConfigRolledBack       EventType = "config.rolled_back.v1"
	EventDeploymentHealthReport EventType = "deployment.health_report.v1"
)

// LaneState is the set of states the vertex-core state machine can be in.
// This is intentionally small and explicit — see internal/statemachine (used
// by cmd/vertex-core) for the transition table.
type LaneState string

const (
	StateIdle         LaneState = "IDLE"
	StateScanning     LaneState = "SCANNING"
	StateWeighing     LaneState = "WEIGHING"
	StateIntervention LaneState = "INTERVENTION"
	StatePayment      LaneState = "PAYMENT"
	StateComplete     LaneState = "COMPLETE"
	StateDegraded     LaneState = "DEGRADED" // explicit fallback mode (flaw #1 fix)
)

// TraceContext carries a W3C-traceparent-compatible correlation context so a
// single shopper transaction can be followed terminal -> store -> cloud.
// See internal/tracing for span creation/propagation helpers.
type TraceContext struct {
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
}

// Envelope wraps every event published on the bus. Wrapping (rather than
// publishing bare payloads) gives every consumer a uniform place to find
// trace context, schema version, idempotency key, and origin — required for
// the offline replay / outbox pattern (internal/outbox) and for tracing.
type Envelope struct {
	ID         string       `json:"id"` // idempotency key (UUID)
	Type       EventType    `json:"type"`
	Source     string       `json:"source"` // originating service name
	LaneID     string       `json:"lane_id"`
	StoreID    string       `json:"store_id"`
	OccurredAt time.Time    `json:"occurred_at"`
	Trace      TraceContext `json:"trace"`
	Payload    any          `json:"payload"`
}

// --- Payload types -----------------------------------------------------

type LaneStateTransitioned struct {
	From   LaneState `json:"from"`
	To     LaneState `json:"to"`
	Reason string    `json:"reason"`
}

type InterventionRequested struct {
	InterventionID string `json:"intervention_id"`
	Kind           string `json:"kind"` // weight_mismatch | coupon_limit | visual_verify | security
	Reason         string `json:"reason"`
	RequiresRole   string `json:"requires_role"` // e.g. "associate", "security"
}

type InterventionResolved struct {
	InterventionID string `json:"intervention_id"`
	ResolvedBy     string `json:"resolved_by"`
	Outcome        string `json:"outcome"` // approved | denied
}

type WeightObserved struct {
	ExpectedGrams float64 `json:"expected_grams"`
	ObservedGrams float64 `json:"observed_grams"`
	TolerancePct  float64 `json:"tolerance_pct"`
}

type WeightMismatchDetected struct {
	DeltaGrams float64 `json:"delta_grams"`
}

type ConfigPublished struct {
	ServiceName string `json:"service_name"`
	Version     int    `json:"version"`
	Checksum    string `json:"checksum"`
	CanaryPct   int    `json:"canary_pct"`
}

type ConfigPromoted struct {
	ServiceName string `json:"service_name"`
	Version     int    `json:"version"`
}

type ConfigRolledBack struct {
	ServiceName string `json:"service_name"`
	FromVersion int    `json:"from_version"`
	ToVersion   int    `json:"to_version"`
	Reason      string `json:"reason"`
}

type DeploymentHealthReport struct {
	StoreID      string  `json:"store_id"`
	ServiceName  string  `json:"service_name"`
	Version      int     `json:"version"`
	ErrorRatePct float64 `json:"error_rate_pct"`
	Healthy      bool    `json:"healthy"`
}
