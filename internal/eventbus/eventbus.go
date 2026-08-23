// Package eventbus abstracts publish/subscribe messaging so services never
// depend on a concrete broker. Two implementations ship here:
//
//   - Memory: an in-process bus (unit tests, local single-binary dev)
//   - MQTT:   a dependency-free MQTT 3.1.1 client (internal/eventbus/mqtt.go)
//     that talks to a clustered broker (see deploy/docker-compose.yml, which
//     runs a 3-node EMQX cluster behind HAProxy) with QoS1, persistent
//     sessions, and automatic reconnect with backoff — this directly
//     addresses "MQTT broker has no stated HA story" from the architecture
//     review: the client survives a broker-node failover, and QoS1 +
//     persistent session means messages published while a client is briefly
//     disconnected are not silently dropped.
//
// Handlers receive a domain.Envelope, not a raw byte payload, keeping the
// wire format (JSON today) an implementation detail.
package eventbus

import "github.com/udaykishore-resu/vertex-sco-platform/internal/domain"

// Handler processes one received envelope. Returning an error signals the
// bus implementation that the message was not successfully processed; QoS1
// implementations should not ack until Handler returns nil (at-least-once
// delivery), and callers that need exactly-once semantics should make their
// handler idempotent using Envelope.ID (see internal/outbox for a pattern).
type Handler func(domain.Envelope) error

// Bus is the minimal contract every transport must satisfy.
type Bus interface {
	// Publish sends an envelope on a topic. QoS/durability semantics are
	// transport-specific; the MQTT implementation uses QoS1 by default.
	Publish(topic string, env domain.Envelope) error

	// Subscribe registers a handler for a topic (MQTT wildcard patterns like
	// "vertex/+/weight/#" are supported by the MQTT implementation).
	Subscribe(topic string, h Handler) error

	// Close releases the underlying connection/goroutines.
	Close() error
}

// Topic builds the canonical topic name for an event type, scoped by store
// and lane so subscribers can filter with MQTT wildcards, e.g.
// "vertex/store-42/+/intervention.requested.v1".
func Topic(storeID, laneID string, t domain.EventType) string {
	if laneID == "" {
		laneID = "_"
	}
	return "vertex/" + storeID + "/" + laneID + "/" + string(t)
}
