package eventbus

import (
	"sync"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
)

// Memory is an in-process pub/sub bus. It supports the same "+" single-level
// and "#" multi-level MQTT-style wildcards as the real MQTT transport so
// tests exercise identical routing logic to production.
type Memory struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

func NewMemory() *Memory {
	return &Memory{handlers: make(map[string][]Handler)}
}

func (m *Memory) Publish(topic string, env domain.Envelope) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for pattern, hs := range m.handlers {
		if topicMatches(pattern, topic) {
			for _, h := range hs {
				// Fire handlers synchronously-in-goroutine so a slow/broken
				// subscriber can never block the publisher (bulkhead at the
				// transport level, complements internal/resilience).
				h := h
				env := env
				go func() { _ = h(env) }()
			}
		}
	}
	return nil
}

func (m *Memory) Subscribe(topic string, h Handler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[topic] = append(m.handlers[topic], h)
	return nil
}

func (m *Memory) Close() error { return nil }

// topicMatches is shared with mqtt.go — see wildcard.go.
