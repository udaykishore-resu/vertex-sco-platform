package eventbus

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
)

// MQTT is a Bus implementation backed by the codec in mqtt_wire.go. It is
// built to survive a broker failover in a clustered deployment
// (deploy/docker-compose.yml runs 3 EMQX nodes behind HAProxy on
// mqtt.vertex.local:1883):
//
//   - CleanSession=false: the broker retains this client's subscriptions and
//     queued QoS1 messages across a brief disconnect.
//   - QoS1 publish blocks for PUBACK (bounded by PublishTimeout) and retries
//     once with the DUP flag set, so a message published during a broker
//     failover is not silently lost.
//   - A background loop reconnects with exponential backoff and replays all
//     active subscriptions once the session is re-established.
type MQTT struct {
	Addr           string // e.g. "mqtt.vertex.local:1883" (HAProxy VIP in front of the EMQX cluster)
	ClientID       string
	KeepAlive      time.Duration
	PublishTimeout time.Duration

	mu        sync.Mutex
	conn      net.Conn
	reader    *bufio.Reader
	connected atomic.Bool
	nextID    uint32

	subsMu sync.RWMutex
	subs   map[string]Handler // topic pattern -> handler

	pendingMu sync.Mutex
	pending   map[uint16]chan struct{}

	closeCh chan struct{}
	closed  atomic.Bool
}

func NewMQTT(addr, clientID string) *MQTT {
	m := &MQTT{
		Addr:           addr,
		ClientID:       clientID,
		KeepAlive:      30 * time.Second,
		PublishTimeout: 5 * time.Second,
		subs:           make(map[string]Handler),
		pending:        make(map[uint16]chan struct{}),
		closeCh:        make(chan struct{}),
	}
	go m.connectionLoop()
	return m
}

func (m *MQTT) connectionLoop() {
	backoff := 250 * time.Millisecond
	const maxBackoff = 10 * time.Second
	for {
		select {
		case <-m.closeCh:
			return
		default:
		}
		if err := m.connectOnce(); err != nil {
			log.Printf("[eventbus/mqtt] connect %s failed: %v (retry in %s)", m.Addr, err, backoff)
			select {
			case <-time.After(backoff):
			case <-m.closeCh:
				return
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		backoff = 250 * time.Millisecond
		m.readLoop() // blocks until the connection drops
		m.connected.Store(false)
	}
}

func (m *MQTT) connectOnce() error {
	conn, err := net.DialTimeout("tcp", m.Addr, 5*time.Second)
	if err != nil {
		return err
	}
	body := buildConnect(m.ClientID, uint16(m.KeepAlive/time.Second), false /* persistent session */)
	if err := writePacket(conn, pktCONNECT, 0, body); err != nil {
		conn.Close()
		return err
	}
	reader := bufio.NewReader(conn)
	pkt, err := readPacket(reader)
	if err != nil {
		conn.Close()
		return err
	}
	if pkt.kind != pktCONNACK || len(pkt.body) < 2 || pkt.body[1] != 0 {
		conn.Close()
		return errors.New("mqtt: connect refused")
	}

	m.mu.Lock()
	m.conn = conn
	m.reader = reader
	m.mu.Unlock()
	m.connected.Store(true)

	// Re-establish every active subscription — required because even with a
	// persistent session, resubscribing defensively avoids relying on the
	// broker cluster's session-replication being perfectly consistent
	// immediately after a failover.
	m.subsMu.RLock()
	for topic := range m.subs {
		_ = m.sendSubscribe(topic)
	}
	m.subsMu.RUnlock()

	go m.pingLoop(conn)
	log.Printf("[eventbus/mqtt] connected to %s as %s", m.Addr, m.ClientID)
	return nil
}

func (m *MQTT) pingLoop(conn net.Conn) {
	t := time.NewTicker(m.KeepAlive / 2)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.mu.Lock()
			active := m.conn == conn
			if active {
				_ = writePacket(conn, pktPINGREQ, 0, nil)
			}
			m.mu.Unlock()
			if !active {
				return
			}
		case <-m.closeCh:
			return
		}
	}
}

func (m *MQTT) readLoop() {
	for {
		m.mu.Lock()
		reader := m.reader
		m.mu.Unlock()
		if reader == nil {
			return
		}
		pkt, err := readPacket(reader)
		if err != nil {
			return // connectionLoop will redial
		}
		switch pkt.kind {
		case pktPUBLISH:
			m.handlePublish(pkt)
		case pktPUBACK:
			if len(pkt.body) >= 2 {
				id := uint16(pkt.body[0])<<8 | uint16(pkt.body[1])
				m.pendingMu.Lock()
				if ch, ok := m.pending[id]; ok {
					close(ch)
					delete(m.pending, id)
				}
				m.pendingMu.Unlock()
			}
		case pktPINGRESP, pktSUBACK:
			// no-op
		}
	}
}

func (m *MQTT) handlePublish(pkt *rawPacket) {
	r := bufio.NewReader(byteReader(pkt.body))
	topic, err := readString(r)
	if err != nil {
		return
	}
	qos := (pkt.flags >> 1) & 0x03
	rest := pkt.body[2+len(topic):]
	if qos > 0 {
		if len(rest) < 2 {
			return
		}
		id := uint16(rest[0])<<8 | uint16(rest[1])
		rest = rest[2:]
		// PUBACK immediately (at-least-once, handlers run async below).
		m.mu.Lock()
		if m.conn != nil {
			_ = writePacket(m.conn, pktPUBACK, 0, buildPuback(id))
		}
		m.mu.Unlock()
	}
	var env domain.Envelope
	if err := json.Unmarshal(rest, &env); err != nil {
		log.Printf("[eventbus/mqtt] bad envelope on %s: %v", topic, err)
		return
	}
	m.subsMu.RLock()
	defer m.subsMu.RUnlock()
	for pattern, h := range m.subs {
		if topicMatches(pattern, topic) {
			h := h
			env := env
			go func() {
				if err := h(env); err != nil {
					log.Printf("[eventbus/mqtt] handler error on %s: %v", topic, err)
				}
			}()
		}
	}
}

func (m *MQTT) Publish(topic string, env domain.Envelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	id := uint16(atomic.AddUint32(&m.nextID, 1))
	ackCh := make(chan struct{})
	m.pendingMu.Lock()
	m.pending[id] = ackCh
	m.pendingMu.Unlock()

	send := func(dup bool) error {
		flags, body := buildPublish(id, topic, payload, 1 /* QoS1 */, dup)
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.conn == nil {
			return errors.New("mqtt: not connected")
		}
		return writePacket(m.conn, pktPUBLISH, flags, body)
	}

	if err := send(false); err != nil {
		return err
	}
	select {
	case <-ackCh:
		return nil
	case <-time.After(m.PublishTimeout):
		// one retry with DUP set before giving up — covers a broker
		// failover happening mid-publish.
		_ = send(true)
		select {
		case <-ackCh:
			return nil
		case <-time.After(m.PublishTimeout):
			return errors.New("mqtt: publish timed out waiting for PUBACK")
		}
	}
}

func (m *MQTT) Subscribe(topic string, h Handler) error {
	m.subsMu.Lock()
	m.subs[topic] = h
	m.subsMu.Unlock()
	if m.connected.Load() {
		return m.sendSubscribe(topic)
	}
	return nil // will be sent once connectOnce succeeds
}

func (m *MQTT) sendSubscribe(topic string) error {
	id := uint16(atomic.AddUint32(&m.nextID, 1))
	body := buildSubscribe(id, topic, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn == nil {
		return errors.New("mqtt: not connected")
	}
	return writePacket(m.conn, pktSUBSCRIBE, 2, body)
}

func (m *MQTT) Close() error {
	if m.closed.CompareAndSwap(false, true) {
		close(m.closeCh)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		_ = writePacket(m.conn, pktDISCONNECT, 0, nil)
		return m.conn.Close()
	}
	return nil
}

// byteReader adapts a []byte to io.Reader without pulling in bytes.Reader
// just for this one call site's readability at the call sites above.
func byteReader(b []byte) *sliceReader { return &sliceReader{b: b} }

type sliceReader struct {
	b   []byte
	pos int
}

func (s *sliceReader) Read(p []byte) (int, error) {
	if s.pos >= len(s.b) {
		return 0, io.EOF
	}
	n := copy(p, s.b[s.pos:])
	s.pos += n
	return n, nil
}
