package eventbus

// A bare-bones MQTT broker used only by TestMQTTClientAgainstRealBroker to
// prove the hand-rolled wire codec (mqtt_wire.go) round-trips real TCP
// bytes correctly, not just that it compiles. It implements just enough of
// the protocol to CONNECT, accept SUBSCRIBE, and fan out PUBLISH to
// matching subscribers with a PUBACK — it is not meant to be a real broker
// (no persistence, no auth, single-node); deploy/docker-compose.yml uses a
// real clustered EMQX broker for actual multi-service runs.

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
)

type testBroker struct {
	ln   net.Listener
	subs sync.Map // topic filter -> []net.Conn (simplified: exact match only, sufficient for this test)
}

func startTestBroker(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	b := &testBroker{ln: ln}
	go b.acceptLoop()
	return ln.Addr().String(), func() { ln.Close() }
}

func (b *testBroker) acceptLoop() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.handleConn(conn)
	}
}

func (b *testBroker) handleConn(conn net.Conn) {
	r := bufio.NewReader(conn)
	// CONNECT
	pkt, err := readPacket(r)
	if err != nil || pkt.kind != pktCONNECT {
		conn.Close()
		return
	}
	_ = writePacket(conn, pktCONNACK, 0, []byte{0, 0})

	for {
		pkt, err := readPacket(r)
		if err != nil {
			return
		}
		switch pkt.kind {
		case pktSUBSCRIBE:
			// body: packetID(2) + topic(len-prefixed) + qos(1)
			id := uint16(pkt.body[0])<<8 | uint16(pkt.body[1])
			tlen := int(pkt.body[2])<<8 | int(pkt.body[3])
			topic := string(pkt.body[4 : 4+tlen])
			var subs []net.Conn
			if v, ok := b.subs.Load(topic); ok {
				subs = v.([]net.Conn)
			}
			b.subs.Store(topic, append(subs, conn))
			// SUBACK: packetID(2) + return code(1)
			_ = writePacket(conn, pktSUBACK, 0, []byte{byte(id >> 8), byte(id), 0})
		case pktPUBLISH:
			r2 := bufio.NewReader(byteReader(pkt.body))
			topic, _ := readString(r2)
			qos := (pkt.flags >> 1) & 0x03
			rest := pkt.body[2+len(topic):]
			var id uint16
			if qos > 0 {
				id = uint16(rest[0])<<8 | uint16(rest[1])
				rest = rest[2:]
				_ = writePacket(conn, pktPUBACK, 0, buildPuback(id))
			}
			if v, ok := b.subs.Load(topic); ok {
				for _, sub := range v.([]net.Conn) {
					fwdID := uint16(1)
					flags, body := buildPublish(fwdID, topic, rest, qos, false)
					_ = writePacket(sub, pktPUBLISH, flags, body)
				}
			}
		case pktPINGREQ:
			_ = writePacket(conn, pktPINGRESP, 0, nil)
		case pktDISCONNECT:
			conn.Close()
			return
		}
	}
}

func TestMQTTClientAgainstRealBroker(t *testing.T) {
	addr, stop := startTestBroker(t)
	defer stop()

	sub := NewMQTT(addr, "test-subscriber")
	defer sub.Close()
	pub := NewMQTT(addr, "test-publisher")
	defer pub.Close()

	received := make(chan domain.Envelope, 1)
	if err := sub.Subscribe("vertex/store-1/lane-1/test.event", func(env domain.Envelope) error {
		received <- env
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// give both clients time to finish their async CONNECT
	time.Sleep(300 * time.Millisecond)

	env := domain.Envelope{ID: "evt-1", Type: "test.event", Source: "test", LaneID: "lane-1", StoreID: "store-1"}
	if err := pub.Publish("vertex/store-1/lane-1/test.event", env); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-received:
		if got.ID != "evt-1" {
			t.Fatalf("got wrong envelope: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message to round-trip through the test broker")
	}
}
