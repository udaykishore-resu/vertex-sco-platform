package eventbus

// A minimal, dependency-free MQTT 3.1.1 wire codec. Only the packet types
// Vertex services actually need are implemented (CONNECT, CONNACK, PUBLISH,
// PUBACK, SUBSCRIBE, SUBACK, PINGREQ, PINGRESP, DISCONNECT). This trades
// protocol completeness (no QoS2, no retained-message edge cases) for zero
// supply-chain risk and a tiny static binary — a deliberate call-out in
// docs/ARCHITECTURE.md.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

const (
	pktCONNECT     = 1
	pktCONNACK     = 2
	pktPUBLISH     = 3
	pktPUBACK      = 4
	pktSUBSCRIBE   = 8
	pktSUBACK      = 9
	pktUNSUBSCRIBE = 10
	pktUNSUBACK    = 11
	pktPINGREQ     = 12
	pktPINGRESP    = 13
	pktDISCONNECT  = 14
)

func writeString(buf *[]byte, s string) {
	l := len(s)
	*buf = append(*buf, byte(l>>8), byte(l))
	*buf = append(*buf, s...)
}

func readString(r *bufio.Reader) (string, error) {
	var lb [2]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return "", err
	}
	l := binary.BigEndian.Uint16(lb[:])
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func encodeRemainingLength(n int) []byte {
	var out []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			break
		}
	}
	return out
}

func decodeRemainingLength(r *bufio.Reader) (int, error) {
	multiplier := 1
	value := 0
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(b&0x7f) * multiplier
		if b&0x80 == 0 {
			break
		}
		multiplier *= 128
		if multiplier > 128*128*128 {
			return 0, errors.New("mqtt: malformed remaining length")
		}
	}
	return value, nil
}

// rawPacket is a decoded fixed-header + raw body, cheap to route on before
// doing type-specific parsing.
type rawPacket struct {
	kind  byte
	flags byte
	body  []byte
}

func readPacket(r *bufio.Reader) (*rawPacket, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	remLen, err := decodeRemainingLength(r)
	if err != nil {
		return nil, err
	}
	body := make([]byte, remLen)
	if remLen > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
	}
	return &rawPacket{kind: first >> 4, flags: first & 0x0f, body: body}, nil
}

func writePacket(w io.Writer, kind byte, flags byte, body []byte) error {
	header := []byte{(kind << 4) | flags}
	header = append(header, encodeRemainingLength(len(body))...)
	_, err := w.Write(append(header, body...))
	return err
}

// buildConnect constructs a CONNECT packet. cleanSession=false requests a
// persistent session from the broker, so subscriptions and any QoS1
// messages queued for this client ID survive a brief disconnect — this is
// the client-side half of the "persistent sessions" fix from the
// architecture review (the broker/cluster side is configured in
// deploy/docker-compose.yml).
func buildConnect(clientID string, keepAliveSec uint16, cleanSession bool) []byte {
	var body []byte
	writeString(&body, "MQTT")
	body = append(body, 4) // protocol level 3.1.1
	var flags byte
	if cleanSession {
		flags |= 0x02
	}
	body = append(body, flags)
	body = append(body, byte(keepAliveSec>>8), byte(keepAliveSec))
	writeString(&body, clientID)
	return body
}

func buildPublish(packetID uint16, topic string, payload []byte, qos byte, dup bool) (flags byte, body []byte) {
	flags = qos << 1
	if dup {
		flags |= 0x08
	}
	writeString(&body, topic)
	if qos > 0 {
		body = append(body, byte(packetID>>8), byte(packetID))
	}
	body = append(body, payload...)
	return
}

func buildPuback(packetID uint16) []byte {
	return []byte{byte(packetID >> 8), byte(packetID)}
}

func buildSubscribe(packetID uint16, topic string, qos byte) []byte {
	var body []byte
	body = append(body, byte(packetID>>8), byte(packetID))
	writeString(&body, topic)
	body = append(body, qos)
	return body
}

func buildPingreq() []byte { return nil }
