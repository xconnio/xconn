// Package transports provides sans-I/O RawSocket framing through Rust.
package transports

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

const (
	Magic              byte       = 0x7f
	ProtocolMaxMsgSize            = 1 << 24
	DefaultMaxMsgSize             = 1 << 20
	SerializerJSON     Serializer = 1
	SerializerMsgPack  Serializer = 2
	SerializerCBOR     Serializer = 3
	MessageWAMP        Message    = 0
	MessagePing        Message    = 1
	MessagePong        Message    = 2
)

type Serializer int
type Message byte
type Handshake struct {
	serializer     Serializer
	maxMessageSize int
}

func NewHandshake(s Serializer, n int) *Handshake { return &Handshake{s, n} }

func (h *Handshake) Serializer() Serializer { return h.serializer }

func (h *Handshake) MaxMessageSize() int { return h.maxMessageSize }

func SendHandshake(h *Handshake) ([]byte, error) {
	v, e := bridge.Call("handshake_send", int(h.serializer), h.maxMessageSize)
	return bridge.Bytes(v), e
}

func ReceiveHandshake(data []byte) (*Handshake, error) {
	v, e := bridge.Call("handshake_receive", data)
	if e != nil {
		return nil, e
	}

	r := bridge.Slice(v)
	return NewHandshake(Serializer(bridge.Int(r[0])), bridge.Int(r[1])), nil
}

type MessageHeader struct {
	kind   Message
	length int
}

func NewMessageHeader(k Message, n int) *MessageHeader { return &MessageHeader{k, n} }

func (h *MessageHeader) Kind() Message { return h.kind }

func (h *MessageHeader) Length() int { return h.length }

func TrySendMessageHeader(h *MessageHeader) ([]byte, error) {
	v, e := bridge.Call("header_send", int(h.kind), h.length)
	return bridge.Bytes(v), e
}

func SendMessageHeader(h *MessageHeader) []byte {
	v, e := TrySendMessageHeader(h)
	if e != nil {
		panic(e)
	}

	return v
}

func ReceiveMessageHeader(data []byte) (*MessageHeader, error) {
	v, e := bridge.Call("header_receive", data)
	if e != nil {
		return nil, e
	}

	r := bridge.Slice(v)
	return NewMessageHeader(Message(bridge.Uint(r[0])), bridge.Int(r[1])), nil
}

func IntToBytes(n int) []byte { return bridge.Bytes(bridge.Must("length_encode", n)) }

func BytesToInt(data []byte) int { return bridge.Int(bridge.Must("length_decode", data)) }
