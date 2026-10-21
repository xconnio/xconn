// Package serializers delegates all WAMP encoding/decoding to the Rust engine.
package serializers

import (
	"github.com/xconnio/xconn-core/bindings/go/internal/bridge"
	"github.com/xconnio/xconn-core/bindings/go/messages"
)

const (
	NoneSerializerID    = 0
	JSONSerializerID    = 1
	MsgPackSerializerID = 2
	CBORSerializerID    = 3
)

type Serializer interface {
	Serialize(messages.Message) ([]byte, error)
	Deserialize([]byte) (messages.Message, error)
	Static() bool
}

type JSONSerializer struct{}
type MsgPackSerializer struct{}
type CBORSerializer struct{}

func encode(codec uint64, message messages.Message) ([]byte, error) {
	var value any
	var err error

	if payload, ok := message.(messages.BinaryPayload); ok && payload.PayloadIsBinary() {
		value, err = bridge.Call("encode_packet", codec, messages.NativePacket(message))
	} else if fields, ok := message.(interface {
		RustFields() (uint64, map[string]any)
	}); ok {
		code, data := fields.RustFields()
		value, err = bridge.Call("encode_message", codec, code, data)
	} else {
		value, err = bridge.Call("encode_wire", codec, message.Marshal())
	}

	if err != nil {
		return nil, err
	}

	return bridge.Bytes(value), nil
}

func decode(codec uint64, data []byte) (messages.Message, error) {
	value, err := bridge.Call("decode_message", codec, data)
	if err != nil {
		return nil, err
	}

	result := bridge.Slice(value)
	return messages.FromFields(bridge.String(result[0]), bridge.Dict(result[1]))
}

func (*JSONSerializer) Serialize(m messages.Message) ([]byte, error) { return encode(1, m) }

func (*JSONSerializer) Deserialize(data []byte) (messages.Message, error) { return decode(1, data) }

func (*JSONSerializer) Static() bool { return false }

func (*MsgPackSerializer) Serialize(m messages.Message) ([]byte, error) { return encode(2, m) }

func (*MsgPackSerializer) Deserialize(data []byte) (messages.Message, error) { return decode(2, data) }

func (*MsgPackSerializer) Static() bool { return false }

func (*CBORSerializer) Serialize(m messages.Message) ([]byte, error) { return encode(3, m) }

func (*CBORSerializer) Deserialize(data []byte) (messages.Message, error) { return decode(3, data) }

func (*CBORSerializer) Static() bool { return false }

func ToMessage(wire []any) (messages.Message, error) { return messages.NewForWire(wire) }
