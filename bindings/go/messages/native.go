package messages

import (
	"fmt"

	"github.com/xconnio/xconn-core/bindings/go/internal/bridge"
)

func NativePacket(m Message) []any {
	var id, data any

	if p, ok := m.(BinaryPayload); ok && p.PayloadIsBinary() {
		id = p.PayloadSerializer()
		data = p.Payload()
	}

	return []any{m.Marshal(), id, data}
}

func FromNativePacket(v any) (Message, error) {
	p := bridge.Slice(v)
	if len(p) != 3 {
		return nil, fmt.Errorf("invalid native message packet")
	}

	wire := bridge.Slice(p[0])
	m, e := NewForWire(wire)
	if e != nil {
		return nil, e
	}

	fields, e := bridge.Parse(m.Type(), wire)
	if e != nil {
		return nil, e
	}

	if p[1] != nil {
		fields["_binary"] = true
		fields["_serializer"] = p[1]
		fields["_payload"] = p[2]
	}

	m.(nativeMessage).load(fields)
	return m, nil
}
