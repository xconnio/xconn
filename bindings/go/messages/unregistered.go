// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type UnregisteredFields interface {
	RequestID() uint64
}

type unregisteredFields struct {
	requestID uint64
}

func (ur *unregisteredFields) RequestID() uint64 {
	return ur.requestID
}

type Unregistered struct {
	UnregisteredFields
}

func NewUnregistered(requestID uint64) *Unregistered {
	return &Unregistered{UnregisteredFields: &unregisteredFields{requestID: requestID}}
}

func NewUnregisteredWithFields(fields UnregisteredFields) *Unregistered {
	return &Unregistered{UnregisteredFields: fields}
}

func (ur *Unregistered) Type() uint64 {
	return MessageTypeUnregistered
}

func (m *Unregistered) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeUnregistered, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Unregistered) load(fields map[string]any) {
	m.UnregisteredFields = &unregisteredFields{requestID: bridge.Uint(fields["request_id"])}
}

func (m *Unregistered) RustFields() (uint64, map[string]any) {
	return MessageTypeUnregistered, map[string]any{"request_id": m.RequestID()}
}

func (m *Unregistered) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
