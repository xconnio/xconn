// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type UnregisterFields interface {
	RequestID() uint64
	RegistrationID() uint64
}

type unregisterFields struct {
	requestID      uint64
	registrationID uint64
}

func (ur *unregisterFields) RequestID() uint64 {
	return ur.requestID
}

func (ur *unregisterFields) RegistrationID() uint64 {
	return ur.registrationID
}

type Unregister struct {
	UnregisterFields
}

func NewUnregister(requestID, registrationID uint64) *Unregister {
	return &Unregister{UnregisterFields: &unregisterFields{requestID: requestID, registrationID: registrationID}}
}

func NewUnregisterWithFields(fields UnregisterFields) *Unregister {
	return &Unregister{UnregisterFields: fields}
}

func (ur *Unregister) Type() uint64 {
	return MessageTypeUnregister
}

func (m *Unregister) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeUnregister, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Unregister) load(fields map[string]any) {
	m.UnregisterFields = &unregisterFields{
		requestID:      bridge.Uint(fields["request_id"]),
		registrationID: bridge.Uint(fields["registration_id"]),
	}
}

func (m *Unregister) RustFields() (uint64, map[string]any) {
	return MessageTypeUnregister, map[string]any{"request_id": m.RequestID(), "registration_id": m.RegistrationID()}
}

func (m *Unregister) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
