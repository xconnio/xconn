// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type RegisteredFields interface {
	RequestID() uint64
	RegistrationID() uint64
}

type registeredFields struct {
	requestID      uint64
	registrationID uint64
}

func (r *registeredFields) RequestID() uint64 {
	return r.requestID
}

func (r *registeredFields) RegistrationID() uint64 {
	return r.registrationID
}

type Registered struct {
	RegisteredFields
}

func NewRegistered(requestID, registrationID uint64) *Registered {
	return &Registered{RegisteredFields: &registeredFields{requestID: requestID, registrationID: registrationID}}
}

func NewRegisteredWithFields(fields RegisteredFields) *Registered {
	return &Registered{RegisteredFields: fields}
}

func (r *Registered) Type() uint64 {
	return MessageTypeRegistered
}

func (m *Registered) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeRegistered, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Registered) load(fields map[string]any) {
	m.RegisteredFields = &registeredFields{
		requestID:      bridge.Uint(fields["request_id"]),
		registrationID: bridge.Uint(fields["registration_id"]),
	}
}

func (m *Registered) RustFields() (uint64, map[string]any) {
	return MessageTypeRegistered, map[string]any{"request_id": m.RequestID(), "registration_id": m.RegistrationID()}
}

func (m *Registered) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
