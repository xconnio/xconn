// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type AuthenticateFields interface {
	Signature() string
	Extra() map[string]any
}

type authenticateFields struct {
	signature string
	extra     map[string]any
}

func (a *authenticateFields) Signature() string {
	return a.signature
}

func (a *authenticateFields) Extra() map[string]any {
	return a.extra
}

type Authenticate struct {
	AuthenticateFields
}

func NewAuthenticateWithFields(fields AuthenticateFields) *Authenticate {
	return &Authenticate{AuthenticateFields: fields}
}

func NewAuthenticate(signature string, extra map[string]any) *Authenticate {
	return &Authenticate{AuthenticateFields: &authenticateFields{signature: signature, extra: extra}}
}

func (a *Authenticate) Type() uint64 {
	return MessageTypeAuthenticate
}

func (m *Authenticate) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeAuthenticate, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Authenticate) load(fields map[string]any) {
	m.AuthenticateFields = &authenticateFields{
		signature: bridge.String(fields["signature"]),
		extra:     bridge.Dict(fields["extra"]),
	}
}

func (m *Authenticate) RustFields() (uint64, map[string]any) {
	return MessageTypeAuthenticate, map[string]any{"signature": m.Signature(), "extra": m.Extra()}
}

func (m *Authenticate) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
