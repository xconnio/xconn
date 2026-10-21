// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type WelcomeFields interface {
	SessionID() uint64
	Details() map[string]any
}

type welcomeFields struct {
	sessionID uint64
	details   map[string]any
}

func (w *welcomeFields) SessionID() uint64 {
	return w.sessionID
}

func (w *welcomeFields) Details() map[string]any {
	return w.details
}

type Welcome struct {
	WelcomeFields
}

func NewWelcome(sessionID uint64, details map[string]any) *Welcome {
	return &Welcome{WelcomeFields: &welcomeFields{sessionID: sessionID, details: details}}
}

func NewWelcomeWithFields(fields WelcomeFields) *Welcome { return &Welcome{WelcomeFields: fields} }

func (w *Welcome) Type() uint64 {
	return MessageTypeWelcome
}

func (m *Welcome) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeWelcome, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Welcome) load(fields map[string]any) {
	m.WelcomeFields = &welcomeFields{
		sessionID: bridge.Uint(fields["session_id"]),
		details:   bridge.Dict(fields["details"]),
	}
}

func (m *Welcome) RustFields() (uint64, map[string]any) {
	return MessageTypeWelcome, map[string]any{"session_id": m.SessionID(), "details": m.Details()}
}

func (m *Welcome) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
