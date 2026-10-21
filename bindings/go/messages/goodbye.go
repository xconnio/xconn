// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type GoodByeFields interface {
	Details() map[string]any
	Reason() string
}

type goodByeFields struct {
	details map[string]any
	reason  string
}

func (g *goodByeFields) Reason() string {
	return g.reason
}

func (g *goodByeFields) Details() map[string]any {
	return g.details
}

type GoodBye struct {
	GoodByeFields
}

func NewGoodByeWithFields(fields GoodByeFields) *GoodBye { return &GoodBye{GoodByeFields: fields} }

func NewGoodBye(reason string, details map[string]any) *GoodBye {
	if details == nil {
		details = map[string]any{}
	}

	return &GoodBye{GoodByeFields: &goodByeFields{reason: reason, details: details}}
}

func (g *GoodBye) Type() uint64 {
	return MessageTypeGoodbye
}

func (m *GoodBye) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeGoodbye, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *GoodBye) load(fields map[string]any) {
	m.GoodByeFields = &goodByeFields{details: bridge.Dict(fields["details"]), reason: bridge.String(fields["reason"])}
}

func (m *GoodBye) RustFields() (uint64, map[string]any) {
	return MessageTypeGoodbye, map[string]any{"details": m.Details(), "reason": m.Reason()}
}

func (m *GoodBye) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
