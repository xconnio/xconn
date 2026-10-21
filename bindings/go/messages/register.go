// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type RegisterFields interface {
	RequestID() uint64
	Options() map[string]any
	Procedure() string
}

type registerFields struct {
	requestID uint64
	options   map[string]any
	procedure string
}

func (r *registerFields) RequestID() uint64 {
	return r.requestID
}

func (r *registerFields) Options() map[string]any {
	return r.options
}

func (r *registerFields) Procedure() string {
	return r.procedure
}

type Register struct {
	RegisterFields
}

func NewRegister(requestID uint64, options map[string]any, uri string) *Register {
	if options == nil {
		options = make(map[string]any)
	}

	return &Register{RegisterFields: &registerFields{requestID: requestID, options: options, procedure: uri}}
}

func NewRegisterWithFields(fields RegisterFields) *Register {
	return &Register{RegisterFields: fields}
}

func (r *Register) Type() uint64 {
	return MessageTypeRegister
}

func (m *Register) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeRegister, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Register) load(fields map[string]any) {
	m.RegisterFields = &registerFields{
		requestID: bridge.Uint(fields["request_id"]),
		options:   bridge.Dict(fields["options"]),
		procedure: bridge.String(fields["procedure"]),
	}
}

func (m *Register) RustFields() (uint64, map[string]any) {
	return MessageTypeRegister, map[string]any{
		"request_id": m.RequestID(),
		"options":    m.Options(),
		"procedure":  m.Procedure(),
	}
}

func (m *Register) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
