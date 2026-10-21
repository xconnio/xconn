// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type InterruptFields interface {
	RequestID() uint64
	Options() map[string]any
}

type interruptFields struct {
	requestID uint64
	options   map[string]any
}

func (c *interruptFields) RequestID() uint64 {
	return c.requestID
}

func (c *interruptFields) Options() map[string]any {
	return c.options
}

type Interrupt struct {
	InterruptFields
}

func NewInterruptWithFields(fields InterruptFields) *Interrupt {
	return &Interrupt{InterruptFields: fields}
}

func NewInterrupt(requestID uint64, options map[string]any) *Interrupt {
	if options == nil {
		options = make(map[string]any)
	}

	return &Interrupt{InterruptFields: &interruptFields{requestID: requestID, options: options}}
}

func (c *Interrupt) Type() uint64 {
	return MessageTypeInterrupt
}

func (m *Interrupt) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeInterrupt, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Interrupt) load(fields map[string]any) {
	m.InterruptFields = &interruptFields{
		requestID: bridge.Uint(fields["request_id"]),
		options:   bridge.Dict(fields["options"]),
	}
}

func (m *Interrupt) RustFields() (uint64, map[string]any) {
	return MessageTypeInterrupt, map[string]any{"request_id": m.RequestID(), "options": m.Options()}
}

func (m *Interrupt) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
