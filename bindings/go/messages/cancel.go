// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type CancelFields interface {
	RequestID() uint64
	Options() map[string]any
}

type cancelFields struct {
	requestID uint64
	options   map[string]any
}

func (c *cancelFields) RequestID() uint64 {
	return c.requestID
}

func (c *cancelFields) Options() map[string]any {
	return c.options
}

type Cancel struct {
	CancelFields
}

func NewCancel(requestID uint64, options map[string]any) *Cancel {
	if options == nil {
		options = make(map[string]any)
	}

	return &Cancel{CancelFields: &cancelFields{requestID: requestID, options: options}}
}

func NewCancelWithFields(fields CancelFields) *Cancel {
	return &Cancel{CancelFields: fields}
}

func (c *Cancel) Type() uint64 {
	return MessageTypeCancel
}

func (m *Cancel) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeCancel, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Cancel) load(fields map[string]any) {
	m.CancelFields = &cancelFields{
		requestID: bridge.Uint(fields["request_id"]),
		options:   bridge.Dict(fields["options"]),
	}
}

func (m *Cancel) RustFields() (uint64, map[string]any) {
	return MessageTypeCancel, map[string]any{"request_id": m.RequestID(), "options": m.Options()}
}

func (m *Cancel) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
