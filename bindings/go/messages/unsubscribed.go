// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type UnsubscribedFields interface {
	RequestID() uint64
}

type unsubscribedFields struct {
	requestID uint64
}

func (us *unsubscribedFields) RequestID() uint64 {
	return us.requestID
}

type Unsubscribed struct {
	UnsubscribedFields
}

func NewUnsubscribed(requestID uint64) *Unsubscribed {
	return &Unsubscribed{UnsubscribedFields: &unsubscribedFields{requestID: requestID}}
}

func NewUnsubscribedWithFields(fields UnsubscribedFields) *Unsubscribed {
	return &Unsubscribed{UnsubscribedFields: fields}
}

func (us *Unsubscribed) Type() uint64 {
	return MessageTypeUnsubscribed
}

func (m *Unsubscribed) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeUnsubscribed, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Unsubscribed) load(fields map[string]any) {
	m.UnsubscribedFields = &unsubscribedFields{requestID: bridge.Uint(fields["request_id"])}
}

func (m *Unsubscribed) RustFields() (uint64, map[string]any) {
	return MessageTypeUnsubscribed, map[string]any{"request_id": m.RequestID()}
}

func (m *Unsubscribed) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
