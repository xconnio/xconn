// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type SubscribeFields interface {
	RequestID() uint64
	Options() map[string]any
	Topic() string
}

type subscribeFields struct {
	requestID uint64
	options   map[string]any
	topic     string
}

func (s *subscribeFields) RequestID() uint64 {
	return s.requestID
}

func (s *subscribeFields) Options() map[string]any {
	return s.options
}

func (s *subscribeFields) Topic() string {
	return s.topic
}

type Subscribe struct {
	SubscribeFields
}

func NewSubscribe(requestID uint64, options map[string]any, uri string) *Subscribe {
	if options == nil {
		options = make(map[string]any)
	}

	return &Subscribe{SubscribeFields: &subscribeFields{requestID: requestID, options: options, topic: uri}}
}

func NewSubscribeWithFields(fields SubscribeFields) *Subscribe {
	return &Subscribe{SubscribeFields: fields}
}

func (s *Subscribe) Type() uint64 {
	return MessageTypeSubscribe
}

func (m *Subscribe) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeSubscribe, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Subscribe) load(fields map[string]any) {
	m.SubscribeFields = &subscribeFields{
		requestID: bridge.Uint(fields["request_id"]),
		options:   bridge.Dict(fields["options"]),
		topic:     bridge.String(fields["topic"]),
	}
}

func (m *Subscribe) RustFields() (uint64, map[string]any) {
	return MessageTypeSubscribe, map[string]any{"request_id": m.RequestID(), "options": m.Options(), "topic": m.Topic()}
}

func (m *Subscribe) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
