// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type PublishFields interface {
	RequestID() uint64
	Options() map[string]any
	Topic() string
	Args() []any
	KwArgs() map[string]any

	BinaryPayload
}

type publishFields struct {
	requestID uint64
	options   map[string]any
	topic     string
	args      []any
	kwArgs    map[string]any
}

func (e *publishFields) RequestID() uint64 {
	return e.requestID
}

func (e *publishFields) Options() map[string]any {
	return e.options
}

func (e *publishFields) Topic() string {
	return e.topic
}

func (e *publishFields) Args() []any {
	return e.args
}

func (e *publishFields) KwArgs() map[string]any {
	return e.kwArgs
}

func (e *publishFields) PayloadIsBinary() bool {
	return false
}

func (e *publishFields) Payload() []byte {
	return nil
}

func (e *publishFields) PayloadSerializer() uint64 {
	return 0
}

type Publish struct {
	PublishFields
}

func NewPublish(requestID uint64, option map[string]any, uri string, args []any, kwArgs map[string]any) *Publish {
	if option == nil {
		option = make(map[string]any)
	}

	return &Publish{PublishFields: &publishFields{
		requestID: requestID,
		options:   option,
		topic:     uri,
		args:      args,
		kwArgs:    kwArgs,
	}}
}

func NewPublishWithFields(fields PublishFields) *Publish { return &Publish{PublishFields: fields} }

func (e *Publish) Type() uint64 {
	return MessageTypePublish
}

func (m *Publish) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypePublish, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Publish) load(fields map[string]any) {
	m.PublishFields = &publishFields{
		requestID: bridge.Uint(fields["request_id"]),
		options:   bridge.Dict(fields["options"]),
		topic:     bridge.String(fields["topic"]),
		args:      bridge.Slice(fields["args"]),
		kwArgs:    bridge.Dict(fields["kwargs"]),
	}
}

func (m *Publish) RustFields() (uint64, map[string]any) {
	return MessageTypePublish, map[string]any{
		"request_id": m.RequestID(),
		"options":    m.Options(),
		"topic":      m.Topic(),
		"args":       m.Args(),
		"kwargs":     m.KwArgs(),
	}
}

func (m *Publish) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
