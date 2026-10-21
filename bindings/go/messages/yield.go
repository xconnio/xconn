// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type YieldFields interface {
	RequestID() uint64
	Options() map[string]any
	Args() []any
	KwArgs() map[string]any

	BinaryPayload
}

type yieldFields struct {
	requestID uint64
	options   map[string]any
	args      []any
	kwArgs    map[string]any

	binary     bool
	serializer uint64
	payload    []byte
}

func (e *yieldFields) RequestID() uint64 {
	return e.requestID
}

func (e *yieldFields) Options() map[string]any {
	return e.options
}

func (e *yieldFields) Args() []any {
	return e.args
}

func (e *yieldFields) KwArgs() map[string]any {
	return e.kwArgs
}

func (e *yieldFields) PayloadIsBinary() bool {
	return e.binary
}

func (e *yieldFields) Payload() []byte {
	return e.payload
}

func (e *yieldFields) PayloadSerializer() uint64 {
	return e.serializer
}

type Yield struct {
	YieldFields
}

func NewYield(requestID uint64, options map[string]any, args []any, kwArgs map[string]any) *Yield {
	if options == nil {
		options = make(map[string]any)
	}

	return &Yield{YieldFields: &yieldFields{requestID: requestID, options: options, args: args, kwArgs: kwArgs}}
}

func NewYieldWithFields(fields YieldFields) *Yield {
	return &Yield{YieldFields: fields}
}

func NewYieldBinary(requestID uint64, options map[string]any, payload []byte, serializer uint64) *Yield {
	if options == nil {
		options = make(map[string]any)
	}

	options["x_payload_serializer"] = serializer

	return &Yield{YieldFields: &yieldFields{
		requestID:  requestID,
		options:    options,
		binary:     true,
		serializer: serializer,
		payload:    payload,
	}}
}

func (e *Yield) Type() uint64 {
	return MessageTypeYield
}

func (m *Yield) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeYield, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Yield) load(fields map[string]any) {
	m.YieldFields = &yieldFields{
		requestID:  bridge.Uint(fields["request_id"]),
		options:    bridge.Dict(fields["options"]),
		binary:     bridge.Bool(fields["_binary"]),
		serializer: bridge.Uint(fields["_serializer"]),
		payload:    bridge.Bytes(fields["_payload"]),
		args:       bridge.Slice(fields["args"]),
		kwArgs:     bridge.Dict(fields["kwargs"]),
	}
}

func (m *Yield) RustFields() (uint64, map[string]any) {
	return MessageTypeYield, map[string]any{
		"request_id": m.RequestID(),
		"options":    m.Options(),
		"args":       m.Args(),
		"kwargs":     m.KwArgs(),
	}
}

func (m *Yield) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
