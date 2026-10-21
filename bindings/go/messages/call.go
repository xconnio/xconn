// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type CallFields interface {
	RequestID() uint64
	Options() map[string]any
	Procedure() string
	Args() []any
	KwArgs() map[string]any

	BinaryPayload
}

type callFields struct {
	requestID uint64
	options   map[string]any
	procedure string
	args      []any
	kwArgs    map[string]any

	binary     bool
	serializer uint64
	payload    []byte
}

func (e *callFields) RequestID() uint64 {
	return e.requestID
}

func (e *callFields) Options() map[string]any {
	return e.options
}

func (e *callFields) Procedure() string {
	return e.procedure
}

func (e *callFields) Args() []any {
	return e.args
}

func (e *callFields) KwArgs() map[string]any {
	return e.kwArgs
}

func (e *callFields) PayloadIsBinary() bool {
	return e.binary
}

func (e *callFields) Payload() []byte {
	return e.payload
}

func (e *callFields) PayloadSerializer() uint64 {
	return e.serializer
}

type Call struct {
	CallFields
}

func NewCall(requestID uint64, options map[string]any, procedure string, args []any, kwArgs map[string]any) *Call {
	if options == nil {
		options = make(map[string]any)
	}

	return &Call{CallFields: &callFields{
		requestID: requestID,
		options:   options,
		procedure: procedure,
		args:      args,
		kwArgs:    kwArgs,
	}}
}

func NewCallWithFields(fields CallFields) *Call { return &Call{CallFields: fields} }

func NewCallBinary(requestID uint64, options map[string]any, procedure string, payload []byte,
	serializer uint64) *Call {
	if options == nil {
		options = make(map[string]any)
	}

	updated := bridge.Dict(bridge.Must("binary_metadata", options, serializer))

	for key, value := range updated {
		options[key] = value
	}

	return &Call{CallFields: &callFields{
		requestID:  requestID,
		options:    options,
		procedure:  procedure,
		binary:     true,
		payload:    payload,
		serializer: serializer,
	}}
}

func (e *Call) Type() uint64 {
	return MessageTypeCall
}

func (m *Call) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeCall, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Call) load(fields map[string]any) {
	m.CallFields = &callFields{
		requestID:  bridge.Uint(fields["request_id"]),
		options:    bridge.Dict(fields["options"]),
		procedure:  bridge.String(fields["procedure"]),
		binary:     bridge.Bool(fields["_binary"]),
		serializer: bridge.Uint(fields["_serializer"]),
		payload:    bridge.Bytes(fields["_payload"]),
		args:       bridge.Slice(fields["args"]),
		kwArgs:     bridge.Dict(fields["kwargs"]),
	}
}

func (m *Call) RustFields() (uint64, map[string]any) {
	return MessageTypeCall, map[string]any{
		"request_id": m.RequestID(),
		"options":    m.Options(),
		"procedure":  m.Procedure(),
		"args":       m.Args(),
		"kwargs":     m.KwArgs(),
	}
}

func (m *Call) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
