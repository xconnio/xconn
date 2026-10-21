// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type ResultFields interface {
	RequestID() uint64
	Details() map[string]any
	Args() []any
	KwArgs() map[string]any

	BinaryPayload
}

type resultFields struct {
	requestID uint64
	details   map[string]any
	args      []any
	kwArgs    map[string]any

	binary     bool
	serializer uint64
	payload    []byte
}

func (e *resultFields) RequestID() uint64 {
	return e.requestID
}

func (e *resultFields) Details() map[string]any {
	return e.details
}

func (e *resultFields) Args() []any {
	return e.args
}

func (e *resultFields) KwArgs() map[string]any {
	return e.kwArgs
}

func (e *resultFields) PayloadIsBinary() bool {
	return e.serializer != 0
}

func (e *resultFields) Payload() []byte {
	return e.payload
}

func (e *resultFields) PayloadSerializer() uint64 {
	return e.serializer
}

type Result struct {
	ResultFields
}

func NewResult(requestID uint64, details map[string]any, args []any, kwArgs map[string]any) *Result {
	if details == nil {
		details = make(map[string]any)
	}

	return &Result{ResultFields: &resultFields{requestID: requestID, details: details, args: args, kwArgs: kwArgs}}
}

func NewResultWithFields(field ResultFields) *Result {
	return &Result{ResultFields: field}
}

func NewResultBinary(requestID uint64, details map[string]any, payload []byte, serializer uint64) *Result {
	if details == nil {
		details = make(map[string]any)
	}

	details["x_payload_serializer"] = serializer

	return &Result{ResultFields: &resultFields{
		requestID:  requestID,
		details:    details,
		binary:     true,
		serializer: serializer,
		payload:    payload,
	}}
}

func (e *Result) Type() uint64 {
	return MessageTypeResult
}

func (m *Result) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeResult, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Result) load(fields map[string]any) {
	m.ResultFields = &resultFields{
		requestID:  bridge.Uint(fields["request_id"]),
		details:    bridge.Dict(fields["details"]),
		binary:     bridge.Bool(fields["_binary"]),
		serializer: bridge.Uint(fields["_serializer"]),
		payload:    bridge.Bytes(fields["_payload"]),
		args:       bridge.Slice(fields["args"]),
		kwArgs:     bridge.Dict(fields["kwargs"]),
	}
}

func (m *Result) RustFields() (uint64, map[string]any) {
	return MessageTypeResult, map[string]any{
		"request_id": m.RequestID(),
		"details":    m.Details(),
		"args":       m.Args(),
		"kwargs":     m.KwArgs(),
	}
}

func (m *Result) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
