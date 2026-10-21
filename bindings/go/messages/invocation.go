// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type InvocationFields interface {
	RequestID() uint64
	RegistrationID() uint64
	Details() map[string]any
	Args() []any
	KwArgs() map[string]any

	BinaryPayload
}

type invocationFields struct {
	requestID      uint64
	registrationID uint64
	details        map[string]any
	args           []any
	kwArgs         map[string]any

	binary     bool
	serializer uint64
	payload    []byte
}

func (e *invocationFields) RequestID() uint64 {
	return e.requestID
}

func (e *invocationFields) Details() map[string]any {
	return e.details
}

func (e *invocationFields) RegistrationID() uint64 {
	return e.registrationID
}

func (e *invocationFields) Args() []any {
	return e.args
}

func (e *invocationFields) KwArgs() map[string]any {
	return e.kwArgs
}

func (e *invocationFields) PayloadIsBinary() bool {
	return e.binary
}

func (e *invocationFields) Payload() []byte {
	return e.payload
}

func (e *invocationFields) PayloadSerializer() uint64 {
	return e.serializer
}

type Invocation struct {
	InvocationFields
}

func NewInvocation(requestID, registrationID uint64, details map[string]any, args []any,
	kwArgs map[string]any) *Invocation {
	if details == nil {
		details = make(map[string]any)
	}

	return &Invocation{InvocationFields: &invocationFields{
		requestID:      requestID,
		registrationID: registrationID,
		details:        details,
		args:           args,
		kwArgs:         kwArgs,
	}}
}

func NewInvocationWithFields(fields InvocationFields) *Invocation {
	return &Invocation{InvocationFields: fields}
}

func NewInvocationBinary(requestID, registrationID uint64, details map[string]any, payload []byte,
	serializer uint64) *Invocation {
	if details == nil {
		details = make(map[string]any)
	}

	details["x_payload_serializer"] = serializer

	return &Invocation{InvocationFields: &invocationFields{
		requestID:      requestID,
		registrationID: registrationID,
		details:        details,
		binary:         true,
		serializer:     serializer,
		payload:        payload,
	}}
}

func (e *Invocation) Type() uint64 {
	return MessageTypeInvocation
}

func (m *Invocation) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeInvocation, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Invocation) load(fields map[string]any) {
	m.InvocationFields = &invocationFields{
		requestID:      bridge.Uint(fields["request_id"]),
		registrationID: bridge.Uint(fields["registration_id"]),
		details:        bridge.Dict(fields["details"]),
		binary:         bridge.Bool(fields["_binary"]),
		serializer:     bridge.Uint(fields["_serializer"]),
		payload:        bridge.Bytes(fields["_payload"]),
		args:           bridge.Slice(fields["args"]),
		kwArgs:         bridge.Dict(fields["kwargs"]),
	}
}

func (m *Invocation) RustFields() (uint64, map[string]any) {
	return MessageTypeInvocation, map[string]any{
		"request_id":      m.RequestID(),
		"registration_id": m.RegistrationID(),
		"details":         m.Details(),
		"args":            m.Args(),
		"kwargs":          m.KwArgs(),
	}
}

func (m *Invocation) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
