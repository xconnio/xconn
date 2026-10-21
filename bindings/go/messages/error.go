// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type ErrorFields interface {
	MessageType() uint64
	RequestID() uint64
	Details() map[string]any
	URI() string
	Args() []any
	KwArgs() map[string]any

	BinaryPayload
}

type errorFields struct {
	messageType uint64
	requestID   uint64
	details     map[string]any
	uri         string
	args        []any
	kwArgs      map[string]any
}

func (e *errorFields) MessageType() uint64 {
	return e.messageType
}

func (e *errorFields) RequestID() uint64 {
	return e.requestID
}

func (e *errorFields) Details() map[string]any {
	return e.details
}

func (e *errorFields) URI() string {
	return e.uri
}

func (e *errorFields) Args() []any {
	return e.args
}

func (e *errorFields) KwArgs() map[string]any {
	return e.kwArgs
}

func (e *errorFields) PayloadIsBinary() bool {
	return false
}

func (e *errorFields) Payload() []byte {
	return nil
}

func (e *errorFields) PayloadSerializer() uint64 {
	return 0
}

type Error struct {
	ErrorFields
}

func NewErrorWithFields(fields ErrorFields) *Error { return &Error{ErrorFields: fields} }

func NewError(messageType, requestID uint64, details map[string]any, uri string, args []any,
	kwArgs map[string]any) *Error {
	if details == nil {
		details = make(map[string]any)
	}

	return &Error{ErrorFields: &errorFields{
		messageType: messageType,
		requestID:   requestID,
		details:     details,
		uri:         uri,
		args:        args,
		kwArgs:      kwArgs,
	}}
}

func (e *Error) Type() uint64 {
	return MessageTypeError
}

func (m *Error) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeError, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Error) load(fields map[string]any) {
	m.ErrorFields = &errorFields{
		messageType: bridge.Uint(fields["message_type"]),
		requestID:   bridge.Uint(fields["request_id"]),
		details:     bridge.Dict(fields["details"]),
		uri:         bridge.String(fields["uri"]),
		args:        bridge.Slice(fields["args"]),
		kwArgs:      bridge.Dict(fields["kwargs"]),
	}
}

func (m *Error) RustFields() (uint64, map[string]any) {
	return MessageTypeError, map[string]any{
		"message_type": m.MessageType(),
		"request_id":   m.RequestID(),
		"details":      m.Details(),
		"uri":          m.URI(),
		"args":         m.Args(),
		"kwargs":       m.KwArgs(),
	}
}

func (m *Error) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
