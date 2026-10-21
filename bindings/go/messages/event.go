// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type EventFields interface {
	SubscriptionID() uint64
	PublicationID() uint64
	Details() map[string]any
	Args() []any
	KwArgs() map[string]any

	BinaryPayload
}

type eventFields struct {
	subscriptionID uint64
	publicationID  uint64
	details        map[string]any
	args           []any
	kwArgs         map[string]any
}

func (e *eventFields) SubscriptionID() uint64 {
	return e.subscriptionID
}

func (e *eventFields) PublicationID() uint64 {
	return e.publicationID
}

func (e *eventFields) Details() map[string]any {
	return e.details
}

func (e *eventFields) Args() []any {
	return e.args
}

func (e *eventFields) KwArgs() map[string]any {
	return e.kwArgs
}

func (e *eventFields) PayloadIsBinary() bool {
	return false
}

func (e *eventFields) Payload() []byte {
	return nil
}

func (e *eventFields) PayloadSerializer() uint64 {
	return 0
}

type Event struct {
	EventFields
}

func NewEventWithFields(fields EventFields) *Event { return &Event{EventFields: fields} }

func NewEvent(subscriptionID, publicationID uint64, details map[string]any, args []any, kwArgs map[string]any) *Event {
	if details == nil {
		details = make(map[string]any)
	}

	return &Event{EventFields: &eventFields{
		subscriptionID: subscriptionID,
		publicationID:  publicationID,
		details:        details,
		args:           args,
		kwArgs:         kwArgs,
	}}
}

func (e *Event) Type() uint64 {
	return MessageTypeEvent
}

func (m *Event) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeEvent, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Event) load(fields map[string]any) {
	m.EventFields = &eventFields{
		subscriptionID: bridge.Uint(fields["subscription_id"]),
		publicationID:  bridge.Uint(fields["publication_id"]),
		details:        bridge.Dict(fields["details"]),
		args:           bridge.Slice(fields["args"]),
		kwArgs:         bridge.Dict(fields["kwargs"]),
	}
}

func (m *Event) RustFields() (uint64, map[string]any) {
	return MessageTypeEvent, map[string]any{
		"subscription_id": m.SubscriptionID(),
		"publication_id":  m.PublicationID(),
		"details":         m.Details(),
		"args":            m.Args(),
		"kwargs":          m.KwArgs(),
	}
}

func (m *Event) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
