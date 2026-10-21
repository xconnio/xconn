// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type UnsubscribeFields interface {
	RequestID() uint64
	SubscriptionID() uint64
}

type unsubscribeFields struct {
	requestID      uint64
	subscriptionID uint64
}

func (us *unsubscribeFields) RequestID() uint64 {
	return us.requestID
}

func (us *unsubscribeFields) SubscriptionID() uint64 {
	return us.subscriptionID
}

type Unsubscribe struct {
	UnsubscribeFields
}

func NewUnsubscribe(requestID, subscriptionID uint64) *Unsubscribe {
	return &Unsubscribe{UnsubscribeFields: &unsubscribeFields{requestID: requestID, subscriptionID: subscriptionID}}
}

func NewUnsubscribeWithFields(fields UnsubscribeFields) *Unsubscribe {
	return &Unsubscribe{UnsubscribeFields: fields}
}

func (us *Unsubscribe) Type() uint64 {
	return MessageTypeUnsubscribe
}

func (m *Unsubscribe) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeUnsubscribe, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Unsubscribe) load(fields map[string]any) {
	m.UnsubscribeFields = &unsubscribeFields{
		requestID:      bridge.Uint(fields["request_id"]),
		subscriptionID: bridge.Uint(fields["subscription_id"]),
	}
}

func (m *Unsubscribe) RustFields() (uint64, map[string]any) {
	return MessageTypeUnsubscribe, map[string]any{"request_id": m.RequestID(), "subscription_id": m.SubscriptionID()}
}

func (m *Unsubscribe) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
