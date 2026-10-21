// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type SubscribedFields interface {
	RequestID() uint64
	SubscriptionID() uint64
}

type subscribedFields struct {
	requestID      uint64
	subscriptionID uint64
}

func (s *subscribedFields) RequestID() uint64 {
	return s.requestID
}

func (s *subscribedFields) SubscriptionID() uint64 {
	return s.subscriptionID
}

type Subscribed struct {
	SubscribedFields
}

func NewSubscribed(requestID, subscriptionID uint64) *Subscribed {
	return &Subscribed{SubscribedFields: &subscribedFields{requestID: requestID, subscriptionID: subscriptionID}}
}

func NewSubscribedWithFields(fields SubscribedFields) *Subscribed {
	return &Subscribed{SubscribedFields: fields}
}

func (s *Subscribed) Type() uint64 {
	return MessageTypeSubscribed
}

func (m *Subscribed) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeSubscribed, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Subscribed) load(fields map[string]any) {
	m.SubscribedFields = &subscribedFields{
		requestID:      bridge.Uint(fields["request_id"]),
		subscriptionID: bridge.Uint(fields["subscription_id"]),
	}
}

func (m *Subscribed) RustFields() (uint64, map[string]any) {
	return MessageTypeSubscribed, map[string]any{"request_id": m.RequestID(), "subscription_id": m.SubscriptionID()}
}

func (m *Subscribed) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
