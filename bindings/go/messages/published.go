// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type PublishedFields interface {
	RequestID() uint64
	PublicationID() uint64
}

type publishedFields struct {
	requestID     uint64
	publicationID uint64
}

func (p *publishedFields) RequestID() uint64 {
	return p.requestID
}

func (p *publishedFields) PublicationID() uint64 {
	return p.publicationID
}

type Published struct {
	PublishedFields
}

func NewPublished(requestID, publicationID uint64) *Published {
	return &Published{PublishedFields: &publishedFields{requestID: requestID, publicationID: publicationID}}
}

func NewPublishedWithFields(fields PublishedFields) *Published {
	return &Published{PublishedFields: fields}
}

func (p *Published) Type() uint64 {
	return MessageTypePublished
}

func (m *Published) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypePublished, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Published) load(fields map[string]any) {
	m.PublishedFields = &publishedFields{
		requestID:     bridge.Uint(fields["request_id"]),
		publicationID: bridge.Uint(fields["publication_id"]),
	}
}

func (m *Published) RustFields() (uint64, map[string]any) {
	return MessageTypePublished, map[string]any{"request_id": m.RequestID(), "publication_id": m.PublicationID()}
}

func (m *Published) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
