package messages

type Message interface {
	Type() uint64
	Parse([]any) error
	Marshal() []any
}

type BinaryPayload interface {
	PayloadIsBinary() bool
	Payload() []byte
	PayloadSerializer() uint64
}
