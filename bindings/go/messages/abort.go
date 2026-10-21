// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type AbortFields interface {
	Details() map[string]any
	Reason() string
	Args() []any
	KwArgs() map[string]any
}

type abortFields struct {
	details map[string]any
	reason  string
	args    []any
	kwArgs  map[string]any
}

func (a *abortFields) Details() map[string]any {
	return a.details
}

func (a *abortFields) Reason() string {
	return a.reason
}

func (a *abortFields) Args() []any {
	return a.args
}

func (a *abortFields) KwArgs() map[string]any {
	return a.kwArgs
}

type Abort struct {
	AbortFields
}

func NewAbort(details map[string]any, reason string, args []any, KwArgs map[string]any) *Abort {
	if details == nil {
		details = make(map[string]any)
	}

	return &Abort{AbortFields: &abortFields{
		details: details,
		reason:  reason,
		args:    args,
		kwArgs:  KwArgs,
	}}
}

func NewAbortWithFields(fields AbortFields) *Abort { return &Abort{AbortFields: fields} }

func (a *Abort) Type() uint64 {
	return MessageTypeAbort
}

func (m *Abort) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeAbort, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Abort) load(fields map[string]any) {
	m.AbortFields = &abortFields{
		details: bridge.Dict(fields["details"]),
		reason:  bridge.String(fields["reason"]),
		args:    bridge.Slice(fields["args"]),
		kwArgs:  bridge.Dict(fields["kwargs"]),
	}
}

func (m *Abort) RustFields() (uint64, map[string]any) {
	return MessageTypeAbort, map[string]any{
		"details": m.Details(),
		"reason":  m.Reason(),
		"args":    m.Args(),
		"kwargs":  m.KwArgs(),
	}
}

func (m *Abort) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
