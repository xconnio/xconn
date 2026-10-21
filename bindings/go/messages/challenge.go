// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type ChallengeFields interface {
	AuthMethod() string
	Extra() map[string]any
}

type challengeFields struct {
	authMethod string
	extra      map[string]any
}

func (c *challengeFields) AuthMethod() string {
	return c.authMethod
}

func (c *challengeFields) Extra() map[string]any {
	return c.extra
}

type Challenge struct {
	ChallengeFields
}

func NewChallengeWithFields(fields ChallengeFields) *Challenge {
	return &Challenge{ChallengeFields: fields}
}

func NewChallenge(authMethod string, extra map[string]any) *Challenge {
	return &Challenge{ChallengeFields: &challengeFields{authMethod: authMethod, extra: extra}}
}

func (c *Challenge) Type() uint64 {
	return MessageTypeChallenge
}

func (m *Challenge) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeChallenge, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Challenge) load(fields map[string]any) {
	m.ChallengeFields = &challengeFields{
		authMethod: bridge.String(fields["authmethod"]),
		extra:      bridge.Dict(fields["extra"]),
	}
}

func (m *Challenge) RustFields() (uint64, map[string]any) {
	return MessageTypeChallenge, map[string]any{"authmethod": m.AuthMethod(), "extra": m.Extra()}
}

func (m *Challenge) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
