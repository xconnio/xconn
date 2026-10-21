// API field containers adapted from wampproto-go (MIT); protocol behavior delegates to Rust.
package messages

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

type HelloFields interface {
	Realm() string
	AuthID() string
	AuthMethods() []string
	AuthExtra() map[string]any
	Roles() map[string]any
}

type helloFields struct {
	realm       string
	authID      string
	authMethods []string
	authExtra   map[string]any
	roles       map[string]any
}

func (h *helloFields) Realm() string {
	return h.realm
}

func (h *helloFields) AuthID() string {
	return h.authID
}

func (h *helloFields) AuthMethods() []string {
	return h.authMethods
}

func (h *helloFields) AuthExtra() map[string]any {
	return h.authExtra
}

func (h *helloFields) Roles() map[string]any {
	return h.roles
}

type Hello struct {
	HelloFields
}

func NewHelloWithFields(fields HelloFields) *Hello { return &Hello{HelloFields: fields} }

func NewHello(realm, authID string, authExtra, roles map[string]any, authMethods []string) *Hello {
	return &Hello{HelloFields: &helloFields{
		realm:       realm,
		authID:      authID,
		authMethods: authMethods,
		authExtra:   authExtra,
		roles:       roles,
	}}
}

func (h *Hello) Type() uint64 {
	return MessageTypeHello
}

func (m *Hello) Parse(wire []any) error {
	fields, err := bridge.Parse(MessageTypeHello, wire)
	if err != nil {
		return err
	}

	m.load(fields)
	return nil
}

func (m *Hello) load(fields map[string]any) {
	m.HelloFields = &helloFields{
		realm:       bridge.String(fields["realm"]),
		authID:      bridge.String(fields["authid"]),
		authMethods: bridge.Strings(fields["authmethods"]),
		authExtra:   bridge.Dict(fields["authextra"]),
		roles:       bridge.Dict(fields["roles"]),
	}
}

func (m *Hello) RustFields() (uint64, map[string]any) {
	return MessageTypeHello, map[string]any{
		"realm":       m.Realm(),
		"authid":      m.AuthID(),
		"authmethods": m.AuthMethods(),
		"authextra":   m.AuthExtra(),
		"roles":       m.Roles(),
	}
}

func (m *Hello) Marshal() []any {
	code, fields := m.RustFields()
	return bridge.Marshal(code, fields)
}
