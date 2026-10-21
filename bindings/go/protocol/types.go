package protocol

import (
	"github.com/xconnio/xconn-core/bindings/go/internal/bridge"
)

type SessionDetails struct{ fields map[string]any }

func NewSessionDetails(
	id uint64,
	realm, authID, authRole, authMethod string,
	staticSerializer bool,
	routerRoles, authExtra map[string]any,
) *SessionDetails {
	return detailsFromNative(
		bridge.Must(
			"details_new",
			map[string]any{
				"id":         id,
				"realm":      realm,
				"authid":     authID,
				"authrole":   authRole,
				"authmethod": authMethod,
				"static":     staticSerializer,
				"roles":      routerRoles,
				"authextra":  authExtra,
			},
		),
	)
}

func detailsFromNative(v any) *SessionDetails { return &SessionDetails{bridge.Dict(v)} }

func (s *SessionDetails) ID() uint64 { return bridge.Uint(s.fields["id"]) }

func (s *SessionDetails) Realm() string { return bridge.String(s.fields["realm"]) }

func (s *SessionDetails) AuthID() string { return bridge.String(s.fields["authid"]) }

func (s *SessionDetails) AuthRole() string { return bridge.String(s.fields["authrole"]) }

func (s *SessionDetails) AuthMethod() string { return bridge.String(s.fields["authmethod"]) }

func (s *SessionDetails) StaticSerializer() bool { return bridge.Bool(s.fields["static"]) }

func (s *SessionDetails) RouterRoles() map[string]any { return bridge.Dict(s.fields["roles"]) }

func (s *SessionDetails) AuthExtra() map[string]any { return bridge.Dict(s.fields["authextra"]) }

func (s *SessionDetails) CreatedAt() string { return bridge.String(s.fields["created_at"]) }

//nolint:gochecknoglobals // Shared role metadata is supplied by Rust.
var RouterRoles = bridge.Dict(bridge.Must("router_roles"))
