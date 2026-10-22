// Package xconn provides native Go APIs backed by the shared Rust networking runtime.
package xconn

import "github.com/xconnio/xconn-core/bindings/go/protocol"

type Options = map[string]any

// Result carries the same payload in handler replies and received RPC results.
// Err contains a native error when an operation or application reply fails.
type Result struct {
	invocationID uint64
	Err          error
	Args         []any
	Kwargs       map[string]any
	Details      map[string]any
}

type SessionDetails = protocol.SessionDetails

func NewSessionDetails(
	id uint64,
	realm, authid, authrole, authmethod string,
	authextra map[string]any,
) *SessionDetails {
	return protocol.NewSessionDetails(id, realm, authid, authrole, authmethod, false, protocol.RouterRoles, authextra)
}

// NewResult constructs a successful payload from positional arguments.
func NewResult(args ...any) *Result { return &Result{Args: args} }

// NewResultError constructs an application-error reply, retaining its WAMP URI and payload.
func NewResultError(uri string, args ...any) *Result {
	return &Result{Err: &Error{URI: uri, Args: args}}
}

func (r *Result) ArgsLen() int { return len(r.Args) }

func (r *Result) KwargsLen() int { return len(r.Kwargs) }

func (r *Result) ArgsStruct(out any) error { return NewList(r.Args).Decode(out) }

func (r *Result) KwargsStruct(out any) error { return NewDict(r.Kwargs).Decode(out) }

func (r *Result) Progress() bool { return r.Details["progress"] == true }
