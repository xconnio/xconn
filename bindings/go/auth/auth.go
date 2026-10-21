// Package auth exposes Rust-backed client authentication.
package auth

import (
	"crypto/ed25519"

	"github.com/xconnio/xconn-core/bindings/go/internal/bridge"
	"github.com/xconnio/xconn-core/bindings/go/messages"
)

type Method string

const (
	Anonymous  Method = "anonymous"
	Ticket     Method = "ticket"
	WAMPCRA    Method = "wampcra"
	CryptoSign Method = "cryptosign"
)

//nolint:gochecknoglobals // Retains the original public Go API.
var Methods = []Method{Anonymous, Ticket, WAMPCRA, CryptoSign}

type ClientAuthenticator interface {
	AuthMethod() string
	AuthID() string
	AuthExtra() map[string]any
	Authenticate(messages.Challenge) (*messages.Authenticate, error)
}

type authenticator struct {
	method, id, credential string
	extra                  map[string]any
}

func (a *authenticator) AuthMethod() string { return a.method }

func (a *authenticator) AuthID() string { return a.id }

func (a *authenticator) AuthExtra() map[string]any { return a.extra }

func (a *authenticator) Authenticate(c messages.Challenge) (*messages.Authenticate, error) {
	v, e := bridge.Call("auth", a.method, a.id, a.credential, c.Marshal())
	if e != nil {
		return nil, e
	}

	r := &messages.Authenticate{}
	e = r.Parse(bridge.Slice(v))
	return r, e
}

func newAuth(method, id, credential string, extra map[string]any) ClientAuthenticator {
	return &authenticator{method, id, credential, extra}
}

func NewAnonymousAuthenticator(id string, extra map[string]any) ClientAuthenticator {
	return newAuth("anonymous", id, "", extra)
}

func NewTicketAuthenticator(id, ticket string, extra map[string]any) ClientAuthenticator {
	return newAuth("ticket", id, ticket, extra)
}

func NewWAMPCRAAuthenticator(id, secret string, extra map[string]any) ClientAuthenticator {
	return newAuth("wampcra", id, secret, extra)
}

func NewCryptoSignAuthenticator(id, key string, extra map[string]any) (ClientAuthenticator, error) {
	v, e := bridge.Call("crypto_extra", id, key, extra)
	if e != nil {
		return nil, e
	}

	if extra == nil {
		extra = map[string]any{}
	}

	for k, v := range bridge.Dict(v) {
		extra[k] = v
	}

	return newAuth("cryptosign", id, key, extra), nil
}

func SignWAMPCRAChallengeBytes(challenge string, key []byte) []byte {
	return bridge.Bytes(bridge.Must("cra_sign_bytes", challenge, key))
}

func SignWAMPCRAChallenge(challenge string, key []byte) string {
	return bridge.String(bridge.Must("cra_sign", challenge, key))
}

func DeriveWAMPCRAKey(salt, secret string, iterations, keyLength int) []byte {
	return bridge.Bytes(bridge.Must("cra_derive", salt, secret, iterations, keyLength))
}

func VerifyWAMPCRASignature(signature, challenge string, key []byte) bool {
	v, e := bridge.Call("cra_verify", signature, challenge, key)
	return e == nil && bridge.Bool(v)
}

func GenerateWAMPCRAChallenge(session uint64, id, role, provider string) (string, error) {
	v, e := bridge.Call("cra_challenge", session, id, role, provider)
	return bridge.String(v), e
}

func SignCryptoSignChallenge(challenge string, key ed25519.PrivateKey) (string, error) {
	v, e := bridge.Call("crypto_sign", challenge, []byte(key))
	return bridge.String(v), e
}

func VerifyCryptoSignSignature(signature string, key []byte) (bool, error) {
	v, e := bridge.Call("crypto_verify", signature, key)
	return bridge.Bool(v), e
}

func GenerateCryptoSignChallenge() (string, error) {
	v, e := bridge.Call("crypto_challenge")
	return bridge.String(v), e
}

func GenerateCryptoSignKeyPair() (string, string, error) {
	v, e := bridge.Call("crypto_keypair")
	if e != nil {
		return "", "", e
	}

	r := bridge.Slice(v)
	return bridge.String(r[0]), bridge.String(r[1]), nil
}

const (
	MethodAnonymous  = "anonymous"
	MethodTicket     = "ticket"
	MethodCRA        = "wampcra"
	MethodCryptoSign = "cryptosign"
)
