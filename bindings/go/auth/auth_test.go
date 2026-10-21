package auth_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/xconnio/xconn-core/bindings/go/auth"
	"github.com/xconnio/xconn-core/bindings/go/messages"
)

func TestCRAAgainstGoCrypto(t *testing.T) {
	key := []byte("secret")
	challenge := "challenge"
	h := hmac.New(sha256.New, key)
	h.Write([]byte(challenge))
	want := h.Sum(nil)
	if got := auth.SignWAMPCRAChallengeBytes(challenge, key); !bytes.Equal(got, want) {
		t.Fatal(got)
	}
	signature := auth.SignWAMPCRAChallenge(challenge, key)
	if signature != base64.StdEncoding.EncodeToString(want) {
		t.Fatal(signature)
	}
	if !auth.VerifyWAMPCRASignature(signature, challenge, key) || auth.VerifyWAMPCRASignature(signature, "wrong", key) {
		t.Fatal("verification")
	}
	a := auth.NewWAMPCRAAuthenticator("alice", "secret", nil)
	for _, extra := range []map[string]any{
		{
			"challenge": challenge,
		},
		{
			"challenge": challenge,
			"salt":      "salt",
		},
		{
			"challenge":  challenge,
			"salt":       "salt",
			"iterations": 0,
			"keylen":     0,
		},
	} {
		reply, e := a.Authenticate(*messages.NewChallenge("wampcra", extra))
		if e != nil {
			t.Fatal(e)
		}
		k := key
		if _, ok := extra["salt"]; ok {
			k = auth.DeriveWAMPCRAKey("salt", "secret", 1000, 32)
		}
		if reply.Signature() != auth.SignWAMPCRAChallenge(challenge, k) {
			t.Fatal("signature mismatch")
		}
	}
	if _, e := a.Authenticate(*messages.NewChallenge("wampcra", map[string]any{})); e == nil {
		t.Fatal("missing challenge accepted")
	}
}

func TestCryptoSignAgainstGoCrypto(t *testing.T) {
	seed := "022b089bed5ab78808365e82dd12c796c835aeb98b4a5a9e099d3e72cb719516"
	public := "2b7ec216daa877c7f4c9439db8a722ea2340eacad506988db2564e258284f895"
	challenge := "5c7b195948a15a9f94bdc31cf6d88294e380985e2c40f5f912fd707e080cb5ff"
	a, e := auth.NewCryptoSignAuthenticator("alice", seed, nil)
	if e != nil {
		t.Fatal(e)
	}
	if a.AuthExtra()["pubkey"] != public {
		t.Fatal(a.AuthExtra())
	}
	r, e := a.Authenticate(*messages.NewChallenge("cryptosign", map[string]any{"challenge": challenge}))
	if e != nil {
		t.Fatal(e)
	}
	signed, _ := hex.DecodeString(r.Signature())
	pub, _ := hex.DecodeString(public)
	data, _ := hex.DecodeString(challenge)
	if !ed25519.Verify(pub, data, signed[:64]) || !bytes.Equal(signed[64:], data) {
		t.Fatal("invalid signature")
	}
	ok, e := auth.VerifyCryptoSignSignature(r.Signature(), pub)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	if _, e = auth.NewCryptoSignAuthenticator("alice", seed, map[string]any{"pubkey": "wrong"}); e == nil {
		t.Fatal("mismatch accepted")
	}
	if _, e = auth.VerifyCryptoSignSignature("bad", pub); e == nil {
		t.Fatal("bad signature accepted")
	}
	p, k, e := auth.GenerateCryptoSignKeyPair()
	if e != nil || len(p) != 64 || len(k) != 64 {
		t.Fatal(p, k, e)
	}
}

func TestCRAUnsignedParameters(t *testing.T) {
	a := auth.NewWAMPCRAAuthenticator("alice", "secret", nil)
	reply, e := a.Authenticate(
		*messages.NewChallenge(
			"wampcra",
			map[string]any{
				"challenge": "hello",
				"salt":      "salt",
				"iterations": uint64(
					1000,
				),
				"keylen": uint64(
					32,
				),
			},
		),
	)
	if e != nil {
		t.Fatal(e)
	}
	key := auth.DeriveWAMPCRAKey("salt", "secret", 1000, 32)
	if reply.Signature() != auth.SignWAMPCRAChallenge("hello", key) {
		t.Fatal("unsigned parameters changed derivation")
	}
}
