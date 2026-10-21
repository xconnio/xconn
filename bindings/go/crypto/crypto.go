// Package crypto exposes shared Rust packet encryption. Applications must
// authenticate peer keys; X25519 alone does not establish the peer's identity.
package crypto

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

func GenerateKeyPair() (public, private []byte, err error) {
	value, err := bridge.Call("crypto_key_pair")
	if err != nil {
		return nil, nil, err
	}
	fields := bridge.Dict(value)
	return bridge.Bytes(fields["public"]), bridge.Bytes(fields["private"]), nil
}

func SharedSecret(private, public []byte) ([]byte, error) {
	value, err := bridge.Call("crypto_shared_secret", private, public)
	return bridge.Bytes(value), err
}

func DeriveKey(secret, info []byte) ([]byte, error) {
	value, err := bridge.Call("crypto_derive_key", secret, info)
	return bridge.Bytes(value), err
}

// Encrypt returns nonce || ciphertext || authentication tag using ChaCha20-Poly1305.
func Encrypt(plaintext, key, additionalData []byte) ([]byte, error) {
	if plaintext == nil {
		plaintext = []byte{}
	}
	if additionalData == nil {
		additionalData = []byte{}
	}
	value, err := bridge.Call("crypto_encrypt", plaintext, key, additionalData)
	return bridge.Bytes(value), err
}

func Decrypt(packet, key, additionalData []byte) ([]byte, error) {
	if additionalData == nil {
		additionalData = []byte{}
	}
	value, err := bridge.Call("crypto_decrypt", packet, key, additionalData)
	return bridge.Bytes(value), err
}
