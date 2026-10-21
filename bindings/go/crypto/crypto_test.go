package crypto_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	crypto "github.com/xconnio/xconn-core/bindings/go/crypto"
)

func TestPacketsMatchTheIndependentGoImplementation(t *testing.T) {
	data, err := os.ReadFile("../../../tests/crypto/go-reference.json")
	require.NoError(t, err)
	var fixture map[string]string
	require.NoError(t, json.Unmarshal(data, &fixture))
	decode := func(name string) []byte {
		value, err := hex.DecodeString(fixture[name])
		require.NoError(t, err)
		return value
	}
	secret, err := crypto.SharedSecret(decode("private"), decode("public"))
	require.NoError(t, err)
	require.Equal(t, decode("shared"), secret)
	key, err := crypto.DeriveKey(secret, decode("info"))
	require.NoError(t, err)
	require.Equal(t, decode("key"), key)
	plaintext, err := crypto.Decrypt(decode("packet"), key, decode("aad"))
	require.NoError(t, err)
	require.Equal(t, decode("plaintext"), plaintext)
	packet, err := crypto.Encrypt(plaintext, key, decode("aad"))
	require.NoError(t, err)
	result, err := crypto.Decrypt(packet, key, decode("aad"))
	require.NoError(t, err)
	require.Equal(t, plaintext, result)
	_, err = crypto.Decrypt(packet, key, []byte("wrong context"))
	require.Error(t, err)
	_, err = crypto.SharedSecret(decode("private"), bytes.Repeat([]byte{0}, 32))
	require.Error(t, err)
}
