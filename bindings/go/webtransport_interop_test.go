package xconn_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
)

//nolint:contextcheck // Router setup and in-memory joins have no context API.
func TestWebTransportOriginalGoInterop(t *testing.T) {
	binary := os.Getenv("XCONN_REFERENCE_GO_BIN")
	if binary == "" {
		t.Skip("Set XCONN_REFERENCE_GO_BIN to test original Go WebTransport peers")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	t.Run("Rust client to Go server", func(t *testing.T) {
		command := exec.CommandContext(ctx, binary)
		stdout, err := command.StdoutPipe()
		require.NoError(t, err)
		command.Stderr = os.Stderr
		require.NoError(t, command.Start())
		defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
		var addresses map[string]string
		require.NoError(t, json.NewDecoder(stdout).Decode(&addresses))
		trust := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true,
			VerifyConnection: func(state tls.ConnectionState) error {
				hash := sha256.Sum256(state.PeerCertificates[0].Raw)
				if hex.EncodeToString(hash[:]) != addresses["wt_hash"] {
					return fmt.Errorf("certificate pin mismatch")
				}
				return nil
			}}
		connection, err := xconn.DialWebTransport(ctx, addresses["wt"], trust)
		require.NoError(t, err)
		defer connection.Close()
		for _, codec := range []string{"json", "cbor", "msgpack"} {
			session, err := connection.OpenSession(ctx, "realm", &xconn.Client{Serializer: codec})
			require.NoError(t, err)
			response := session.Call("echo").Arg(codec).ExecuteContext(ctx)
			require.NoError(t, response.Err)
			require.Equal(t, codec, response.ArgStringOr(0, ""))
			require.NoError(t, session.Close())
		}
	})
	t.Run("Go client to Rust server", func(t *testing.T) {
		router, err := xconn.NewRouter(nil)
		require.NoError(t, err)
		defer router.Close()
		require.NoError(t, router.AddRealm("realm", &xconn.RealmConfig{Roles: []xconn.RealmRole{{Name: "anonymous",
			Permissions: []xconn.Permission{{URI: "rust.echo", AllowCall: true}}}}}))
		callee, err := xconn.ConnectInMemory(router, "realm")
		require.NoError(t, err)
		defer callee.Close()
		require.NoError(t, callee.Register("rust.echo", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
			return xconn.NewResult(inv.Args()...)
		}).ExecuteContext(ctx).Err)
		identity, err := xconn.GenerateSelfSignedTLSConfig()
		require.NoError(t, err)
		server := xconn.NewServer(router, nil, nil)
		defer server.Close()
		listener, err := server.ListenWebTransport(ctx, "127.0.0.1:0", "/wamp", identity.Certificates[0])
		require.NoError(t, err)
		defer listener.Close()
		command := exec.CommandContext(
			ctx,
			binary,
			"wt-client",
			"https://"+listener.Address+"/wamp",
			hex.EncodeToString(listener.Certificate),
		)
		output, err := command.CombinedOutput()
		require.NoError(t, err, string(output))
	})
}
