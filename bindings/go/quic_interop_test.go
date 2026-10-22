package xconn_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
)

//nolint:contextcheck // Router creation, realm setup and in-memory connections have no context API.
func TestQUICOriginalGoInterop(t *testing.T) {
	binary := os.Getenv("XCONN_REFERENCE_GO_BIN")
	if binary == "" {
		t.Skip("Set XCONN_REFERENCE_GO_BIN to test original Go QUIC peers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Run("rust client to Go server", func(t *testing.T) {
		command := exec.CommandContext(ctx, binary)
		stdout, err := command.StdoutPipe()
		require.NoError(t, err)
		command.Stderr = os.Stderr
		require.NoError(t, command.Start())
		defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
		var addresses map[string]string
		require.NoError(t, json.NewDecoder(stdout).Decode(&addresses))
		certificate, err := hex.DecodeString(addresses["quic_cert"])
		require.NoError(t, err)
		connection, err := xconn.DialQUIC(ctx, addresses["quic"], "localhost", certificate)
		require.NoError(t, err)
		defer connection.Close()
		for _, codec := range []string{"json", "cbor", "msgpack"} {
			session, err := connection.OpenSession(ctx, "realm", &xconn.Client{Serializer: codec})
			require.NoError(t, err)
			result := session.Call("echo").Arg(codec).ExecuteContext(ctx)
			require.NoError(t, result.Err)
			require.Equal(t, codec, result.ArgStringOr(0, ""))
			stream, err := connection.OpenStream(ctx)
			require.NoError(t, err)
			_, err = stream.WriteContext(ctx, []byte("PING"))
			require.NoError(t, err)
			reply := make([]byte, 4)
			_, err = io.ReadFull(stream, reply)
			require.NoError(t, err)
			require.Equal(t, "PING", string(reply))
			require.NoError(t, stream.Close())
			require.NoError(t, session.Close())
		}
	})
	t.Run("Go client to Rust server", func(t *testing.T) {
		router, err := xconn.NewRouter(nil)
		require.NoError(t, err)
		defer router.Close()
		require.NoError(t, router.AddRealm("realm", &xconn.RealmConfig{Roles: []xconn.RealmRole{{
			Name: "anonymous", Permissions: []xconn.Permission{{URI: "rust.echo", AllowCall: true}},
		}}}))
		callee, err := xconn.ConnectInMemory(router, "realm")
		require.NoError(t, err)
		defer callee.Close()
		require.NoError(t, callee.Register("rust.echo", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
			return xconn.NewResult(inv.Args()...)
		}).ExecuteContext(ctx).Err)
		listener, err := router.ListenQUIC(ctx, "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		command := exec.CommandContext(
			ctx, binary, "quic-client", listener.Address, hex.EncodeToString(listener.Certificate),
		)
		output, err := command.CombinedOutput()
		require.NoError(t, err, string(output))
	})
}
