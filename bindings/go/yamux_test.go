package xconn_test

import (
	"bytes"
	"context"
	"encoding/pem"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/xconnio/xconn-core/bindings/go"
)

func TestYamuxTLSUsesNormalServerCertificateValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("example", &api.RealmConfig{Roles: []api.RealmRole{{
		Name: "anonymous", Permissions: []api.Permission{{
			URI: "app.", MatchPolicy: "prefix", AllowRegister: true, AllowCall: true,
		}},
	}}}))
	server := api.NewServer(router, nil, nil)
	t.Cleanup(func() { _ = server.Close() })
	tlsConfig, _, err := api.GenerateWebTransportTLSConfig()
	require.NoError(t, err)
	listener, err := server.ListenAndServeYamuxTLS("127.0.0.1:0", tlsConfig.Certificates[0])
	require.NoError(t, err)
	uri := "yamuxs://" + listener.Address
	_, err = api.ConnectAnonymous(ctx, uri, "example")
	require.Error(t, err, "an untrusted server certificate must be rejected")

	if runtime.GOOS != "linux" {
		t.Skip("temporary SSL_CERT_FILE trust applies to Linux; Rust tests cover explicit TLS roots")
	}

	// Trust this fixture's certificate. Production clients use ordinary system roots.
	root := filepath.Join(t.TempDir(), "root.pem")
	certificate := tlsConfig.Certificates[0].Certificate[0]
	require.NoError(t, os.WriteFile(root, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}), 0o600))
	t.Setenv("SSL_CERT_FILE", root)
	t.Setenv("SSL_CERT_DIR", t.TempDir())
	session, err := api.ConnectAnonymous(ctx, uri, "example")
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	require.NoError(t, session.Register("app.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
		return api.NewResult(inv.Args()...)
	}).ExecuteContext(ctx).Err)
	result := session.Call("app.echo").Arg("encrypted").ExecuteContext(ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "encrypted", result.ArgStringOr(0, ""))
}

func TestYamuxTCPAndUnixCarryOrdinaryWAMP(t *testing.T) {
	for _, network := range []api.Network{api.NetworkTCP, api.NetworkUnix} {
		t.Run(string(network), func(t *testing.T) {
			if network == api.NetworkUnix && runtime.GOOS == "windows" {
				t.Skip("Unix sockets are unavailable")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			router, err := api.NewRouter(nil)
			require.NoError(t, err)
			t.Cleanup(router.Close)
			require.NoError(t, router.AddRealm("example", nil))
			server := api.NewServer(router, legacyPolicy{}, nil)
			t.Cleanup(func() { _ = server.Close() })
			address := "127.0.0.1:0"
			if network == api.NetworkUnix {
				address = filepath.Join(t.TempDir(), "app.sock")
			}
			listener, err := server.ListenAndServeYamux(network, address)
			require.NoError(t, err)
			uri := "yamux://" + listener.Address
			if network == api.NetworkUnix {
				uri = "unix+yamux://" + listener.Address
			}
			_, err = api.ConnectCRA(ctx, uri, "example", "alice", "incorrect")
			require.Error(t, err, "WAMP authentication must reject invalid credentials")
			backend, err := api.ConnectCRA(ctx, uri, "example", "alice", "secret")
			require.NoError(t, err)
			t.Cleanup(func() { _ = backend.Close() })
			require.NoError(t, backend.Register("app.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
				return api.NewResult(inv.Args()...)
			}).ExecuteContext(ctx).Err)
			caller, err := api.ConnectCRA(ctx, uri, "example", "bob", "secret")
			require.NoError(t, err)
			t.Cleanup(func() { _ = caller.Close() })
			payload := bytes.Repeat([]byte{0, 255, 3, 128}, 128*1024)
			result := caller.Call("app.echo").Arg(payload).ExecuteContext(ctx)
			require.NoError(t, result.Err)
			require.Equal(t, payload, result.Args[0])
			require.NoError(t, caller.Close())
			require.NoError(t, server.Close())
			select {
			case <-backend.Done():
			case <-ctx.Done():
				t.Fatal("listener shutdown did not close the accepted session")
			}
		})
	}
}
