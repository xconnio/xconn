//go:build cgo && (linux || darwin)

package webrtc_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
	rtc "github.com/xconnio/xconn-core/bindings/go/webrtc"
)

func referenceBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("XCONN_REFERENCE_GO_BIN")
	if binary == "" {
		t.Skip("Set XCONN_REFERENCE_GO_BIN to test the original pure-Go implementation")
	}
	return binary
}

func TestRustBackedClientToOriginalGoProvider(t *testing.T) {
	binary := referenceBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	command.Stderr = os.Stderr
	require.NoError(t, command.Start())
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	var addresses map[string]string
	require.NoError(t, json.NewDecoder(stdout).Decode(&addresses))
	signaling, err := xconn.ConnectAnonymous(ctx, addresses["ws"], "realm")
	require.NoError(t, err)
	defer signaling.Close()
	specs := []xconn.SerializerSpec{xconn.JSONSerializerSpec, xconn.CBORSerializerSpec, xconn.MsgPackSerializerSpec}
	for _, spec := range specs {
		t.Run(spec.SubProtocol(), func(t *testing.T) {
			session, err := rtc.ConnectWAMPContext(ctx, &rtc.ClientConfig{
				Session: signaling, Realm: "realm", Serializer: spec,
				ProcedureWebRTCOffer: "offer", TopicAnswererOnCandidate: "to-provider",
				TopicOffererOnCandidate: "to-client", ConnectTimeout: 5 * time.Second,
			})
			require.NoError(t, err)
			defer session.Connection().CloseContext(context.WithoutCancel(ctx))
			defer session.CloseContext(context.WithoutCancel(ctx))
			result := session.Call("echo").Arg("original Go provider").ExecuteContext(ctx)
			require.NoError(t, result.Err)
			require.Equal(t, "original Go provider", result.ArgStringOr(0, ""))
			another, err := session.OpenSessionContext(ctx, "realm", nil)
			require.NoError(t, err)
			defer another.CloseContext(context.WithoutCancel(ctx))
			require.NoError(t, session.CloseContext(context.WithoutCancel(ctx)))
			require.NoError(t, another.Call("echo").Arg("independent").ExecuteContext(ctx).Err)
		})
	}
}

func TestOriginalGoClientToRustBackedProvider(t *testing.T) {
	binary := referenceBinary(t)
	f := newFixture(t, nil)
	listener, err := f.router.Listen(f.ctx, "127.0.0.1:0", "websocket")
	require.NoError(t, err)
	defer listener.Close()
	result := f.callee.Register("rust.echo", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		return xconn.NewResult(inv.Args()...)
	}).Execute()
	require.NoError(t, result.Err)
	output, err := exec.CommandContext(f.ctx, binary, "rtc-client", "ws://"+listener.Address).CombinedOutput()
	require.NoError(t, err, string(output))
}
