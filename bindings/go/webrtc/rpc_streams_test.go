//go:build cgo && (linux || darwin)

package webrtc_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
)

func TestRPCStreamsUploadAndDownload(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	payload := bytes.Repeat([]byte{0, 255, 128, 3}, 384*1024)
	require.NoError(t, f.callee.Register("transfer", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		stream, err := inv.ArgStream(1)
		if err != nil {
			return xconn.NewResultError("test.bad_stream", err.Error())
		}
		defer stream.Close()
		body, err := io.ReadAll(stream)
		if err != nil {
			return xconn.NewResultError("test.read_failed", err.Error())
		}
		return xconn.NewResult(inv.ArgStringOr(0, ""), xconn.ReadStream(io.NopCloser(bytes.NewReader(body))))
	}).ExecuteContext(f.ctx).Err)
	result := session.Call("transfer").Arg("binary.dat").
		Arg(xconn.ReadStream(io.NopCloser(bytes.NewReader(payload)))).ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "binary.dat", result.ArgStringOr(0, ""))
	stream, err := result.ArgStream(1)
	require.NoError(t, err)
	defer stream.Close()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, payload, body)
	require.NoError(t, session.Call("echo").Arg("still connected").ExecuteContext(f.ctx).Err)
}

func TestRPCStreamHalfClosePreservesLargeReply(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	payload := bytes.Repeat([]byte("reply"), 256*1024)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	endpoint, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer endpoint.Close()
	process, err := listener.Accept()
	require.NoError(t, err)
	defer process.Close()
	completed := make(chan error, 1)
	require.NoError(t, f.callee.Register("duplex", func(ctx context.Context, _ *xconn.Invocation) *xconn.Result {
		stop := context.AfterFunc(ctx, func() { _ = process.Close() })
		go func() {
			defer stop()
			defer process.Close()
			body, err := io.ReadAll(process)
			if err == nil && string(body) == "request" {
				_, err = process.Write(payload)
			}
			completed <- err
		}()
		return xconn.NewResult(xconn.DuplexStream(endpoint))
	}).ExecuteContext(f.ctx).Err)
	result := session.Call("duplex").ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer stream.Close()
	_, err = stream.Write([]byte("request"))
	require.NoError(t, err)
	half, ok := stream.(interface{ CloseWrite() error })
	require.True(t, ok)
	require.NoError(t, half.CloseWrite())
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, payload, body)
	select {
	case err = <-completed:
		require.NoError(t, err)
	case <-f.ctx.Done():
		t.Fatal("server did not finish its response")
	}
}

func TestRPCStreamSlowConsumerDoesNotCloseChannel(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	payload := bytes.Repeat([]byte("buffered"), 384*1024)
	require.NoError(t, f.callee.Register("download", func(context.Context, *xconn.Invocation) *xconn.Result {
		return xconn.NewResult(xconn.ReadStream(io.NopCloser(bytes.NewReader(payload))))
	}).ExecuteContext(f.ctx).Err)
	result := session.Call("download").ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer stream.Close()
	// Pause consumption long enough to fill the bounded receive queue. SCTP
	// backpressure should pause this stream rather than discard its data.
	time.Sleep(150 * time.Millisecond)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, payload, body)
	require.NoError(t, session.Call("echo").Arg("still connected").ExecuteContext(f.ctx).Err)
}

func TestRPCStreamCancellationClosesItsSource(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	reader, writer := io.Pipe()
	defer writer.Close()
	require.NoError(t, f.callee.Register("wait", func(context.Context, *xconn.Invocation) *xconn.Result {
		return xconn.NewResult(xconn.ReadStream(reader))
	}).ExecuteContext(f.ctx).Err)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	result := session.Call("wait").ExecuteContext(ctx)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer stream.Close()
	cancel()
	closed := make(chan error, 1)
	go func() {
		for {
			if _, writeErr := writer.Write([]byte("x")); writeErr != nil {
				closed <- writeErr
				return
			}
		}
	}()
	select {
	case err = <-closed:
		require.ErrorIs(t, err, io.ErrClosedPipe)
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled stream did not close its source")
	}
}

func TestSecondaryWAMPSessionRejectsRPCStreamOffers(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	secondary, err := session.OpenSessionContext(f.ctx, "realm", nil)
	require.NoError(t, err)
	defer secondary.Close()
	require.NoError(t, secondary.Call("echo").Arg("ordinary RPC").ExecuteContext(f.ctx).Err)
	result := secondary.Call("echo").Arg(xconn.ReadStream(io.NopCloser(bytes.NewReader([]byte("x"))))).
		ExecuteContext(f.ctx)
	require.ErrorContains(t, result.Err, "does not support RPC streams")
	require.NoError(t, session.Call("echo").ExecuteContext(f.ctx).Err)
}

func TestRPCStreamsCrossWebRTCAndYamuxBridge(t *testing.T) {
	f := newFixture(t, nil)
	local, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(local.Close)
	require.NoError(t, local.AddRealm("local", &xconn.RealmConfig{Roles: []xconn.RealmRole{{
		Name: "anonymous", Permissions: []xconn.Permission{{
			URI: "stream.", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true,
		}},
	}}}))
	server := xconn.NewServer(local, nil, nil)
	t.Cleanup(func() { _ = server.Close() })
	listener, err := server.ListenAndServeYamux(xconn.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	target, err := xconn.ConnectAnonymous(f.ctx, "yamux://"+listener.Address, "local")
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })
	backend, err := xconn.ConnectAnonymous(f.ctx, "yamux://"+listener.Address, "local")
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	require.NoError(t, backend.Register("stream.copy", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		stream, openErr := inv.ArgStream(1)
		if openErr != nil {
			return xconn.NewResultError("app.bad_stream", openErr.Error())
		}
		defer func() { _ = stream.Close() }()
		body, readErr := io.ReadAll(stream)
		if readErr != nil {
			return xconn.NewResultError("app.read_failed", readErr.Error())
		}
		return xconn.NewResult(inv.ArgStringOr(0, ""), xconn.ReadStream(io.NopCloser(bytes.NewReader(body))))
	}).ExecuteContext(f.ctx).Err)
	bridge, err := f.callee.Bridge(f.ctx, xconn.BridgeConfig{
		Target: target,
		Routes: []xconn.BridgeRoute{{Procedure: "stream.", Options: xconn.Options{"match": "prefix"}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bridge.Close() })
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	payload := bytes.Repeat([]byte{0, 255, 128, 3}, 384*1024)
	result := session.Call("stream.copy").Arg("report.bin").
		Arg(xconn.ReadStream(io.NopCloser(bytes.NewReader(payload)))).ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "report.bin", result.ArgStringOr(0, ""))
	stream, err := result.ArgStream(1)
	require.NoError(t, err)
	defer stream.Close()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, payload, body)
	require.NoError(t, session.Call("echo").Arg("still connected").ExecuteContext(f.ctx).Err)
}
