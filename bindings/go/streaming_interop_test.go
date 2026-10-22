package xconn_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn-core/bindings/go"
)

// TestStreamingInteropPeer is launched in a separate process by the Python tests.
func TestStreamingInteropPeer(t *testing.T) {
	mode := os.Getenv("XCONN_STREAM_MODE")
	if mode == "" {
		t.Skip("started by the Python cross-language tests")
	}
	if mode == "serve" {
		serveStreamingPeer(t)
		return
	}
	require.Equal(t, "verify", mode)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := xconn.Client{Serializer: os.Getenv("XCONN_STREAM_SERIALIZER")}
	session, err := client.Connect(ctx, os.Getenv("XCONN_STREAM_URI"), "realm")
	require.NoError(t, err)
	defer session.Close()
	verifyStreamingPeer(t, ctx, session)
}

func serveStreamingPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	require.NoError(t, router.AddRealm("realm", &xconn.RealmConfig{Roles: []xconn.RealmRole{{
		Name: "anonymous",
		Permissions: []xconn.Permission{
			{URI: "", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true},
		},
	}}}))
	websocket, err := router.Listen(ctx, "127.0.0.1:0", "websocket")
	require.NoError(t, err)
	rawsocket, err := router.Listen(ctx, "127.0.0.1:0", "rawsocket")
	require.NoError(t, err)
	session, err := router.Connect(ctx, "cbor")
	require.NoError(t, err)
	defer session.Close()
	var mu sync.Mutex
	cancelled := make(map[string]bool)
	handlers := map[string]func(context.Context, *xconn.Invocation) *xconn.Result{
		"duplex": func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
			if err := inv.SendProgress(inv.Args(), inv.Kwargs()); err != nil {
				return &xconn.Result{Err: err}
			}
			if inv.Progress() {
				return xconn.NewResultError(xconn.ErrNoResult)
			}
			return &xconn.Result{Args: []any{"done"}, Kwargs: map[string]any{"complete": true}}
		},
		"fail": func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
			if err := inv.SendProgress([]any{"before-error"}, nil); err != nil {
				return &xconn.Result{Err: err}
			}
			return &xconn.Result{
				Err: &xconn.Error{
					URI:    "example.failure",
					Args:   []any{"detail"},
					Kwargs: map[string]any{"enabled": true},
				},
			}
		},
		"wait": func(ctx context.Context, inv *xconn.Invocation) *xconn.Result {
			token := inv.ArgStringOr(0, "")
			if err := inv.SendProgress([]any{"started"}, nil); err != nil {
				return &xconn.Result{Err: err}
			}
			<-ctx.Done()
			mu.Lock()
			cancelled[token] = true
			mu.Unlock()
			return xconn.NewResultError(xconn.ErrNoResult)
		},
		"cancelled": func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
			mu.Lock()
			defer mu.Unlock()
			return xconn.NewResult(cancelled[inv.ArgStringOr(0, "")])
		},
	}
	for name, handler := range handlers {
		require.NoError(t, session.Register("go."+name, handler).Execute().Err)
	}
	require.NoError(t, json.NewEncoder(os.Stdout).Encode(map[string]string{
		"ws": "ws://" + websocket.Address, "rs": "rs://" + rawsocket.Address,
	}))
	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
}

func verifyStreamingPeer(t *testing.T, ctx context.Context, session *xconn.Session) {
	nested := map[string]any{"nested": []any{true, nil, "مرحبا 🦀"}}
	call, err := session.Call("python.duplex").Args("first", nested).Kwarg("marker", "initial").
		StreamContext(ctx, xconn.StreamOptions{ProgressiveInput: true})
	require.NoError(t, err)
	defer call.Close()
	first, err := call.Receive(ctx)
	require.NoError(t, err)
	require.True(t, first.Progress())
	require.Equal(t, []any{"first", nested}, first.Args)
	require.Equal(t, map[string]any{"marker": "initial"}, first.Kwargs)
	type received struct {
		result *xconn.Result
		err    error
	}
	receiving := make(chan received, 1)
	go func() { result, receiveErr := call.Receive(ctx); receiving <- received{result, receiveErr} }()
	require.NoError(t, call.Send(ctx, []any{"second"}, map[string]any{"marker": "middle"}))
	next := <-receiving
	require.NoError(t, next.err)
	require.Equal(t, []any{"second"}, next.result.Args)
	require.Equal(t, "middle", next.result.Kwargs["marker"])
	require.NoError(t, call.Finish(ctx, []any{"last"}, map[string]any{"marker": "final"}))
	last, err := call.Receive(ctx)
	require.NoError(t, err)
	require.True(t, last.Progress())
	require.Equal(t, []any{"last"}, last.Args)
	require.Equal(t, "final", last.Kwargs["marker"])
	final, err := call.Receive(ctx)
	require.NoError(t, err)
	require.False(t, final.Progress())
	require.Equal(t, []any{"done"}, final.Args)
	require.Equal(t, map[string]any{"complete": true}, final.Kwargs)
	_, err = call.Receive(ctx)
	require.ErrorIs(t, err, io.EOF)

	failure, err := session.Call("python.fail").StreamContext(ctx)
	require.NoError(t, err)
	defer failure.Close()
	before, err := failure.Receive(ctx)
	require.NoError(t, err)
	require.True(t, before.Progress())
	require.Equal(t, []any{"before-error"}, before.Args)
	_, err = failure.Receive(ctx)
	var application *xconn.Error
	require.ErrorAs(t, err, &application)
	require.Equal(t, "example.failure", application.URI)
	require.Equal(t, []any{"detail"}, application.Args)
	require.Equal(t, map[string]any{"enabled": true}, application.Kwargs)
	_, err = failure.Receive(ctx)
	require.ErrorIs(t, err, io.EOF)

	for _, token := range []string{"cancel", "deadline"} {
		lifetime, stop := context.WithCancel(ctx)
		waiting, openErr := session.Call("python.wait").Arg(token).StreamContext(lifetime)
		require.NoError(t, openErr)
		started, receiveErr := waiting.Receive(ctx)
		require.NoError(t, receiveErr)
		require.Equal(t, []any{"started"}, started.Args)
		if token == "cancel" {
			require.NoError(t, waiting.Close())
			_, receiveErr = waiting.Receive(ctx)
			require.ErrorIs(t, receiveErr, context.Canceled)
		} else {
			deadline, finish := context.WithTimeout(ctx, 30*time.Millisecond)
			_, receiveErr = waiting.Receive(deadline)
			finish()
			require.ErrorIs(t, receiveErr, context.DeadlineExceeded)
		}
		stop()
		require.Eventually(t, func() bool {
			reply := session.Call("python.cancelled").Arg(token).ExecuteContext(ctx)
			return reply.Err == nil && reply.ArgBoolOr(0, false)
		}, 3*time.Second, 10*time.Millisecond)
	}
}
