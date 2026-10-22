package xconn_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn-core/bindings/go"
)

func TestStreamDuplexAndCompletion(t *testing.T) {
	forEachSerializer(func(name string, spec xconn.SerializerSpec) {
		t.Run(name, func(t *testing.T) {
			callee, caller := connectedLocalSessions(t, spec.Serializer())
			defer callee.Close()
			defer caller.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			registration := callee.Register("stream.duplex", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
				if err := inv.SendProgress(inv.Args(), nil); err != nil {
					return &xconn.Result{Err: err}
				}
				if inv.Progress() {
					return xconn.NewResultError(xconn.ErrNoResult)
				}
				return xconn.NewResult("done")
			}).
				Execute()
			require.NoError(t, registration.Err)
			call, err := caller.Call("stream.duplex").
				Arg("initial").
				StreamContext(ctx, xconn.StreamOptions{ProgressiveInput: true})
			require.NoError(t, err)
			defer call.Close()
			first, err := call.Receive(ctx)
			require.NoError(t, err)
			require.True(t, first.Progress())
			require.Equal(t, "initial", first.ArgStringOr(0, ""))
			received := make(chan *xconn.Result, 1)
			failures := make(chan error, 1)
			go func() { result, receiveErr := call.Receive(ctx); received <- result; failures <- receiveErr }()
			require.NoError(t, call.Send(ctx, []any{"second"}, nil))
			require.NoError(t, <-failures)
			require.Equal(t, "second", (<-received).ArgStringOr(0, ""))
			require.NoError(t, call.Finish(ctx, []any{"last"}, nil))
			last, err := call.Receive(ctx)
			require.NoError(t, err)
			require.True(t, last.Progress())
			result, err := call.Receive(ctx)
			require.NoError(t, err)
			require.False(t, result.Progress())
			require.Equal(t, "done", result.ArgStringOr(0, ""))
			_, err = call.Receive(ctx)
			require.ErrorIs(t, err, io.EOF)
			require.Error(t, call.Send(ctx, nil, nil))
		})
	})
}

func TestStreamOptionsErrorsAndDeadline(t *testing.T) {
	_, err := (&xconn.CallRequest{}).Stream(xconn.StreamOptions{}, xconn.StreamOptions{})
	require.ErrorContains(t, err, "at most one")
	callee, caller := connectedLocalSessions(t, xconn.CBORSerializerSpec.Serializer())
	defer callee.Close()
	defer caller.Close()
	registration := callee.Register("stream.fail", func(_ context.Context, _ *xconn.Invocation) *xconn.Result {
		return xconn.NewResultError("example.failure", "detail")
	}).
		Execute()
	require.NoError(t, registration.Err)
	call, err := caller.Call("stream.fail").Stream()
	require.NoError(t, err)
	defer call.Close()
	_, err = call.Receive(context.Background())
	var application *xconn.Error
	require.ErrorAs(t, err, &application)
	require.Equal(t, "example.failure", application.URI)
	require.Equal(t, []any{"detail"}, application.Args)
	_, err = call.Receive(context.Background())
	require.ErrorIs(t, err, io.EOF)
	registration = callee.Register("stream.wait", func(ctx context.Context, _ *xconn.Invocation) *xconn.Result {
		<-ctx.Done()
		return xconn.NewResultError(xconn.ErrNoResult)
	}).
		Execute()
	require.NoError(t, registration.Err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	call, err = caller.Call("stream.wait").StreamContext(ctx)
	require.NoError(t, err)
	defer call.Close()
	_, err = call.Receive(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = call.Receive(context.Background())
	require.ErrorIs(t, err, io.EOF)
}

func TestStreamInputAndResultProgressAreIndependent(t *testing.T) {
	modes := []struct {
		name    string
		results xconn.StreamResults
	}{
		{"default", xconn.ResultsDefault},
		{"final", xconn.ResultsFinalOnly},
		{"progressive", xconn.ResultsProgressive},
	}
	for _, progressiveInput := range []bool{false, true} {
		for _, mode := range modes {
			t.Run(fmt.Sprintf("input=%t/results=%s", progressiveInput, mode.name), func(t *testing.T) {
				callee, caller := connectedLocalSessions(t, xconn.JSONSerializerSpec.Serializer())
				defer callee.Close()
				defer caller.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				wantsResults := mode.results != xconn.ResultsFinalOnly
				registered := callee.Register("stream.modes", func(_ context.Context, invocation *xconn.Invocation) *xconn.Result {
					if (invocation.Details()["receive_progress"] == true) != wantsResults {
						return xconn.NewResultError("unexpected.progress.mode")
					}

					if wantsResults {
						if err := invocation.SendProgress([]any{"partial"}, nil); err != nil {
							return &xconn.Result{Err: err}
						}
					}

					if invocation.Progress() {
						return xconn.NewResultError(xconn.ErrNoResult)
					}
					return xconn.NewResult("final")
				}).
					Execute()
				require.NoError(t, registered.Err)

				call, err := caller.Call("stream.modes").Option("receive_progress", !wantsResults).StreamContext(ctx,
					xconn.StreamOptions{ProgressiveInput: progressiveInput, Results: mode.results})
				require.NoError(t, err)
				defer call.Close()

				if progressiveInput {
					if wantsResults {
						result, receiveErr := call.Receive(ctx)
						require.NoError(t, receiveErr)
						require.True(t, result.Progress())
					}
					require.NoError(t, call.Finish(ctx, nil, nil))
				}

				if wantsResults {
					result, receiveErr := call.Receive(ctx)
					require.NoError(t, receiveErr)
					require.True(t, result.Progress())
				}

				result, err := call.Receive(ctx)
				require.NoError(t, err)
				require.False(t, result.Progress())
				require.Equal(t, "final", result.ArgStringOr(0, ""))
				_, err = call.Receive(ctx)
				require.ErrorIs(t, err, io.EOF)
			})
		}
	}
}

func TestStreamRejectsUnknownResultsMode(t *testing.T) {
	callee, caller := connectedLocalSessions(t, xconn.JSONSerializerSpec.Serializer())
	defer callee.Close()
	defer caller.Close()
	_, err := caller.Call("unused").Stream(xconn.StreamOptions{Results: 255})
	require.ErrorContains(t, err, "invalid stream results mode")
}
