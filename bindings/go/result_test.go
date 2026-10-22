package xconn_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn-core/bindings/go"
)

func TestResultCanBeForwardedFromCallerToHandler(t *testing.T) {
	callee, caller := connectedLocalSessions(t, xconn.CBORSerializerSpec.Serializer())
	defer callee.Close()
	defer caller.Close()
	inner := callee.Register("result.inner", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		return &xconn.Result{Args: inv.Args(), Kwargs: inv.Kwargs()}
	}).Execute()
	require.NoError(t, inner.Err)
	defer func() { require.NoError(t, inner.Unregister()) }()
	outer := callee.Register("result.outer", func(ctx context.Context, inv *xconn.Invocation) *xconn.Result {
		return callee.Call("result.inner").Args(inv.Args()...).Kwargs(inv.Kwargs()).ExecuteContext(ctx)
	}).Execute()
	require.NoError(t, outer.Err)
	defer func() { require.NoError(t, outer.Unregister()) }()
	reply := caller.Call("result.outer").Arg("hello").Kwarg("enabled", true).Execute()
	require.NoError(t, reply.Err)
	require.Equal(t, "hello", reply.ArgStringOr(0, ""))
	require.True(t, reply.KwargBoolOr("enabled", false))
	failed := callee.Register("result.failed", func(_ context.Context, _ *xconn.Invocation) *xconn.Result {
		return xconn.NewResultError("example.failure", "detail")
	}).Execute()
	require.NoError(t, failed.Err)
	defer func() { require.NoError(t, failed.Unregister()) }()
	failure := caller.Call("result.failed").Execute()
	var application *xconn.Error
	require.ErrorAs(t, failure.Err, &application)
	require.Equal(t, "example.failure", application.URI)
	require.Equal(t, []any{"detail"}, application.Args)
	require.Equal(t, "hello", xconn.NewResult("hello").ArgStringOr(0, ""))
}
