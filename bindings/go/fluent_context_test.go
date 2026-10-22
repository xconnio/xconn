package xconn_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn-core/bindings/go"
)

func TestFluentTerminalsPropagateCancelledContext(t *testing.T) {
	session, other := connectedLocalSessions(t, xconn.JSONSerializerSpec.Serializer())
	defer session.Close()
	defer other.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	registered := session.Register("test.cancelled", func(context.Context, *xconn.Invocation) *xconn.Result {
		return xconn.NewResult()
	}).ExecuteContext(ctx)
	require.ErrorIs(t, registered.Err, context.Canceled)
	subscribed := session.Subscribe("test.cancelled", func(*xconn.Event) {}).ExecuteContext(ctx)
	require.ErrorIs(t, subscribed.Err, context.Canceled)
	published := session.Publish("test.cancelled").ExecuteContext(ctx)
	require.ErrorIs(t, published.Err, context.Canceled)
	require.True(t, session.Connected())
}
