package xconn_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/xconnio/xconn-core/bindings/go"
)

func TestAttachmentOwnsUpstreamSessionsAndPreservesBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	for _, realm := range []string{"device", "control", "backend"} {
		require.NoError(t, router.AddRealm(realm, &api.RealmConfig{}))
	}
	connect := func(realm string) *api.Session {
		session, connectErr := api.ConnectInMemory(router, realm)
		require.NoError(t, connectErr)
		t.Cleanup(func() { _ = session.Close() })
		return session
	}
	backend := connect("backend")
	device := connect("device")
	control := connect("control")
	caller := connect("device")
	require.NoError(t, backend.Register("example.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
		return api.NewResult(inv.Args()...)
	}).ExecuteContext(ctx).Err)
	bridge, err := device.Bridge(ctx, api.BridgeConfig{
		Target: backend, Routes: []api.BridgeRoute{{Procedure: "example.echo"}},
	})
	require.NoError(t, err)
	var shutdowns atomic.Int32
	attachment, err := api.NewAttachment(ctx, api.AttachmentConfig{
		Sessions: []*api.Session{device, control}, Bridges: []*api.Bridge{bridge},
		Shutdown: func() error { shutdowns.Add(1); return nil },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = attachment.Close() })
	require.NoError(t, caller.Call("example.echo").ExecuteContext(ctx).Err)
	require.NoError(t, control.Close())
	require.NoError(t, attachment.Wait(ctx))
	require.NoError(t, attachment.Close())
	require.EqualValues(t, 1, shutdowns.Load())
	require.Error(t, caller.Call("example.echo").ExecuteContext(ctx).Err)
	require.NoError(t, backend.Call("example.echo").ExecuteContext(ctx).Err)
}

func TestAttachmentCleanupOutlivesCloseContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("example", &api.RealmConfig{}))
	session, err := api.ConnectInMemory(router, "example")
	require.NoError(t, err)
	started := make(chan struct{})
	finish := make(chan struct{})
	attachment, err := api.NewAttachment(ctx, api.AttachmentConfig{
		Sessions: []*api.Session{session},
		Shutdown: func() error {
			close(started)
			<-finish
			return nil
		},
	})
	require.NoError(t, err)
	closing, stop := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- attachment.CloseContext(closing) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("shutdown callback did not start")
	}
	stop()
	require.ErrorIs(t, <-result, context.Canceled)
	close(finish)
	require.NoError(t, attachment.Wait(ctx))
	require.NoError(t, attachment.Close())
}

func TestAttachmentLifetimeCancellationAndImmediateClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("example", &api.RealmConfig{}))
	for _, cancelLifetime := range []bool{false, true} {
		lifetime, stop := context.WithCancel(ctx)
		session, connectErr := api.ConnectInMemory(router, "example")
		require.NoError(t, connectErr)
		attachment, createErr := api.NewAttachment(lifetime, api.AttachmentConfig{Sessions: []*api.Session{session}})
		require.NoError(t, createErr)
		if cancelLifetime {
			stop()
			require.NoError(t, attachment.Wait(ctx))
		} else {
			require.NoError(t, attachment.CloseContext(ctx))
			stop()
		}
	}
}
