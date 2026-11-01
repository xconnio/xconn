package xconn_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/xconnio/xconn-core/bindings/go"
)

func TestBridgeCallerInitializationDoesNotBlockOtherCallers(t *testing.T) {
	for _, cached := range []bool{false, true} {
		name := "new caller"
		if cached {
			name = "cached caller"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			router, err := api.NewRouter(nil)
			require.NoError(t, err)
			defer router.Close()
			require.NoError(t, router.AddRealm("source", &api.RealmConfig{AutoDiscloseCaller: true, Meta: true}))
			require.NoError(t, router.AddRealm("target", nil))
			connect := func(realm string) *api.Session {
				session, err := api.ConnectInMemory(router, realm)
				require.NoError(t, err)
				t.Cleanup(func() { _ = session.Close() })
				return session
			}
			source, target := connect("source"), connect("target")
			first, second := connect("source"), connect("source")
			require.NoError(t, target.Register("app.echo", func(context.Context, *api.Invocation) *api.Result {
				return api.NewResult("ok")
			}).ExecuteContext(ctx).Err)
			started, release := make(chan struct{}), make(chan struct{})
			bridge, err := source.Bridge(ctx, api.BridgeConfig{
				Target: target, Routes: []api.BridgeRoute{{Procedure: "app.echo"}},
				NewSession: func(ctx context.Context, caller api.CallerIdentity) (*api.Session, error) {
					if caller.SessionID == first.ID() {
						close(started)
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return api.ConnectInMemory(router, "target") //nolint:contextcheck // Constructor has no context API.
				},
			})
			require.NoError(t, err)
			defer bridge.Close()
			if cached {
				require.NoError(t, second.Call("app.echo").ExecuteContext(ctx).Err)
			}
			finished := make(chan *api.Result, 1)
			go func() { finished <- first.Call("app.echo").ExecuteContext(ctx) }()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("first caller never reached its factory")
			}
			independent, stop := context.WithTimeout(ctx, time.Second)
			reply := second.Call("app.echo").ExecuteContext(independent)
			stop()
			close(release)
			require.NoError(t, reply.Err, "a different caller must progress while the first factory is blocked")
			require.Equal(t, "ok", reply.ArgStringOr(0, ""))
			select {
			case result := <-finished:
				require.NoError(t, result.Err)
			case <-ctx.Done():
				t.Fatal("first call did not complete after factory release")
			}
		})
	}
}
