package xconn_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/xconnio/xconn-core/bindings/go"
	"github.com/xconnio/xconn-core/bindings/go/serializers"
)

func TestGenericDiscoveryAuthorizationOwnershipAndPresence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	roles := []api.RealmRole{}
	for _, role := range []string{"infrastructure", "research", "other"} {
		roles = append(
			roles,
			api.RealmRole{
				Name: role,
				Permissions: []api.Permission{
					{URI: "", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true, AllowSubscribe: true},
				},
			},
		)
	}
	require.NoError(t, router.AddRealm("lab", &api.RealmConfig{AutoDiscloseCaller: true, Meta: true, Roles: roles}))
	connect := func(id, role string) *api.Session {
		base, err := api.ConnectInMemoryBase(router, "lab", id, role, &serializers.CBORSerializer{}, 16)
		require.NoError(t, err)
		session, err := api.NewSession(base, base.Serializer())
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })
		return session
	}
	host := connect("directory", "infrastructure")
	service, err := host.ServeDiscovery(
		ctx,
		"lab.endpoints",
		func(caller api.CallerIdentity, _ api.DiscoveryAction, endpoint api.Endpoint) bool {
			return endpoint.Metadata["group"] == caller.AuthRole
		},
	)
	require.NoError(t, err)
	defer service.Close()
	owner := connect("sensor-controller", "research")
	reader := connect("dashboard", "research")
	outsider := connect("outsider", "other")
	directory := owner.Discovery("lab.endpoints")
	endpoint := api.Endpoint{
		ID:           "sensor-42",
		Addresses:    []string{"quic://sensor.example:443"},
		Capabilities: []string{"telemetry"},
		Metadata:     map[string]any{"group": "research"},
	}
	require.NoError(t, directory.Advertise(ctx, endpoint))
	found, err := reader.Discovery("lab.endpoints").Get(ctx, endpoint.ID)
	require.NoError(t, err)
	require.Equal(t, endpoint, found)
	hidden, err := outsider.Discovery("lab.endpoints").List(ctx)
	require.NoError(t, err)
	require.Empty(t, hidden)
	_, err = outsider.Discovery("lab.endpoints").Get(ctx, endpoint.ID)
	require.Error(t, err)
	require.Error(
		t,
		reader.Discovery("lab.endpoints").Advertise(ctx, endpoint),
		"another identity must not take ownership",
	)
	require.Error(t, reader.Discovery("lab.endpoints").Withdraw(ctx, endpoint.ID))
	require.NoError(t, owner.Close())
	require.Eventually(t, func() bool {
		endpoints, err := reader.Discovery("lab.endpoints").List(ctx)
		return err == nil && len(endpoints) == 0
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, service.Close())
	_, err = reader.Discovery("lab.endpoints").List(ctx)
	require.Error(t, err, "closing a directory must release its registrations")
}

func TestGenericAttachmentRetriesDetachesAndCancels(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var attempts, failures atomic.Int32
	err := api.RunAttachment(ctx, api.AttachmentRetryPolicy{Initial: time.Millisecond, Maximum: 2 * time.Millisecond},
		func(context.Context) (api.AttachmentOutcome, error) {
			if attempts.Add(1) == 3 {
				return api.AttachmentDetached, nil
			}
			return api.AttachmentRetry, context.DeadlineExceeded
		}, func(error) { failures.Add(1) })
	require.NoError(t, err)
	require.EqualValues(t, 3, attempts.Load())
	require.EqualValues(t, 2, failures.Load())
	running, stop := context.WithCancel(ctx)
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		cycle := func(ctx context.Context) (api.AttachmentOutcome, error) {
			close(started)
			<-ctx.Done()
			return api.AttachmentRetry, ctx.Err()
		}
		result <- api.RunAttachment(running, api.AttachmentRetryPolicy{}, cycle, nil)
	}()
	<-started
	stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("attachment cancellation leaked its callback worker")
	}
}
