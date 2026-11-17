package xconn_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn/bindings/go"
	"github.com/xconnio/xconn/bindings/go/auth"
)

type blockingAuthenticator struct {
	started chan struct{}
	release chan struct{}
}

func (a *blockingAuthenticator) Methods() []auth.Method {
	return []auth.Method{auth.Ticket}
}

func (a *blockingAuthenticator) Authenticate(request auth.Request) (auth.Response, error) {
	if request.AuthID() == "blocked" {
		close(a.started)
		<-a.release
	}

	return auth.NewResponse(request.AuthID(), "trusted", 0)
}

func TestAuthenticationDoesNotBlockOtherConnections(t *testing.T) {
	const realm = "authentication"

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	require.NoError(t, router.AddRealm(realm, nil))

	policy := &blockingAuthenticator{started: make(chan struct{}), release: make(chan struct{})}
	server := xconn.NewServer(router, policy, nil)
	defer server.Close()
	listener, err := server.ListenAndServeWebSocket(xconn.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	release := sync.OnceFunc(func() { close(policy.release) })
	defer release()

	uri := "ws://" + listener.Addr().String()
	finished := make(chan error, 1)
	go func() {
		session, connectErr := xconn.ConnectTicket(ctx, uri, realm, "blocked", "secret")
		if session != nil {
			_ = session.Close()
		}
		finished <- connectErr
	}()

	select {
	case <-policy.started:
	case <-ctx.Done():
		t.Fatal("first connection did not reach authentication")
	}

	session, err := xconn.ConnectTicket(ctx, uri, realm, "independent", "secret")
	require.NoError(t, err, "a blocked authentication must not prevent another connection")
	defer session.Close()

	release()
	select {
	case err = <-finished:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("first connection did not finish after releasing authentication")
	}
}
