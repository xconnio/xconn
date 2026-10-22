package xconn_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn-core/bindings/go"
	"github.com/xconnio/xconn-core/bindings/go/messages"
)

type policyFunc func(xconn.BaseSession, messages.Message) (bool, error)

func (f policyFunc) Authorize(s xconn.BaseSession, m messages.Message) (bool, error) { return f(s, m) }

func TestRealmPolicyLifetime(t *testing.T) {
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	allow := policyFunc(func(xconn.BaseSession, messages.Message) (bool, error) { return true, nil })
	require.NoError(t, router.AddRealm("realm", &xconn.RealmConfig{Authorizer: allow}))
	original := xconn.RealmPolicyDone(router, "realm")
	require.NoError(t, router.AddRealmAlias("realm", "alias"))
	router.RemoveRealm("realm")
	select {
	case <-original:
		t.Fatal("alias lost its authorizer")
	default:
	}
	require.NoError(t, router.SetRealmAuthorizer("alias", allow))
	select {
	case <-original:
	case <-time.After(time.Second):
		t.Fatal("replaced policy retained")
	}
	replacement := xconn.RealmPolicyDone(router, "alias")
	router.RemoveRealm("alias")
	select {
	case <-replacement:
	case <-time.After(time.Second):
		t.Fatal("removed policy retained")
	}
	require.NoError(t, router.AddRealm("alias", &xconn.RealmConfig{Authorizer: allow}))
	recreated := xconn.RealmPolicyDone(router, "alias")
	router.Close()
	select {
	case <-recreated:
	case <-time.After(time.Second):
		t.Fatal("router retained policy")
	}
	require.Error(t, router.AddRealm("closed", nil))
}

func TestAuthorizerCanReplaceItself(t *testing.T) {
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	replaced := make(chan error, 1)
	allow := policyFunc(func(xconn.BaseSession, messages.Message) (bool, error) { return true, nil })
	replace := policyFunc(func(xconn.BaseSession, messages.Message) (bool, error) {
		replaced <- router.SetRealmAuthorizer("realm", allow)
		return true, nil
	})
	require.NoError(t, router.AddRealm("realm", &xconn.RealmConfig{Authorizer: replace}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := router.Connect(ctx, "json")
	require.NoError(t, err)
	defer session.Close()
	// Replacing the bridge cancels its in-flight authorization; it must not deadlock.
	_ = session.Publish("policy.replace").Options(map[string]any{"acknowledge": true}).ExecuteContext(ctx)
	select {
	case err = <-replaced:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("authorizer replacement deadlocked")
	}
	require.NoError(
		t,
		session.Publish("policy.replaced").Options(map[string]any{"acknowledge": true}).ExecuteContext(ctx).Err,
	)
}
