package xconn

import (
	"context"
	"fmt"
	"sync"
)

// CallerIdentity is the authenticated identity disclosed by the source router.
type CallerIdentity struct {
	SessionID uint64
	AuthID    string
	AuthRole  string
}

// BridgeRoute describes a forwarded registration. Options use WAMP registration semantics.
type BridgeRoute struct {
	Procedure string
	Options   Options
}

// BridgeConfig forwards calls to Target or to an owned session for each caller.
// NewSession is application policy; routing, cancellation and cleanup run in Rust.
type BridgeConfig struct {
	Target     *Session
	Routes     []BridgeRoute
	NewSession func(context.Context, CallerIdentity) (*Session, error)
}

type Bridge struct {
	handle    uint64
	callbacks *foreignCallbacks
	cancel    context.CancelFunc
	once      sync.Once
	err       error
}

func (s *Session) Bridge(ctx context.Context, config BridgeConfig) (*Bridge, error) {
	if config.Target == nil {
		return nil, fmt.Errorf("bridge target is required")
	}
	lifetime, cancel := context.WithCancel(ctx)
	fields := map[string]any{"source": s.handle, "target": config.Target.handle, "routes": config.Routes}
	var callbacks *foreignCallbacks
	if config.NewSession != nil {
		var err error
		callbacks, err = newForeignCallbacks(lifetime, func(method string, value any) (any, error) {
			if method != "bridge_session" {
				return nil, unexpectedCallback(method)
			}
			identity := nativeFields(value)
			session, err := config.NewSession(lifetime, CallerIdentity{
				SessionID: nativeUint64(identity["session_id"]),
				AuthID:    stringField(identity, "authid"), AuthRole: stringField(identity, "authrole"),
			})
			if err != nil {
				return nil, err
			}
			if session == nil {
				return nil, fmt.Errorf("bridge session factory returned nil")
			}
			if err = lifetime.Err(); err != nil {
				_ = session.Close()
				return nil, err
			}
			go func() { <-session.Done(); _ = session.Close() }()
			return map[string]any{"handle": session.handle}, nil
		})
		if err != nil {
			cancel()
			return nil, err
		}
		fields["callback"] = callbacks.handle
	}
	// Keep protocol field names independent of Go's struct field spelling.
	routes := make([]any, len(config.Routes))
	for index, route := range config.Routes {
		routes[index] = map[string]any{"procedure": route.Procedure, "options": route.Options}
	}
	fields["routes"] = routes
	value, err := invoke(lifetime, "service_bridge_new", fields)
	if err != nil {
		cancel()
		if callbacks != nil {
			callbacks.close()
		}
		return nil, err
	}
	bridge := &Bridge{handle: nativeUint64(nativeFields(value)["handle"]), callbacks: callbacks, cancel: cancel}
	go func() {
		select {
		case <-lifetime.Done():
		case <-s.Done():
		}
		_ = bridge.CloseContext(context.WithoutCancel(ctx))
	}()
	return bridge, nil
}

func (b *Bridge) Close() error { return b.CloseContext(context.Background()) }

func (b *Bridge) CloseContext(ctx context.Context) error {
	b.once.Do(func() {
		b.cancel()
		if b.callbacks != nil {
			b.callbacks.close()
		}
		_, b.err = invoke(ctx, "service_bridge_close", map[string]any{"handle": b.handle})
		closeHandle(b.handle)
	})
	return b.err
}
