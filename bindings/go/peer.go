package xconn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xconnio/xconn-core/bindings/go/auth"
)

// DirectConfig configures direct connections to the single edge serving the hub realm.
// Authentication is independent of the hub login. Nil ICE servers use Google STUN;
// an explicitly empty slice disables external ICE servers. No TURN is configured by default.
type DirectConfig struct {
	Authenticator  auth.ClientAuthenticator
	Serializer     SerializerSpec
	ICEServers     []WebRTCICEServer
	ConnectTimeout time.Duration
	RetryInterval  time.Duration
}

// peerManager borrows a hub session and owns direct connections. Calls select one path
// when executed; dispatched calls and active streams are never replayed or moved.
// Keep subscriptions, publications and registrations on the original hub session.
type peerManager struct {
	hub       *Session
	handle    uint64
	callbacks *foreignCallbacks
	done      chan struct{}
	mu        sync.Mutex
	sessions  map[uint64]*Session
	once      sync.Once
}

// startPeer only marshals configuration and owns native resource handles.
func (s *Session) startPeer(ctx context.Context, config *DirectConfig) (*peerManager, error) {
	client := &Client{Authenticator: config.Authenticator, SerializerSpec: config.Serializer}
	if err := client.validate(); err != nil {
		return nil, err
	}
	fields := client.fields()
	callbacks, _, err := client.configureCallbacks(ctx, fields)
	if err != nil {
		return nil, err
	}
	fields["session"] = s.handle
	if config.ConnectTimeout != 0 {
		fields["connect_timeout"] = config.ConnectTimeout.Seconds()
	}
	if config.RetryInterval != 0 {
		fields["retry_interval"] = config.RetryInterval.Seconds()
	}
	if config.ICEServers != nil {
		ice, err := webRTCICEFields(config.ICEServers)
		if err != nil {
			if callbacks != nil {
				callbacks.close()
			}
			return nil, err
		}
		fields["ice_servers"] = ice
	}
	value, err := invoke(ctx, "webrtc_peer_start", fields)
	if err != nil {
		if callbacks != nil {
			callbacks.close()
		}
		return nil, err
	}
	if value == nil {
		if callbacks != nil {
			callbacks.close()
		}
		return nil, nil
	}
	p := &peerManager{hub: s, handle: nativeUint64(nativeFields(value)["handle"]), callbacks: callbacks,
		done: make(chan struct{}), sessions: make(map[uint64]*Session)}
	go func() {
		_, _ = invoke(context.WithoutCancel(ctx), "webrtc_peer_wait", map[string]any{"handle": p.handle})
		close(p.done)
		p.close()
	}()
	return p, nil
}

func (p *peerManager) selectSession(ctx context.Context, procedure string) (*Session, error) {
	value, err := invoke(ctx, "webrtc_peer_select", map[string]any{"handle": p.handle, "procedure": procedure})
	if err != nil {
		return nil, err
	}
	fields := nativeFields(value)
	handle := nativeUint64(fields["handle"])
	if handle == p.hub.handle {
		return p.hub, nil
	}
	p.mu.Lock()
	session := p.sessions[handle]
	if session == nil {
		session = newSession(ctx, fields)
		p.sessions[handle] = session
		go func() {
			<-session.Done()
			p.mu.Lock()
			delete(p.sessions, handle)
			p.mu.Unlock()
			session.cancel() // The native manager owns this borrowed transport.
		}()
	}
	p.mu.Unlock()
	return session, nil
}

// ConnectionPath describes the path available for new calls.
type ConnectionPath string

const (
	PathRouted ConnectionPath = "routed"
	PathDirect ConnectionPath = "direct"
	PathClosed ConnectionPath = "closed"
)

func (s *Session) Path() ConnectionPath {
	if s.peer == nil {
		if s.Connected() {
			return PathRouted
		}
		return PathClosed
	}
	value, err := invoke(context.Background(), "webrtc_peer_status", map[string]any{"handle": s.peer.handle})
	if err != nil {
		return PathClosed
	}
	return ConnectionPath(stringField(nativeFields(value), "path"))
}

// DirectError returns the last failed direct connection attempt, or nil after
// a successful negotiation. Routed calls remain available after these errors.
func (s *Session) DirectError() error {
	if s.peer == nil {
		return nil
	}
	p := s.peer
	value, err := invoke(context.Background(), "webrtc_peer_error", map[string]any{"handle": p.handle})
	if err != nil {
		return err
	}
	if message, ok := value.(string); ok {
		return fmt.Errorf("%s", message)
	}
	return nil
}

func (p *peerManager) close() {
	p.once.Do(func() {
		if p.callbacks != nil {
			p.callbacks.close()
		}
		closeHandle(p.handle)
	})
	<-p.done
}

// PeerEndpoint advertises and accepts direct sessions in its signaling realm.
// The supplied router must already expose the same procedures as the hub path.
type PeerEndpoint struct {
	handle    uint64
	callbacks *foreignCallbacks
	done      chan struct{}
	once      sync.Once
	err       error
}

// ExposeConfig configures direct authentication and optional ICE servers.
// Nil ICE servers use Google STUN; an empty slice disables external servers.
type ExposeConfig struct {
	Authenticator auth.ServerAuthenticator
	ICEServers    []WebRTCICEServer
}

// Expose advertises one direct endpoint in this realm using Rust's defaults.
func (s *Session) Expose(ctx context.Context, router *Router, config *ExposeConfig) (*PeerEndpoint, error) {
	if router == nil {
		return nil, fmt.Errorf("local router is required")
	}
	options := ExposeConfig{}
	if config != nil {
		options = *config
	}
	fields := map[string]any{"session": s.handle, "router": router.handle}
	if options.ICEServers != nil {
		ice, err := webRTCICEFields(options.ICEServers)
		if err != nil {
			return nil, err
		}
		fields["ice_servers"] = ice
	}
	var callbacks *foreignCallbacks
	if options.Authenticator != nil {
		server := NewServer(router, options.Authenticator, nil)
		var err error
		callbacks, err = newForeignCallbacks(ctx, server.authenticate)
		if err != nil {
			return nil, err
		}
		methods := options.Authenticator.Methods()
		names := make([]string, len(methods))
		for i, method := range methods {
			names[i] = string(method)
		}
		fields["server_auth"] = map[string]any{"callback": callbacks.handle, "methods": names}
	}
	value, err := invoke(ctx, "webrtc_peer_expose", fields)
	if err != nil {
		if callbacks != nil {
			callbacks.close()
		}
		return nil, err
	}
	endpoint := &PeerEndpoint{
		handle: nativeUint64(nativeFields(value)["handle"]), callbacks: callbacks, done: make(chan struct{}),
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-s.Done():
		case <-endpoint.done:
		}
		_ = endpoint.CloseContext(context.WithoutCancel(ctx))
	}()
	return endpoint, nil
}

func (e *PeerEndpoint) Close() error { return e.CloseContext(context.Background()) }

func (e *PeerEndpoint) CloseContext(ctx context.Context) error {
	e.once.Do(func() {
		_, e.err = invoke(ctx, "webrtc_peer_endpoint_close", map[string]any{"handle": e.handle})
		closeHandle(e.handle)
		if e.callbacks != nil {
			e.callbacks.close()
		}
		close(e.done)
	})
	return e.err
}
