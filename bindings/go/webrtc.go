package xconn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xconnio/xconn-core/bindings/go/auth"
)

type WebRTCCredentialType string

const (
	WebRTCICECredentialTypePassword WebRTCCredentialType = "password"
	WebRTCICECredentialTypeOauth    WebRTCCredentialType = "oauth"
)

// WebRTCICEServer describes a STUN or TURN server. OAuth credentials are not supported by the Rust backend.
type WebRTCICEServer struct {
	URLs           []string
	Username       string
	Credential     any
	CredentialType WebRTCCredentialType
}

type WebRTCClientConfig struct {
	Realm                    string
	ProcedureWebRTCOffer     string
	TopicAnswererOnCandidate string
	TopicOffererOnCandidate  string
	ConnectTimeout           time.Duration
	OpenTimeout              time.Duration
	HandshakeTimeout         time.Duration
	Serializer               SerializerSpec
	Authenticator            auth.ClientAuthenticator
	Session                  *Session
	ICEServers               []WebRTCICEServer
	OnDisconnect             func()
}

type WebRTCOpenSessionConfig struct {
	Serializer    SerializerSpec
	Authenticator auth.ClientAuthenticator
	OpenTimeout   time.Duration
}

type WebRTCProviderConfig struct {
	Session                     *Session
	ProcedureHandleOffer        string
	TopicHandleRemoteCandidates string
	TopicPublishLocalCandidate  string
	Router                      *Router
	Authenticator               auth.ServerAuthenticator
	ICEServers                  []WebRTCICEServer
	ConnectTimeout              time.Duration
	OpenTimeout                 time.Duration
	HandshakeTimeout            time.Duration
}

func webRTCFields(offer, remote, local string, servers []WebRTCICEServer,
	connectTimeout, openTimeout, handshakeTimeout time.Duration) (map[string]any, error) {
	if offer == "" || remote == "" || local == "" {
		return nil, fmt.Errorf("WebRTC offer procedure and candidate topics are required")
	}

	ice, err := webRTCICEFields(servers)
	if err != nil {
		return nil, err
	}

	fields := map[string]any{
		"offer_procedure": offer, "remote_candidates_topic": remote,
		"local_candidates_topic": local, "ice_servers": ice,
	}

	for key, duration := range map[string]time.Duration{
		"connect_timeout": connectTimeout, "open_timeout": openTimeout, "handshake_timeout": handshakeTimeout,
	} {
		if duration < 0 {
			return nil, fmt.Errorf("%s must not be negative", key)
		}

		if duration == 0 {
			duration = 20 * time.Second
		}

		fields[key] = duration.Seconds()
	}

	return fields, nil
}

func webRTCICEFields(servers []WebRTCICEServer) ([]any, error) {
	fields := make([]any, len(servers))

	for i, server := range servers {
		if server.CredentialType != "" && server.CredentialType != WebRTCICECredentialTypePassword {
			return nil, fmt.Errorf("unsupported ICE credential type %q", server.CredentialType)
		}

		credential := ""
		if server.Credential != nil {
			var ok bool
			credential, ok = server.Credential.(string)

			if !ok {
				return nil, fmt.Errorf("ICE password credential must be a string")
			}
		}

		if len(server.URLs) == 0 {
			return nil, fmt.Errorf("ICE server URLs must not be empty")
		}

		fields[i] = map[string]any{"urls": server.URLs, "username": server.Username, "credential": credential}
	}

	return fields, nil
}

// ConnectWebRTC retains the blocking ConnectWAMP behavior. Use ConnectWebRTCContext for cancellation.
func ConnectWebRTC(config *WebRTCClientConfig) (*WebRTCSession, error) {
	return ConnectWebRTCContext(context.Background(), config)
}

func ConnectWebRTCContext(ctx context.Context, config *WebRTCClientConfig) (*WebRTCSession, error) {
	if config == nil || config.Session == nil || config.Realm == "" {
		return nil, fmt.Errorf("WebRTC config, signaling session, and realm are required")
	}

	fields, err := webRTCFields(config.ProcedureWebRTCOffer, config.TopicOffererOnCandidate,
		config.TopicAnswererOnCandidate, config.ICEServers, config.ConnectTimeout, config.OpenTimeout,
		config.HandshakeTimeout)
	if err != nil {
		return nil, err
	}

	fields["signaling"], fields["realm"] = config.Session.handle, config.Realm
	client := &Client{SerializerSpec: config.Serializer, Authenticator: config.Authenticator}
	session, err := client.connect(ctx, "webrtc_connect", fields)
	if err != nil {
		return nil, err
	}

	connection := newWebRTCConnection(ctx, session.connectionHandle, config.OpenTimeout)
	if callback := config.OnDisconnect; callback != nil {
		go func() {
			<-connection.Done()
			callback()
		}()
	}

	return &WebRTCSession{Session: session, connection: connection}, nil
}

// WebRTCSession embeds the existing WAMP API; its Close leaves sibling channels open.
type WebRTCSession struct {
	*Session
	connection *WebRTCConnection
}

func (s *WebRTCSession) Connection() *WebRTCConnection { return s.connection }

func (s *WebRTCSession) Close() error { return s.CloseContext(context.Background()) }

func (s *WebRTCSession) CloseContext(ctx context.Context) error { return s.LeaveContext(ctx) }

func (s *WebRTCSession) OpenSession(realm string, config *WebRTCOpenSessionConfig) (*WebRTCSession, error) {
	return s.OpenSessionContext(context.Background(), realm, config)
}

func (s *WebRTCSession) OpenSessionContext(ctx context.Context, realm string,
	config *WebRTCOpenSessionConfig) (*WebRTCSession, error) {
	if realm == "" {
		return nil, fmt.Errorf("realm must not be empty")
	}

	if config == nil {
		config = &WebRTCOpenSessionConfig{}
	}

	if config.OpenTimeout < 0 {
		return nil, fmt.Errorf("open timeout must not be negative")
	}

	timeout := config.OpenTimeout
	if timeout == 0 {
		timeout = s.connection.openTimeout
	}

	fields := map[string]any{"handle": s.connection.handle, "realm": realm}
	if timeout > 0 {
		fields["open_timeout"] = timeout.Seconds()
	}

	session, err := (&Client{SerializerSpec: config.Serializer, Authenticator: config.Authenticator}).connect(
		ctx, "webrtc_open_session", fields)
	if err != nil {
		return nil, err
	}

	return &WebRTCSession{Session: session, connection: s.connection}, nil
}

func (s *WebRTCSession) OpenChannel(label string, options *WebRTCDataChannelInit) (*WebRTCDataChannel, error) {
	return s.connection.OpenChannel(label, options)
}

func (s *WebRTCSession) OnDataChannel(callback func(*WebRTCDataChannel)) {
	s.connection.OnDataChannel(callback)
}

// WebRTCConnection is a Rust-owned peer connection, not a Pion PeerConnection.
type WebRTCConnection struct {
	handle        uint64
	openTimeout   time.Duration
	done          chan struct{}
	once          sync.Once
	err           error
	callbackMu    sync.Mutex
	onDataChannel func(*WebRTCDataChannel)
	receiveOnce   sync.Once
}

func newWebRTCConnection(ctx context.Context, handle uint64, timeout time.Duration) *WebRTCConnection {
	if timeout == 0 {
		timeout = 20 * time.Second
	}

	c := &WebRTCConnection{handle: handle, openTimeout: timeout, done: make(chan struct{})}
	go func() {
		_, _ = invoke(context.WithoutCancel(ctx), "webrtc_connection_wait", map[string]any{"handle": handle})
		close(c.done)
	}()
	return c
}

func (c *WebRTCConnection) Done() <-chan struct{} { return c.done }

func (c *WebRTCConnection) Close() error { return c.CloseContext(context.Background()) }

func (c *WebRTCConnection) CloseContext(ctx context.Context) error {
	c.once.Do(func() {
		_, c.err = invoke(ctx, "webrtc_connection_close", map[string]any{"handle": c.handle})
		closeHandle(c.handle)
	})
	return c.err
}

// WebRTCDataChannelInit mirrors the Pion channel settings with Rust-backed values.
type WebRTCDataChannelInit struct {
	Ordered           *bool
	MaxPacketLifeTime *uint16
	MaxRetransmits    *uint16
	Protocol          *string
	Negotiated        *bool
	ID                *uint16
}

func (o *WebRTCDataChannelInit) fields() (map[string]any, error) {
	f := map[string]any{}
	if o == nil {
		return f, nil
	}

	if o.Protocol != nil && *o.Protocol != "" && *o.Protocol != "xconn.raw" {
		return nil, fmt.Errorf("raw channel protocol must be xconn.raw")
	}

	if o.Ordered != nil {
		f["ordered"] = *o.Ordered
	}

	if o.MaxPacketLifeTime != nil {
		f["max_packet_life_time"] = *o.MaxPacketLifeTime
	}

	if o.MaxRetransmits != nil {
		f["max_retransmits"] = *o.MaxRetransmits
	}

	if o.Negotiated != nil && *o.Negotiated {
		if o.ID == nil {
			return nil, fmt.Errorf("negotiated channels require an ID")
		}

		f["negotiated"] = *o.ID
	} else if o.ID != nil {
		return nil, fmt.Errorf("an explicit channel ID requires Negotiated=true")
	}

	return f, nil
}

func (c *WebRTCConnection) OpenChannel(label string, options *WebRTCDataChannelInit) (*WebRTCDataChannel, error) {
	return c.OpenChannelContext(context.Background(), label, options)
}

func (c *WebRTCConnection) OpenChannelContext(ctx context.Context, label string,
	options *WebRTCDataChannelInit) (*WebRTCDataChannel, error) {
	f, err := options.fields()
	if err != nil {
		return nil, err
	}

	fields := map[string]any{"handle": c.handle, "label": label, "options": f}
	if c.openTimeout > 0 {
		fields["open_timeout"] = c.openTimeout.Seconds()
	}

	result, err := invoke(ctx, "webrtc_channel_open", fields)
	if err != nil {
		return nil, err
	}

	return newWebRTCDataChannel(ctx, nativeFields(result)), nil
}

func (c *WebRTCConnection) AcceptChannel(ctx context.Context) (*WebRTCDataChannel, error) {
	result, err := invoke(ctx, "webrtc_channel_accept", map[string]any{"handle": c.handle})
	if err != nil {
		return nil, err
	}

	return newWebRTCDataChannel(ctx, nativeFields(result)), nil
}

// OnDataChannel consumes incoming channels. Use either this callback or AcceptChannel.
func (c *WebRTCConnection) OnDataChannel(callback func(*WebRTCDataChannel)) {
	c.callbackMu.Lock()
	c.onDataChannel = callback
	c.callbackMu.Unlock()

	if callback == nil {
		return
	}

	c.receiveOnce.Do(func() {
		go func() {
			for {
				channel, err := c.AcceptChannel(context.Background())
				if err != nil {
					return
				}

				c.callbackMu.Lock()
				receiver := c.onDataChannel
				c.callbackMu.Unlock()

				if receiver != nil {
					go receiver(channel)
				} else {
					_ = channel.Close()
				}
			}
		}()
	})
}

// SelectedRemoteAddress returns the nominated ICE peer or TURN relay IP.
// An empty address means ICE has not selected a candidate pair yet.
func (c *WebRTCConnection) SelectedRemoteAddress(ctx context.Context) (string, error) {
	value, err := invoke(ctx, "webrtc_remote_address", map[string]any{"handle": c.handle})
	if err != nil {
		return "", err
	}
	address, _ := value.(string)
	return address, nil
}
