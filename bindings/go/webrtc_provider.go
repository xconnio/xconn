package xconn

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// WebRTCProvider accepts WAMP sessions into a router and exposes independent raw channels.
type WebRTCProvider struct {
	openTimeout   time.Duration
	mu            sync.Mutex
	handle        uint64
	started       bool
	closed        bool
	cancel        context.CancelFunc
	done          chan struct{}
	incoming      chan *WebRTCDataChannel
	onDataChannel func(string, *WebRTCDataChannel, []byte)
	callbacks     *foreignCallbacks
}

func NewWebRTCHandler() *WebRTCProvider {
	return &WebRTCProvider{done: make(chan struct{}), incoming: make(chan *WebRTCDataChannel, 64)}
}

func (p *WebRTCProvider) Setup(config *WebRTCProviderConfig) error {
	return p.SetupContext(context.Background(), config)
}

func (p *WebRTCProvider) SetupContext(ctx context.Context, config *WebRTCProviderConfig) error {
	if config == nil || config.Session == nil || config.Router == nil {
		return fmt.Errorf("WebRTC provider config, signaling session, and router are required")
	}

	fields, err := webRTCFields(config.ProcedureHandleOffer, config.TopicHandleRemoteCandidates,
		config.TopicPublishLocalCandidate, config.ICEServers, config.ConnectTimeout,
		config.OpenTimeout, config.HandshakeTimeout)
	if err != nil {
		return err
	}
	// Serialize setup/close so a successful setup cannot outlive a concurrent Close.
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started || p.closed {
		return fmt.Errorf("WebRTC provider is already started or closed")
	}

	fields["signaling"], fields["router"] = config.Session.handle, config.Router.handle

	if config.Authenticator != nil {
		server := NewServer(config.Router, config.Authenticator, nil)
		callbacks, err := newForeignCallbacks(ctx, server.authenticate)
		if err != nil {
			return err
		}

		p.callbacks = callbacks
		methods := config.Authenticator.Methods()
		names := make([]string, len(methods))

		for i, method := range methods {
			names[i] = string(method)
		}

		fields["server_auth"] = map[string]any{"callback": callbacks.handle, "methods": names}
	}

	value, err := invoke(ctx, "webrtc_provider_start", fields)
	if err != nil {
		if p.callbacks != nil {
			p.callbacks.close()
			p.callbacks = nil
		}

		return err
	}

	p.openTimeout = config.OpenTimeout

	if p.openTimeout == 0 {
		p.openTimeout = 20 * time.Second
	}

	p.handle = nativeUint64(nativeFields(value)["handle"])
	p.started = true
	workerContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.cancel = cancel
	go p.receive(workerContext)
	return nil
}

func (p *WebRTCProvider) receive(ctx context.Context) {
	defer close(p.done)
	defer close(p.incoming)

	for {
		fields := map[string]any{"handle": p.handle, "open_timeout": p.openTimeout.Seconds()}
		value, err := invoke(ctx, "webrtc_provider_accept", fields)
		if err != nil || value == nil {
			return
		}

		fields = nativeFields(value)
		channel := newWebRTCDataChannel(ctx, fields)
		first, _ := fields["first_packet"].([]byte)
		p.mu.Lock()
		callback := p.onDataChannel
		p.mu.Unlock()

		if callback != nil {
			go callback(stringField(fields, "connection_id"), channel, first)
			continue
		}

		if first != nil {
			_, err = invoke(
				ctx,
				"webrtc_channel_unread",
				map[string]any{"handle": channel.handle, "data": first, "is_string": fields["first_is_string"]},
			)

			if err != nil {
				_ = channel.CloseContext(context.WithoutCancel(ctx))
				continue
			}
		}

		select {
		case p.incoming <- channel:
		case <-ctx.Done():
			_ = channel.CloseContext(context.WithoutCancel(ctx))
			return
		}
	}
}

// OnDataChannel keeps the old callback shape, replacing Pion's channel with a Rust-backed channel.
// It applies to subsequently accepted channels. Use either this callback or AcceptChannel.
func (p *WebRTCProvider) OnDataChannel(callback func(string, *WebRTCDataChannel, []byte)) {
	p.mu.Lock()
	p.onDataChannel = callback
	p.mu.Unlock()
}

func (p *WebRTCProvider) AcceptChannel(ctx context.Context) (*WebRTCDataChannel, error) {
	p.mu.Lock()
	started, closed := p.started, p.closed
	p.mu.Unlock()

	if !started || closed {
		return nil, io.EOF
	}

	select {
	case channel, ok := <-p.incoming:
		if !ok {
			return nil, io.EOF
		}

		return channel, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// UpdateICEServers applies to future connections. Active connections are unchanged.
func (p *WebRTCProvider) UpdateICEServers(servers []WebRTCICEServer) error {
	ice, err := webRTCICEFields(servers)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started || p.closed {
		return fmt.Errorf("WebRTC provider is not running")
	}

	_, err = invoke(context.Background(), "webrtc_provider_ice", map[string]any{"handle": p.handle, "ice_servers": ice})
	return err
}

func (p *WebRTCProvider) Close() error {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil
	}

	p.closed = true
	started, handle, cancel, callbacks := p.started, p.handle, p.cancel, p.callbacks
	p.mu.Unlock()

	if !started {
		return nil
	}

	cancel()
	_, err := invoke(context.Background(), "webrtc_provider_close", map[string]any{"handle": handle})
	closeHandle(handle)
	<-p.done

	for channel := range p.incoming {
		_ = channel.Close()
	}

	if callbacks != nil {
		callbacks.close()
	}

	return err
}
