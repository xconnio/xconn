// Package webrtc runs WAMP and raw data channels over the shared Rust WebRTC implementation.
package webrtc

import (
	"context"

	xconn "github.com/xconnio/xconn-core/bindings/go"
)

type (
	ClientConfig       = xconn.WebRTCClientConfig
	ProviderConfig     = xconn.WebRTCProviderConfig
	OpenSessionConfig  = xconn.WebRTCOpenSessionConfig
	WebRTCSession      = xconn.WebRTCSession
	WebRTCProvider     = xconn.WebRTCProvider
	Connection         = xconn.WebRTCConnection
	DataChannel        = xconn.WebRTCDataChannel
	DataChannelInit    = xconn.WebRTCDataChannelInit
	DataChannelMessage = xconn.WebRTCDataChannelMessage
	ICEServer          = xconn.WebRTCICEServer
	CredentialType     = xconn.WebRTCCredentialType
)

const (
	ICECredentialTypePassword = xconn.WebRTCICECredentialTypePassword
	ICECredentialTypeOauth    = xconn.WebRTCICECredentialTypeOauth
)

func ConnectWAMP(config *ClientConfig) (*WebRTCSession, error) { return xconn.ConnectWebRTC(config) }

func ConnectWAMPContext(ctx context.Context, config *ClientConfig) (*WebRTCSession, error) {
	return xconn.ConnectWebRTCContext(ctx, config)
}

func NewWebRTCHandler() *WebRTCProvider { return xconn.NewWebRTCHandler() }
