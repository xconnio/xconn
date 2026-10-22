package xconn_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
)

func TestServerKeepaliveClosesSilentPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	require.NoError(t, router.AddRealm("realm", nil))
	server := xconn.NewServer(
		router,
		nil,
		&xconn.ServerConfig{KeepAliveInterval: 30 * time.Millisecond, KeepAliveTimeout: 20 * time.Millisecond},
	)
	defer server.Close()
	listener, err := server.ListenAndServeRawSocket(xconn.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	var dialer net.Dialer
	peer, err := dialer.DialContext(ctx, "tcp", listener.Address)
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, peer.SetDeadline(time.Now().Add(time.Second)))
	_, err = peer.Write([]byte{0x7f, 0xf1, 0, 0})
	require.NoError(t, err)
	_, err = io.ReadFull(peer, make([]byte, 4))
	require.NoError(t, err)
	// Send HELLO directly so this peer deliberately never answers transport PING.
	hello := []byte(`[1,"realm",{"roles":{"caller":{}}}]`)
	frame := append([]byte{0, 0, 0, byte(len(hello))}, hello...)
	_, err = peer.Write(frame)
	require.NoError(t, err)
	header := make([]byte, 4)
	_, err = io.ReadFull(peer, header)
	require.NoError(t, err)
	length := int(header[1])<<16 | int(header[2])<<8 | int(header[3])
	_, err = io.ReadFull(peer, make([]byte, length))
	require.NoError(t, err)
	_, err = io.ReadFull(peer, header)
	require.NoError(t, err)
	require.Equal(t, byte(1), header[0], "server must send RawSocket PING")
	length = int(header[1])<<16 | int(header[2])<<8 | int(header[3])
	_, err = io.ReadFull(peer, make([]byte, length))
	require.NoError(t, err)
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "silent peer must be disconnected after the ping timeout")
}
