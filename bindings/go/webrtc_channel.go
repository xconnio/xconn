package xconn

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// WebRTCDataChannel owns one Rust data channel. Closing it leaves its connection open.
// Packet reads, message reads, and OnMessage are alternative consumption modes.
type WebRTCDataChannel struct {
	messageMu      sync.Mutex
	messageHandler func(WebRTCDataChannelMessage)
	messageOnce    sync.Once
	messageStarted bool
	messageDone    chan struct{}
	localClose     chan struct{}
	errorHandler   func(error)
	lowHandler     func()
	errorOnce      sync.Once
	lowOnce        sync.Once
	handle         uint64
	label          string
	protocol       string
	done           chan struct{}
	once           sync.Once
	err            error
	readMu         sync.Mutex
	remainder      []byte
}

func newWebRTCDataChannel(ctx context.Context, fields map[string]any) *WebRTCDataChannel {
	channel := &WebRTCDataChannel{
		handle: nativeUint64(fields["handle"]), label: stringField(fields, "label"),
		protocol: stringField(fields, "protocol"), done: make(chan struct{}),
		messageDone: make(chan struct{}), localClose: make(chan struct{}),
	}

	go func() {
		_, _ = invoke(context.WithoutCancel(ctx), "webrtc_channel_wait", map[string]any{"handle": channel.handle})
		close(channel.done)
	}()
	return channel
}

func (c *WebRTCDataChannel) Label() string { return c.label }

func (c *WebRTCDataChannel) Protocol() string { return c.protocol }

func (c *WebRTCDataChannel) Done() <-chan struct{} { return c.done }

func (c *WebRTCDataChannel) Close() error { return c.CloseContext(context.Background()) }

// CloseContext drains queued sends for up to two seconds before closing.
// Context cancellation aborts the channel immediately instead of waiting for the drain.
func (c *WebRTCDataChannel) CloseContext(ctx context.Context) error {
	c.once.Do(func() {
		close(c.localClose)
		_, c.err = invoke(ctx, "webrtc_channel_close", map[string]any{"handle": c.handle})
		closeHandle(c.handle)
	})
	return c.err
}

func (c *WebRTCDataChannel) Send(data []byte) error {
	return c.SendPacketContext(context.Background(), data)
}

func (c *WebRTCDataChannel) SendPacketContext(ctx context.Context, data []byte) error {
	return c.write(ctx, "webrtc_write_packet", data)
}

func (c *WebRTCDataChannel) WriteMessage(data []byte) error {
	return c.WriteMessageContext(context.Background(), data)
}

func (c *WebRTCDataChannel) WriteMessageContext(ctx context.Context, data []byte) error {
	return c.write(ctx, "webrtc_write_message", data)
}

func (c *WebRTCDataChannel) write(ctx context.Context, method string, data []byte) error {
	if data == nil {
		data = []byte{}
	}

	_, err := invoke(ctx, method, map[string]any{"handle": c.handle, "data": data})
	return err
}

func (c *WebRTCDataChannel) ReadPacket() ([]byte, error) {
	return c.ReadPacketContext(context.Background())
}

func (c *WebRTCDataChannel) ReadPacketContext(ctx context.Context) ([]byte, error) {
	return c.read(ctx, "webrtc_read_packet")
}

func (c *WebRTCDataChannel) ReadMessage() ([]byte, error) {
	return c.ReadMessageContext(context.Background())
}

func (c *WebRTCDataChannel) ReadMessageContext(ctx context.Context) ([]byte, error) {
	return c.read(ctx, "webrtc_read_message")
}

func (c *WebRTCDataChannel) read(ctx context.Context, method string) ([]byte, error) {
	value, err := invoke(ctx, method, map[string]any{"handle": c.handle})
	if err != nil {
		return nil, channelReadError(err)
	}

	return value.([]byte), nil
}

// Read streams raw packets, retaining unread bytes when p is smaller than a packet.
func (c *WebRTCDataChannel) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	c.readMu.Lock()
	defer c.readMu.Unlock()

	for len(c.remainder) == 0 {
		data, err := c.ReadPacket()
		if err != nil {
			return 0, err
		}

		c.remainder = data
	}

	n := copy(p, c.remainder)
	c.remainder = c.remainder[n:]
	return n, nil
}

// Write sends one raw packet. Use WriteMessage for large, chunked messages.
func (c *WebRTCDataChannel) Write(data []byte) (int, error) {
	if err := c.Send(data); err != nil {
		return 0, err
	}

	return len(data), nil
}

// Connection returns an independently owned handle to the shared connection.
// Closing it closes every channel on that connection.
func (c *WebRTCDataChannel) Connection() (*WebRTCConnection, error) {
	value, err := invoke(context.Background(), "webrtc_channel_connection", map[string]any{"handle": c.handle})
	if err != nil {
		return nil, err
	}

	return newWebRTCConnection(context.Background(), nativeUint64(nativeFields(value)["handle"]), 0), nil
}

type WebRTCDataChannelMessage struct {
	Data     []byte
	IsString bool
}

// OnMessage consumes raw packets until closure. Use one reader or callback per channel.
func (c *WebRTCDataChannel) OnMessage(callback func(WebRTCDataChannelMessage)) {
	c.messageMu.Lock()
	c.messageHandler = callback
	c.messageMu.Unlock()

	if callback == nil {
		return
	}

	c.messageOnce.Do(func() {
		c.messageMu.Lock()
		c.messageStarted = true
		c.messageMu.Unlock()
		go func() {
			defer close(c.messageDone)
			for {
				packet, err := c.ReadDataPacketContext(context.Background())
				if err != nil {
					return
				}

				c.messageMu.Lock()
				receiver := c.messageHandler
				c.messageMu.Unlock()

				if receiver != nil {
					receiver(packet)
				}
			}
		}()
	})
}

func (c *WebRTCDataChannel) OnOpen(callback func()) {
	if callback != nil {
		go callback()
	}
}

// OnClose follows queued OnMessage callbacks after remote closure.
// Explicit local closure can interrupt callbacks blocked on application I/O.
func (c *WebRTCDataChannel) OnClose(callback func()) {
	if callback != nil {
		go func() {
			<-c.Done()
			c.waitMessageCallbacks()
			callback()
		}()
	}
}

// ReadDataPacketContext preserves whether the remote peer sent text or binary.
func (c *WebRTCDataChannel) ReadDataPacketContext(ctx context.Context) (WebRTCDataChannelMessage, error) {
	value, err := invoke(ctx, "webrtc_read_data_packet", map[string]any{"handle": c.handle})
	if err != nil {
		return WebRTCDataChannelMessage{}, channelReadError(err)
	}
	fields := nativeFields(value)
	text, _ := fields["is_string"].(bool)
	return WebRTCDataChannelMessage{Data: fields["data"].([]byte), IsString: text}, nil
}

func (c *WebRTCDataChannel) SendText(text string) error {
	_, err := invoke(context.Background(), "webrtc_send_text", map[string]any{"handle": c.handle, "text": text})
	return err
}

func (c *WebRTCDataChannel) BufferedAmount() uint64 {
	value, err := invoke(context.Background(), "webrtc_buffered_amount", map[string]any{"handle": c.handle})
	if err != nil {
		return 0
	}
	return nativeUint64(value)
}

// SetBufferedAmountLowThreshold configures application buffered-low notifications.
func (c *WebRTCDataChannel) SetBufferedAmountLowThreshold(threshold uint64) {
	_, _ = invoke(
		context.Background(),
		"webrtc_buffered_threshold",
		map[string]any{"handle": c.handle, "threshold": threshold},
	)
}

func (c *WebRTCDataChannel) OnBufferedAmountLow(callback func()) {
	c.messageMu.Lock()
	c.lowHandler = callback
	c.messageMu.Unlock()
	if callback == nil {
		return
	}
	c.lowOnce.Do(func() {
		go func() {
			var previous uint64
			for {
				value, err := invoke(
					context.Background(),
					"webrtc_buffered_low",
					map[string]any{"handle": c.handle, "previous": previous},
				)
				if err != nil {
					return
				}
				previous = nativeUint64(value)
				c.messageMu.Lock()
				callback := c.lowHandler
				c.messageMu.Unlock()
				if callback != nil {
					callback()
				}
			}
		}()
	})
}

// OnError reports transport failure after buffered OnMessage callbacks finish.
// Done observes transport termination immediately, independently of callbacks.
func (c *WebRTCDataChannel) OnError(callback func(error)) {
	c.messageMu.Lock()
	c.errorHandler = callback
	c.messageMu.Unlock()
	if callback == nil {
		return
	}
	c.errorOnce.Do(func() {
		go func() {
			value, err := invoke(context.Background(), "webrtc_channel_failure", map[string]any{"handle": c.handle})
			if err != nil || value == nil {
				return
			}
			c.waitMessageCallbacks()
			c.messageMu.Lock()
			callback := c.errorHandler
			c.messageMu.Unlock()
			if callback != nil {
				callback(fmt.Errorf("WebRTC channel: %s", value))
			}
		}()
	})
}

func channelReadError(err error) error {
	if err != nil &&
		(strings.Contains(err.Error(), "channel closed") || strings.Contains(err.Error(), "closed or invalid resource")) {
		return io.EOF
	}
	return err
}

// Remote completion follows queued callbacks. Explicit local close may interrupt
// a callback blocked on application I/O, so shutdown cannot depend on that I/O.
func (c *WebRTCDataChannel) waitMessageCallbacks() {
	c.messageMu.Lock()
	started := c.messageStarted
	c.messageMu.Unlock()
	if started {
		select {
		case <-c.messageDone:
		case <-c.localClose:
		}
	}
}
