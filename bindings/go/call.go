package xconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// StreamResults selects whether a stream requests intermediate results.
type StreamResults uint8

const (
	ResultsDefault StreamResults = iota // Requests progressive results, preserving the default.
	ResultsFinalOnly
	ResultsProgressive
)

// StreamOptions configures an RPC stream. Zero BufferSize uses the default (32).
// ResultsDefault requests progressive results; ResultsFinalOnly requests only the final reply.
type StreamOptions struct {
	ProgressiveInput bool
	Results          StreamResults
	BufferSize       int
}

// Call owns one RPC. Send/Finish and Receive may run concurrently; only one Receive may be pending.
// Close unfinished calls to cancel remote work and release their native resources.
type Call struct {
	handle    uint64
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	terminal  error
	stop      func() bool
	receiving atomic.Bool
}

// Stream starts a call with a background lifetime context. Accepts zero or one options value.
func (r *CallRequest) Stream(options ...StreamOptions) (*Call, error) {
	return r.StreamContext(context.Background(), options...)
}

// StreamContext starts a call bound to ctx. Accepts zero or one options value.
func (r *CallRequest) StreamContext(ctx context.Context, options ...StreamOptions) (*Call, error) {
	if len(options) > 1 {
		return nil, fmt.Errorf("Stream accepts at most one StreamOptions value")
	}

	if r.session == nil {
		return nil, fmt.Errorf("CallRequest is not attached to a session")
	}

	config := StreamOptions{BufferSize: 32}
	if len(options) == 1 {
		config = options[0]

		if config.BufferSize == 0 {
			config.BufferSize = 32
		}
	}

	if config.Results > ResultsProgressive {
		return nil, fmt.Errorf("invalid stream results mode")
	}

	if config.BufferSize < 1 || config.BufferSize > 65536 {
		return nil, fmt.Errorf("result buffer size must be between 1 and 65536")
	}

	if r.progressReceiver != nil || r.progressSender != nil {
		return nil, fmt.Errorf("streaming calls use Send/Finish/Receive instead of callbacks")
	}

	for _, arg := range r.args {
		if streamSource(arg) != nil {
			return nil, fmt.Errorf("transport stream arguments currently require ExecuteContext")
		}
	}

	session, err := r.selectSession(ctx)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(ctx)
	value, err := session.request(lifetime, "stream_open", map[string]any{
		"uri": r.procedure, "args": r.args, "kwargs": r.kwargs, "options": r.options,
		"progressive_input": config.ProgressiveInput, "buffer_size": config.BufferSize,
		"progressive_results": config.Results != ResultsFinalOnly,
	})

	if err != nil {
		cancel()
		return nil, err
	}

	call := &Call{
		handle: nativeUint64(nativeFields(value)["handle"]),
		ctx:    lifetime,
		cancel: cancel,
	}

	call.mu.Lock()
	call.stop = context.AfterFunc(lifetime, func() { call.terminate(lifetime.Err(), true) })
	call.mu.Unlock()
	return call, nil
}

// Receive returns progress updates, then the final Result, then io.EOF.
// Application errors are returned through error, retaining their WAMP URI and payload.
func (c *Call) Receive(ctx context.Context) (*Result, error) {
	if !c.receiving.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("another Receive is already pending")
	}

	defer c.receiving.Store(false)
	c.mu.Lock()

	if c.closed {
		err := c.terminal
		c.terminal = io.EOF
		c.mu.Unlock()
		return nil, err
	}

	c.mu.Unlock()
	value, err := invoke(ctx, "stream_receive", map[string]any{"handle": c.handle})
	if err != nil {
		c.mu.Lock()

		if c.closed && !errors.Is(c.terminal, io.EOF) {
			err = c.terminal
			c.terminal = io.EOF
		}

		c.mu.Unlock()
		c.terminate(err, false)
		return nil, err
	}

	if value == nil {
		c.terminate(io.EOF, false)
		return nil, io.EOF
	}

	result := decodeResult(value)
	if result.Details["progress"] != true {
		c.terminate(io.EOF, false)
	}

	return result, nil
}

// Send sends an intermediate progressive input.
func (c *Call) Send(ctx context.Context, args []any, kwargs map[string]any) error {
	return c.input(ctx, args, kwargs, false)
}

// Finish sends the final input; results can continue afterward.
func (c *Call) Finish(ctx context.Context, args []any, kwargs map[string]any) error {
	return c.input(ctx, args, kwargs, true)
}

func (c *Call) input(ctx context.Context, args []any, kwargs map[string]any, finish bool) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()

	if closed {
		return fmt.Errorf("call is finished")
	}

	_, err := invoke(
		ctx,
		"stream_input",
		map[string]any{"handle": c.handle, "args": args, "kwargs": kwargs, "finish": finish},
	)

	if ctx.Err() != nil {
		c.terminate(ctx.Err(), true)
	}

	return err
}

// Cancel requests killnowait cancellation. It is idempotent.
func (c *Call) Cancel() error {
	c.terminate(context.Canceled, true)
	return nil
}

// Close releases the call, cancelling unfinished work. It is idempotent.
func (c *Call) Close() error { return c.Cancel() }

func (c *Call) terminate(cause error, report bool) {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}

	c.closed = true
	c.terminal = io.EOF

	if report {
		c.terminal = cause
	}

	stop := c.stop
	c.mu.Unlock()

	if stop != nil {
		stop()
	}

	closeHandle(c.handle)
	c.cancel()
}
