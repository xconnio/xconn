package xconn

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// StreamSource transfers ownership of a reader or duplex endpoint on execution.
// The source must unblock its I/O when Close is called.
type StreamSource struct {
	reader io.ReadCloser
	writer io.Writer
	used   atomic.Bool
}

// ReadStream attaches a readable source at its ordinary positional argument index.
func ReadStream(reader io.ReadCloser) *StreamSource {
	return &StreamSource{reader: reader}
}

// DuplexStream attaches a bidirectional endpoint, for example a PTY or socket.
func DuplexStream(stream io.ReadWriteCloser) *StreamSource {
	return &StreamSource{reader: stream, writer: stream}
}

// rpcStream binds native I/O to the lifetime of its RPC payload owner.
type rpcStream struct {
	*nativeStreamConn
	ctx  context.Context
	stop func() bool
	once sync.Once
}

func (s *rpcStream) Read(buffer []byte) (int, error) {
	n, err := s.ReadContext(s.ctx, buffer)
	return n, streamDeadlineError(err)
}

func (s *rpcStream) Write(buffer []byte) (int, error) {
	n, err := s.WriteContext(s.ctx, buffer)
	return n, streamDeadlineError(err)
}

func (s *rpcStream) Close() error {
	s.once.Do(func() {
		s.stop()
		_ = s.QUICStream.Close()
	})
	return nil
}

type streamPayload struct {
	session *Session
	ctx     context.Context
	mu      sync.Mutex
	claimed map[int]bool
}

func (p *streamPayload) open(args []any, index int) (io.ReadWriteCloser, error) {
	if index < 0 || index >= len(args) {
		return nil, fmt.Errorf("argument index out of range: %d", index)
	}
	if p == nil {
		return nil, fmt.Errorf("payload has no stream transport")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.claimed[index] {
		return nil, fmt.Errorf("stream argument %d was already opened", index)
	}
	value, err := p.session.request(p.ctx, "rpc_stream_accept", map[string]any{"reference": args[index]})
	if err != nil {
		return nil, err
	}
	stream := &rpcStream{nativeStreamConn: &nativeStreamConn{
		QUICStream: &QUICStream{handle: nativeUint64(nativeFields(value)["handle"])},
		local:      &net.UnixAddr{Name: "local", Net: "xconn-rpc"},
		remote:     &net.UnixAddr{Name: "remote", Net: "xconn-rpc"},
	}, ctx: p.ctx}
	stream.stop = context.AfterFunc(p.ctx, func() { _ = stream.QUICStream.Close() })
	if p.claimed == nil {
		p.claimed = make(map[int]bool)
	}
	p.claimed[index] = true
	return stream, nil
}

// ArgStream opens the stream at index. Close it when finished. Cancellation of
// the call context closes it; receiving the final RPC result does not.
func (r *Result) ArgStream(index int) (io.ReadWriteCloser, error) {
	return r.streams.open(r.Args, index)
}

// ArgStream opens an invocation's indexed stream. Input streams end with the
// invocation; returned streams may outlive its final RPC reply.
func (inv *Invocation) ArgStream(index int) (io.ReadWriteCloser, error) {
	return inv.streams.open(inv.Args(), index)
}

func streamSource(value any) *StreamSource {
	if source, ok := value.(*StreamSource); ok {
		return source
	}
	if reader, ok := value.(io.ReadCloser); ok {
		return ReadStream(reader)
	}
	return nil
}

type streamTransfer struct {
	cleanup []func()
	done    chan struct{}
}

func (t *streamTransfer) close() {
	if t == nil {
		return
	}
	for _, closeStream := range t.cleanup {
		closeStream()
	}
}

// streamArguments changes only top-level positional stream values. Everything
// else retains the ordinary WAMP payload representation.
func (s *Session) streamArguments(ctx context.Context, args []any) ([]any, *streamTransfer, error) {
	encoded := args
	copied := false
	var transfer *streamTransfer
	var workers sync.WaitGroup
	for index, value := range args {
		source := streamSource(value)
		if source == nil {
			continue
		}
		if transfer == nil {
			transfer = &streamTransfer{done: make(chan struct{})}
		}
		if source.reader == nil || !source.used.CompareAndSwap(false, true) {
			transfer.close()
			return nil, nil, fmt.Errorf("stream source is nil or has already been transferred")
		}
		native, err := s.request(ctx, "rpc_stream_offer", nil)
		if err != nil {
			_ = source.reader.Close()
			transfer.close()
			return nil, nil, err
		}
		fields := nativeFields(native)
		stream := &QUICStream{handle: nativeUint64(fields["handle"])}
		workers.Add(1)
		transfer.cleanup = append(transfer.cleanup, source.start(ctx, s.done, stream, workers.Done))
		if !copied {
			encoded = append([]any(nil), args...)
			copied = true
		}
		encoded[index] = fields["reference"]
	}
	if transfer != nil {
		go func() {
			workers.Wait()
			close(transfer.done)
		}()
	}
	return encoded, transfer, nil
}

// start adapts host I/O to the native stream and returns idempotent cleanup.
func (source *StreamSource) start(
	ctx context.Context, sessionDone <-chan struct{}, stream *QUICStream, complete func(),
) func() {
	finished := make(chan struct{})
	var once sync.Once
	closeStream := func() {
		once.Do(func() {
			_ = stream.Close()
			_ = source.reader.Close()
			close(finished)
		})
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-sessionDone:
		case <-finished:
			return
		}
		closeStream()
	}()
	go func() {
		defer complete()
		defer closeStream()
		output := make(chan struct{})
		go func() {
			defer close(output)
			if _, copyErr := io.Copy(stream, source.reader); copyErr != nil {
				closeStream()
				return
			}
			_ = stream.CloseWriteContext(ctx)
		}()
		if source.writer == nil {
			_, _ = io.Copy(io.Discard, stream)
		} else {
			_, copyErr := io.Copy(source.writer, stream)
			if half, ok := source.reader.(interface{ CloseWrite() error }); ok && copyErr == nil {
				_ = half.CloseWrite()
				<-output
			}
		}
		closeStream()
		<-output
	}()
	return closeStream
}
