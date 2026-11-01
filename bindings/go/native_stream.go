package xconn

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// QUICStream supports concurrent reading and writing. CloseWrite sends EOF in one direction.
// Context methods allow blocked operations to be cancelled without closing sibling streams.
type QUICStream struct {
	handle uint64
	once   sync.Once
}

// nativeStream is the private transport-neutral name; the public name stays
// unchanged for compatibility with existing QUIC callers.
type nativeStream = QUICStream

func (s *QUICStream) Read(data []byte) (int, error) { return s.ReadContext(context.Background(), data) }

func (s *QUICStream) ReadContext(ctx context.Context, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	size := min(len(data), 1024*1024)
	value, err := invoke(ctx, "quic_read", map[string]any{"handle": s.handle, "size": size})
	if err != nil {
		return 0, err
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		return 0, io.EOF
	}
	return copy(data, bytes), nil
}

func (s *QUICStream) Write(data []byte) (int, error) {
	return s.WriteContext(context.Background(), data)
}

func (s *QUICStream) WriteContext(ctx context.Context, data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		size := min(len(data), 1024*1024)
		value, err := invoke(ctx, "quic_write", map[string]any{"handle": s.handle, "data": data[:size]})
		if err != nil {
			return written, err
		}
		nativeCount := nativeUint64(value)
		if nativeCount > 1024*1024 {
			return written, fmt.Errorf("invalid QUIC write count")
		}
		count := int(nativeCount)
		if count > size {
			return written, fmt.Errorf("invalid QUIC write count")
		}
		if count == 0 {
			return written, io.ErrNoProgress
		}
		written += count
		data = data[count:]
	}
	return written, nil
}

func (s *QUICStream) CloseWrite() error { return s.CloseWriteContext(context.Background()) }

func (s *QUICStream) CloseWriteContext(ctx context.Context) error {
	_, err := invoke(ctx, "quic_finish", map[string]any{"handle": s.handle})
	return err
}

func (s *QUICStream) Close() error {
	s.once.Do(func() { closeHandle(s.handle) })
	return nil
}
