package xconn

import (
	"context"
	"fmt"
)

// AttachmentConfig describes one established upstream attachment. Its sessions
// and bridges close together. Shared local backend sessions must not be included.
type AttachmentConfig struct {
	Sessions []*Session
	Bridges  []*Bridge
	// Shutdown closes additional connections and workers after the sessions close.
	// It runs once and must not call Close or Wait on this attachment itself.
	Shutdown func() error
}

// Attachment owns a cycle's upstream resources. Losing any session or cancelling
// its lifetime context starts Rust cleanup. Close and Wait wait for completion.
type Attachment struct {
	handle uint64
	done   chan struct{}
	err    error
}

func NewAttachment(ctx context.Context, config AttachmentConfig) (*Attachment, error) {
	config.Sessions = append([]*Session(nil), config.Sessions...)
	config.Bridges = append([]*Bridge(nil), config.Bridges...)
	if len(config.Sessions) == 0 {
		return nil, fmt.Errorf("an attachment requires at least one upstream session")
	}
	sessions := make([]any, len(config.Sessions))
	for index, session := range config.Sessions {
		if session == nil {
			return nil, fmt.Errorf("attachment session is nil")
		}
		sessions[index] = session.handle
	}
	bridges := make([]any, len(config.Bridges))
	for index, bridge := range config.Bridges {
		if bridge == nil {
			return nil, fmt.Errorf("attachment bridge is nil")
		}
		bridges[index] = bridge.handle
	}
	fields := map[string]any{"sessions": sessions, "bridges": bridges}
	var callbacks *foreignCallbacks
	if config.Shutdown != nil {
		var err error
		callbacks, err = newForeignCallbacks(ctx, func(method string, _ any) (any, error) {
			if method != "attachment_shutdown" {
				return nil, unexpectedCallback(method)
			}
			return nil, config.Shutdown()
		})
		if err != nil {
			return nil, err
		}
		fields["callback"] = callbacks.handle
	}
	value, err := invoke(ctx, "service_attachment_new", fields)
	if err != nil {
		if callbacks != nil {
			callbacks.close()
		}
		return nil, err
	}
	attachment := &Attachment{handle: nativeUint64(nativeFields(value)["handle"]), done: make(chan struct{})}
	lifetime := context.WithoutCancel(ctx)
	stop := context.AfterFunc(ctx, func() {
		_, _ = invoke(lifetime, "service_attachment_stop", map[string]any{"handle": attachment.handle})
	})
	go func() {
		_, attachment.err = invoke(lifetime, "service_attachment_wait", map[string]any{"handle": attachment.handle})
		for _, bridge := range config.Bridges {
			_ = bridge.CloseContext(lifetime)
		}
		for _, session := range config.Sessions {
			_ = session.Close()
			<-session.Done()
		}
		if callbacks != nil {
			callbacks.close()
		}
		stop()
		close(attachment.done)
		closeHandle(attachment.handle)
	}()
	return attachment, nil
}

func (a *Attachment) Wait(ctx context.Context) error {
	select {
	case <-a.done:
		return a.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Attachment) Close() error { return a.CloseContext(context.Background()) }

func (a *Attachment) CloseContext(ctx context.Context) error {
	select {
	case <-a.done:
		return a.err
	default:
	}
	// Cleanup outlives a cancelled CloseContext wait.
	_, err := invoke(context.WithoutCancel(ctx), "service_attachment_stop", map[string]any{"handle": a.handle})
	if err != nil {
		select {
		case <-a.done:
			return a.err
		default:
			return err
		}
	}
	return a.Wait(ctx)
}
