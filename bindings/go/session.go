package xconn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type InvocationHandler func(context.Context, *Invocation) *Result
type ProgressReceiver func(*Result)
type ProgressSender func(context.Context) *Progress
type nativeInvocationHandler func(context.Context, *Result) (*Result, error)

type EventHandler func(*Event)
type nativeEventHandler func(*Result)

type handler struct {
	invocation nativeInvocationHandler
	event      nativeEventHandler
}

type Session struct {
	connectionHandle uint64
	closeBase        func()
	base             *nativeBase
	hostCallbacks    *foreignCallbacks
	onLeave          func()
	goodBye          *GoodBye
	cancel           context.CancelFunc
	handle           uint64
	details          *SessionDetails
	mu               sync.Mutex
	callbacks        map[uint64]handler
	invocations      map[uint64]context.CancelFunc
	tokens           atomic.Uint64
	closeOnce        sync.Once
	done             chan struct{}
}

func newSession(ctx context.Context, fields map[string]any, callbacks ...*foreignCallbacks) *Session {
	sessionContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &Session{
		connectionHandle: optionalNativeUint(fields["connection"]),
		cancel:           cancel,
		handle:           nativeUint64(fields["handle"]),
		details: NewSessionDetails(
			nativeUint64(fields["session_id"]),
			stringField(fields, "realm"),
			stringField(fields, "authid"),
			stringField(fields, "authrole"),
			stringField(fields, "authmethod"),
			nativeFields(fields["authextra"]),
		),
		callbacks:   map[uint64]handler{},
		invocations: map[uint64]context.CancelFunc{},
		done:        make(chan struct{}),
	}

	if len(callbacks) > 0 {
		s.hostCallbacks = callbacks[0]
	}

	go s.events(sessionContext)
	return s
}

func (s *Session) request(ctx context.Context, method string, f map[string]any) (any, error) {
	if f == nil {
		f = map[string]any{}
	}

	f["handle"] = s.handle
	return invoke(ctx, method, f)
}

func (s *Session) events(ctx context.Context) {
	defer func() {
		close(s.done)
		s.mu.Lock()
		listener := s.onLeave
		s.mu.Unlock()

		if listener != nil {
			listener()
		}
	}()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		for _, cancel := range s.invocations {
			cancel()
		}
	}()

	for {
		value, err := s.request(ctx, "next_event", nil)
		if err != nil || value == nil {
			return
		}

		event := nativeFields(value)
		if event["kind"] == "interrupt" {
			s.mu.Lock()
			cancel := s.invocations[nativeUint64(event["id"])]
			s.mu.Unlock()

			if cancel != nil {
				cancel()
			}

			continue
		}

		s.mu.Lock()
		h, ok := s.callbacks[nativeUint64(event["token"])]
		s.mu.Unlock()

		if !ok {
			if event["kind"] == "invocation" {
				_, _ = s.request(
					ctx,
					"reply",
					map[string]any{"id": event["id"], "error": map[string]any{"uri": "wamp.error.canceled"}},
				)
			}

			continue
		}

		if event["kind"] == "event" {
			go h.event(decodeResult(value))
			continue
		}

		id := nativeUint64(event["id"])
		ctx, cancel := context.WithCancel(ctx)
		s.mu.Lock()
		s.invocations[id] = cancel
		s.mu.Unlock()
		go func() {
			defer cancel()
			defer func() {
				s.mu.Lock()
				delete(s.invocations, id)
				s.mu.Unlock()
			}()
			response, err := h.invocation(ctx, decodeResult(value))
			fields := map[string]any{"id": id}
			if err != nil {
				var app *Error

				if errors.As(err, &app) {
					fields["error"] = map[string]any{"uri": app.URI, "args": app.Args, "kwargs": app.Kwargs}
				} else {
					fields["error"] = map[string]any{"uri": "wamp.error.runtime_error", "args": []any{err.Error()}}
				}
			} else if response != nil {
				fields["args"], fields["kwargs"] = response.Args, response.Kwargs
			}

			_, _ = s.request(ctx, "reply", fields)
		}()
	}
}

func (s *Session) CallContext(
	ctx context.Context,
	procedure string,
	args []any,
	kwargs map[string]any,
	options Options,
) (*Result, error) {
	value, err := s.request(
		ctx,
		"call",
		map[string]any{"uri": procedure, "args": args, "kwargs": kwargs, "options": options},
	)

	if err != nil {
		return nil, err
	}

	return decodeResult(value), nil
}

func (s *Session) PublishContext(
	ctx context.Context,
	topic string,
	args []any,
	kwargs map[string]any,
	options Options,
) error {
	_, err := s.request(
		ctx,
		"publish",
		map[string]any{"uri": topic, "args": args, "kwargs": kwargs, "options": options},
	)
	return err
}

type Registration struct {
	ID      int64
	session *Session
	token   uint64
}

func (s *Session) RegisterContext(
	ctx context.Context,
	procedure string,
	callback nativeInvocationHandler,
	options Options,
) (*Registration, error) {
	if callback == nil {
		return nil, fmt.Errorf("invocation handler is required")
	}

	token := s.tokens.Add(1)
	s.mu.Lock()
	s.callbacks[token] = handler{invocation: callback}
	s.mu.Unlock()
	value, err := s.request(ctx, "register", map[string]any{"uri": procedure, "token": token, "options": options})
	if err != nil {
		s.mu.Lock()
		delete(s.callbacks, token)
		s.mu.Unlock()
		return nil, err
	}

	return &Registration{nativeInt64(nativeFields(value)["id"]), s, token}, nil
}

func (r *Registration) UnregisterContext(ctx context.Context) error {
	_, err := r.session.request(ctx, "unregister", map[string]any{"id": r.ID})
	if err == nil {
		r.session.mu.Lock()
		delete(r.session.callbacks, r.token)
		r.session.mu.Unlock()
	}

	return err
}

type Subscription struct {
	ID      int64
	session *Session
	token   uint64
}

func (s *Session) SubscribeContext(
	ctx context.Context,
	topic string,
	callback nativeEventHandler,
	options Options,
) (*Subscription, error) {
	if callback == nil {
		return nil, fmt.Errorf("event handler is required")
	}

	token := s.tokens.Add(1)
	s.mu.Lock()
	s.callbacks[token] = handler{event: callback}
	s.mu.Unlock()
	value, err := s.request(ctx, "subscribe", map[string]any{"uri": topic, "token": token, "options": options})
	if err != nil {
		s.mu.Lock()
		delete(s.callbacks, token)
		s.mu.Unlock()
		return nil, err
	}

	return &Subscription{nativeInt64(nativeFields(value)["id"]), s, token}, nil
}

func (s *Subscription) UnsubscribeContext(ctx context.Context) error {
	_, err := s.session.request(ctx, "unsubscribe", map[string]any{"id": s.ID, "token": s.token})
	if err == nil {
		s.session.mu.Lock()
		delete(s.session.callbacks, s.token)
		s.session.mu.Unlock()
	}

	return err
}

func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()

		if s.closeBase != nil {
			s.closeBase()
		}

		closeHandle(s.handle)

		if s.base != nil {
			closeHandle(s.base.handle)
		}

		if s.hostCallbacks != nil {
			s.hostCallbacks.close()
		}
	})
	return nil
}

func (s *Session) LeaveContext(ctx context.Context) error {
	_, err := s.request(ctx, "leave", nil)
	_ = s.Close()
	return err
}

func (s *Session) Done() <-chan struct{} { return s.done }
