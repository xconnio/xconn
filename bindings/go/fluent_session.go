package xconn

import (
	"context"
	"fmt"
)

const ErrNoResult = "io.xconn.no_result"

func (s *Session) Call(procedure string) *CallRequest {
	return &CallRequest{session: s, procedure: procedure}
}

func (s *Session) callWithRequest(ctx context.Context, request *CallRequest) *Result {
	if request.progressReceiver != nil || request.progressSender != nil {
		return s.callProgressive(ctx, request)
	}

	result, err := s.CallContext(ctx, request.procedure, request.args, request.kwargs, request.options)
	if err != nil {
		return &Result{Err: err}
	}

	return result
}

func (s *Session) Register(procedure string, handler InvocationHandler) *RegisterRequest {
	return &RegisterRequest{session: s, procedure: procedure, handler: handler}
}

func (s *Session) register(
	ctx context.Context,
	procedure string,
	handler InvocationHandler,
	options map[string]any,
) RegisterResponse {
	if handler == nil {
		return RegisterResponse{Err: fmt.Errorf("invocation handler is required")}
	}

	registration, err := s.RegisterContext(
		ctx,
		procedure,
		func(ctx context.Context, result *Result) (*Result, error) {
			invocation := NewInvocation(result.Args, result.Kwargs, result.Details)
			invocation.streams = result.streams
			invocation.SendProgress = func(args []any, kwargs map[string]any) error {
				_, err := s.request(
					ctx,
					"invocation_progress",
					map[string]any{"id": result.invocationID, "args": args, "kwargs": kwargs},
				)
				return err
			}

			response := handler(ctx, invocation)
			if response == nil {
				return nil, nil
			}

			if response.Err != nil {
				return nil, response.Err
			}

			return &Result{Args: response.Args, Kwargs: response.Kwargs, Details: response.Details}, nil
		},
		options,
	)
	return RegisterResponse{registration: registration, Err: err}
}

func (s *Session) Subscribe(topic string, handler EventHandler) *SubscribeRequest {
	return &SubscribeRequest{session: s, topic: topic, handler: handler}
}

func (s *Session) subscribe(
	ctx context.Context,
	topic string,
	handler EventHandler,
	options map[string]any,
) SubscribeResponse {
	if handler == nil {
		return SubscribeResponse{Err: fmt.Errorf("event handler is required")}
	}

	subscription, err := s.SubscribeContext(ctx, topic, func(result *Result) {
		handler(NewEvent(result.Args, result.Kwargs, result.Details))
	}, options)
	return SubscribeResponse{subscription: subscription, Err: err}
}

func (s *Session) Publish(topic string) *PublishRequest {
	return &PublishRequest{session: s, topic: topic}
}

func (s *Session) publish(
	ctx context.Context,
	topic string,
	args []any,
	kwargs, options map[string]any,
) PublishResponse {
	return PublishResponse{Err: s.PublishContext(ctx, topic, args, kwargs, options)}
}

func (r *Registration) unregister() error { return r.UnregisterContext(context.Background()) }

func (s *Subscription) unsubscribe() error { return s.UnsubscribeContext(context.Background()) }

func (s *Session) Leave() error { return s.LeaveContext(context.Background()) }

func (s *Session) Details() *SessionDetails { return s.details }

func (s *Session) ID() uint64 { return s.details.ID() }

func (s *Session) Connected() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

func (s *Session) SetOnLeaveListener(listener func()) {
	s.mu.Lock()
	s.onLeave = listener
	s.mu.Unlock()
}

func (s *Session) GoodBye() *GoodBye {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.goodBye
}
