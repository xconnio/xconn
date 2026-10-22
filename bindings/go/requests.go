package xconn

import (
	"context"

	"github.com/xconnio/xconn-core/bindings/go/messages"
	"github.com/xconnio/xconn-core/bindings/go/protocol"
)

type RegisterRequest struct {
	session *Session

	procedure string
	handler   InvocationHandler
	options   map[string]any
}

func (r *RegisterRequest) Execute() RegisterResponse {
	return r.ExecuteContext(context.Background())
}

func (r *RegisterRequest) ExecuteContext(ctx context.Context) RegisterResponse {
	return r.session.register(ctx, r.procedure, r.handler, r.options)
}

func (r *RegisterRequest) Option(key string, value any) *RegisterRequest {
	if r.options == nil {
		r.options = make(map[string]any)
	}

	r.options[key] = value
	return r
}

func (r *RegisterRequest) Options(options map[string]any) *RegisterRequest {
	r.options = options
	return r
}

func (r *RegisterRequest) Match(value string) *RegisterRequest {
	if r.options == nil {
		r.options = make(map[string]any)
	}

	r.options[protocol.OptionMatch] = value
	return r
}

func (r *RegisterRequest) Invoke(value string) *RegisterRequest {
	if r.options == nil {
		r.options = make(map[string]any)
	}

	r.options["invoke"] = value
	return r
}

func (r *RegisterRequest) ToRegister(requestID uint64) *messages.Register {
	return messages.NewRegister(requestID, r.options, r.procedure)
}

type CallRequest struct {
	session *Session

	procedure string
	args      []any
	kwargs    map[string]any
	options   map[string]any

	progressReceiver ProgressReceiver
	progressSender   ProgressSender
}

func (c *CallRequest) Execute() *Result {
	return c.ExecuteContext(context.Background())
}

func (c *CallRequest) ExecuteContext(ctx context.Context) *Result {
	return c.session.callWithRequest(ctx, c)
}

func (c *CallRequest) Option(key string, value any) *CallRequest {
	if c.options == nil {
		c.options = make(map[string]any)
	}

	c.options[key] = value
	return c
}

func (c *CallRequest) Options(options map[string]any) *CallRequest {
	c.options = options
	return c
}

func (c *CallRequest) Arg(arg any) *CallRequest {
	if c.args == nil {
		c.args = make([]any, 0)
	}

	c.args = append(c.args, arg)
	return c
}

func (c *CallRequest) Args(args ...any) *CallRequest {
	c.args = args
	return c
}

func (c *CallRequest) Kwarg(key string, value any) *CallRequest {
	if c.kwargs == nil {
		c.kwargs = make(map[string]any)
	}

	c.kwargs[key] = value
	return c
}

func (c *CallRequest) Kwargs(kwargs map[string]any) *CallRequest {
	c.kwargs = kwargs
	return c
}

func (c *CallRequest) ProgressReceiver(handler ProgressReceiver) *CallRequest {
	c.progressReceiver = handler
	return c
}

func (c *CallRequest) ProgressSender(handler ProgressSender) *CallRequest {
	c.progressSender = handler
	return c
}

type SubscribeRequest struct {
	session *Session

	topic   string
	handler EventHandler
	options map[string]any
}

func (r *SubscribeRequest) Execute() SubscribeResponse {
	return r.ExecuteContext(context.Background())
}

func (r *SubscribeRequest) ExecuteContext(ctx context.Context) SubscribeResponse {
	return r.session.subscribe(ctx, r.topic, r.handler, r.options)
}

func (r *SubscribeRequest) Option(key string, value any) *SubscribeRequest {
	if r.options == nil {
		r.options = make(map[string]any)
	}

	r.options[key] = value
	return r
}

func (r *SubscribeRequest) Options(options map[string]any) *SubscribeRequest {
	r.options = options
	return r
}

func (r *SubscribeRequest) Match(value string) *SubscribeRequest {
	if r.options == nil {
		r.options = make(map[string]any)
	}

	r.options[protocol.OptionMatch] = value
	return r
}

func (r *SubscribeRequest) ToSubscribe(requestID uint64) *messages.Subscribe {
	return messages.NewSubscribe(requestID, r.options, r.topic)
}

type PublishRequest struct {
	session *Session

	topic   string
	args    []any
	kwargs  map[string]any
	options map[string]any
}

func (p *PublishRequest) Execute() PublishResponse {
	return p.ExecuteContext(context.Background())
}

func (p *PublishRequest) ExecuteContext(ctx context.Context) PublishResponse {
	return p.session.publish(ctx, p.topic, p.args, p.kwargs, p.options)
}

func (p *PublishRequest) Option(key string, value any) *PublishRequest {
	if p.options == nil {
		p.options = make(map[string]any)
	}

	p.options[key] = value
	return p
}

func (p *PublishRequest) Options(options map[string]any) *PublishRequest {
	p.options = options
	return p
}

func (p *PublishRequest) Acknowledge(value bool) *PublishRequest {
	if p.options == nil {
		p.options = make(map[string]any)
	}

	p.options["acknowledge"] = value
	return p
}

func (p *PublishRequest) ExcludeMe(value bool) *PublishRequest {
	if p.options == nil {
		p.options = make(map[string]any)
	}

	p.options["exclude_me"] = value
	return p
}

func (p *PublishRequest) Arg(arg any) *PublishRequest {
	if p.args == nil {
		p.args = make([]any, 0)
	}

	p.args = append(p.args, arg)
	return p
}

func (p *PublishRequest) Args(args ...any) *PublishRequest {
	p.args = args
	return p
}

func (p *PublishRequest) Kwarg(key string, value any) *PublishRequest {
	if p.kwargs == nil {
		p.kwargs = make(map[string]any)
	}

	p.kwargs[key] = value
	return p
}

func (p *PublishRequest) Kwargs(kwArgs map[string]any) *PublishRequest {
	p.kwargs = kwArgs
	return p
}

func (p *PublishRequest) ToPublish(requestID uint64) *messages.Publish {
	return messages.NewPublish(requestID, p.options, p.topic, p.args, p.kwargs)
}
