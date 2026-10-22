package xconn

import (
	"context"
	"fmt"
	"time"
)

type AttachmentOutcome string

const (
	AttachmentDetached     AttachmentOutcome = "detached"
	AttachmentRetry        AttachmentOutcome = "retry"
	AttachmentDisconnected AttachmentOutcome = "disconnected"
)

type AttachmentRetryPolicy struct {
	Initial time.Duration
	Maximum time.Duration
}

// RunAttachment supervises an application-supplied attachment cycle in Rust.
// Each cycle owns its connection resources and must stop when ctx is cancelled.
// Storage, enrollment and remote service contracts are supplied by the application.
func RunAttachment(ctx context.Context, policy AttachmentRetryPolicy,
	cycle func(context.Context) (AttachmentOutcome, error), onError func(error)) error {
	if cycle == nil {
		return fmt.Errorf("attachment cycle is required")
	}
	if policy.Initial == 0 {
		policy.Initial = time.Second
	}
	if policy.Maximum == 0 {
		policy.Maximum = 30 * time.Second
	}
	callbacks, err := newForeignCallbacks(ctx, func(method string, value any) (any, error) {
		switch method {
		case "attachment_cycle":
			outcome, err := cycle(ctx)
			if outcome != AttachmentDetached && outcome != AttachmentRetry && outcome != AttachmentDisconnected {
				return nil, fmt.Errorf("invalid attachment outcome %q", outcome)
			}
			fields := map[string]any{"state": string(outcome)}
			if err != nil {
				fields["error"] = err.Error()
			}
			return fields, nil
		case "attachment_error":
			if onError != nil {
				onError(fmt.Errorf("%s", value))
			}
			return nil, nil
		default:
			return nil, unexpectedCallback(method)
		}
	})
	if err != nil {
		return err
	}
	defer callbacks.close()
	_, err = invoke(ctx, "service_attachment_run", map[string]any{
		"callback": callbacks.handle, "initial": policy.Initial.Seconds(), "maximum": policy.Maximum.Seconds(),
	})
	return err
}
