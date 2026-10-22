package xconn

import (
	"context"
	"fmt"
	"maps"
)

func (s *Session) callProgressive(ctx context.Context, request *CallRequest) *Result {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	args, kwargs, options := request.args, request.kwargs, maps.Clone(request.options)
	var first *Progress

	if request.progressSender != nil {
		first = request.progressSender(ctx)

		if first == nil {
			return &Result{Err: fmt.Errorf("progress sender returned nil")}
		}

		if first.Err != nil {
			return &Result{Err: first.Err}
		}

		args, kwargs, options = first.Args, first.Kwargs, maps.Clone(first.Options)
	}

	value, err := s.request(
		ctx,
		"call_open",
		map[string]any{"uri": request.procedure, "args": args, "kwargs": kwargs, "options": options},
	)

	if err != nil {
		return &Result{Err: err}
	}

	handle := nativeUint64(nativeFields(value)["handle"])
	defer closeHandle(handle)
	senderError := make(chan error, 1)
	if first != nil && first.Options["progress"] == true {
		go func() {
			for {
				if ctx.Err() != nil {
					return
				}

				progress := request.progressSender(ctx)
				var err error

				if progress == nil {
					err = fmt.Errorf("progress sender returned nil")
				} else {
					err = progress.Err
				}

				if err != nil {
					senderError <- err
					cancel()
					return
				}

				_, err = invoke(
					ctx,
					"call_input",
					map[string]any{
						"handle": handle,
						"args":   progress.Args,
						"kwargs": progress.Kwargs,
						"more":   progress.Options["progress"] == true,
					},
				)

				if err != nil {
					if ctx.Err() == nil {
						senderError <- err
						cancel()
					}

					return
				}

				if progress.Options["progress"] != true {
					return
				}
			}
		}()
	}

	for {
		value, err := invoke(ctx, "call_next", map[string]any{"handle": handle})
		if err != nil {
			select {
			case err = <-senderError:
			default:
			}

			return &Result{Err: err}
		}

		result := decodeResult(value)
		if result.Details["progress"] == true {
			if request.progressReceiver != nil {
				request.progressReceiver(result)
			}

			continue
		}

		return result
	}
}
