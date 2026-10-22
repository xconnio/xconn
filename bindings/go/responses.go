package xconn

import (
	"fmt"
	"strings"

	"github.com/xconnio/xconn-core/bindings/go/protocol"
)

type SendProgress func(args []any, kwargs map[string]any) error

type Progress struct {
	Args    []any
	Kwargs  map[string]any
	Options map[string]any
	Err     error
}

func NewProgress(args ...any) *Progress {
	return &Progress{
		Args:    args,
		Options: map[string]any{protocol.OptionProgress: true},
	}
}

func NewFinalProgress(args ...any) *Progress {
	return &Progress{
		Args: args,
	}
}

type Error struct {
	URI    string
	Args   []any
	Kwargs map[string]any
}

func (e *Error) Error() string {
	errStr := e.URI
	if e.Args != nil {
		args := make([]string, len(e.Args))

		for i, arg := range e.Args {
			args[i] = fmt.Sprintf("%v", arg)
		}

		errStr += ": " + strings.Join(args, ", ")
	}

	if e.Kwargs != nil {
		kwargs := make([]string, len(e.Kwargs))

		for key, value := range e.Kwargs {
			kwargs = append(kwargs, fmt.Sprintf("%s=%v", key, value))
		}

		errStr += ": " + strings.Join(kwargs, ", ")
	}

	return errStr
}

type GoodBye struct {
	Details map[string]any
	Reason  string
}

type RegisterResponse struct {
	registration *Registration
	Err          error
}

func (r RegisterResponse) Unregister() error {
	if r.registration == nil {
		return fmt.Errorf("empty registration")
	}

	return r.registration.unregister()
}

type SubscribeResponse struct {
	subscription *Subscription
	Err          error
}

func (r SubscribeResponse) Unsubscribe() error {
	if r.subscription == nil {
		return fmt.Errorf("empty subscription")
	}

	return r.subscription.unsubscribe()
}

type PublishResponse struct {
	Err error
}
