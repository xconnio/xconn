package messages

import (
	"fmt"

	"github.com/xconnio/xconn-core/bindings/go/internal/bridge"
)

type nativeMessage interface {
	Message
	load(map[string]any)
}

func newMessage(name string) (nativeMessage, error) {
	switch name {
	case "Abort":
		return &Abort{}, nil
	case "Authenticate":
		return &Authenticate{}, nil
	case "Call":
		return &Call{}, nil
	case "Cancel":
		return &Cancel{}, nil
	case "Challenge":
		return &Challenge{}, nil
	case "Error":
		return &Error{}, nil
	case "Event":
		return &Event{}, nil
	case "Goodbye":
		return &GoodBye{}, nil
	case "Hello":
		return &Hello{}, nil
	case "Interrupt":
		return &Interrupt{}, nil
	case "Invocation":
		return &Invocation{}, nil
	case "Publish":
		return &Publish{}, nil
	case "Published":
		return &Published{}, nil
	case "Register":
		return &Register{}, nil
	case "Registered":
		return &Registered{}, nil
	case "Result":
		return &Result{}, nil
	case "Subscribe":
		return &Subscribe{}, nil
	case "Subscribed":
		return &Subscribed{}, nil
	case "Unregister":
		return &Unregister{}, nil
	case "Unregistered":
		return &Unregistered{}, nil
	case "Unsubscribe":
		return &Unsubscribe{}, nil
	case "Unsubscribed":
		return &Unsubscribed{}, nil
	case "Welcome":
		return &Welcome{}, nil
	case "Yield":
		return &Yield{}, nil
	default:
		return nil, fmt.Errorf("missing Go adapter for %s", name)
	}
}

func FromFields(name string, fields map[string]any) (Message, error) {
	message, err := newMessage(name)
	if err != nil {
		return nil, err
	}

	message.load(fields)
	return message, nil
}

// NewForWire selects the language adapter using Rust's message dispatch.
func NewForWire(wire []any) (Message, error) {
	name, err := bridge.Call("identify", wire)
	if err != nil {
		return nil, err
	}

	return newMessage(bridge.String(name))
}
