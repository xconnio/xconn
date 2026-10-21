package messages_test

import (
	"testing"

	"github.com/xconnio/xconn-core/bindings/go/internal/bridge"
	"github.com/xconnio/xconn-core/bindings/go/messages"
)

func TestConstantsMatchRust(t *testing.T) {
	codes := []uint64{
		messages.MessageTypeHello,
		messages.MessageTypeWelcome,
		messages.MessageTypeAbort,
		messages.MessageTypeChallenge,
		messages.MessageTypeAuthenticate,
		messages.MessageTypeGoodbye,
		messages.MessageTypeError,
		messages.MessageTypePublish,
		messages.MessageTypePublished,
		messages.MessageTypeSubscribe,
		messages.MessageTypeSubscribed,
		messages.MessageTypeUnsubscribe,
		messages.MessageTypeUnsubscribed,
		messages.MessageTypeEvent,
		messages.MessageTypeCall,
		messages.MessageTypeCancel,
		messages.MessageTypeResult,
		messages.MessageTypeRegister,
		messages.MessageTypeRegistered,
		messages.MessageTypeUnregister,
		messages.MessageTypeUnregistered,
		messages.MessageTypeInvocation,
		messages.MessageTypeInterrupt,
		messages.MessageTypeYield,
	}
	v, e := bridge.Call("definitions")
	if e != nil {
		t.Fatal(e)
	}
	defs := bridge.Slice(v)
	if len(codes) != len(defs) {
		t.Fatal("missing message")
	}
	for i, d := range defs {
		m := bridge.Dict(d)
		if codes[i] != bridge.Uint(m["code"]) {
			t.Fatal("constant mismatch", m)
		}
		if _, err := messages.FromFields(bridge.String(m["class"]), map[string]any{}); err != nil {
			t.Fatal("missing factory", m)
		}
	}
}
