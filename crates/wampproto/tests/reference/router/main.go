// Generate baseline router transcripts from the pinned sibling wampproto-go.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	proto "github.com/xconnio/wampproto-go"
	"github.com/xconnio/wampproto-go/messages"
)

type delivery struct {
	Recipient uint64 `json:"recipient"`
	Wire      []any  `json:"wire"`
}
type event struct {
	Recipients []uint64 `json:"recipients"`
	Wire       []any    `json:"wire"`
}
type step struct {
	Operation  string     `json:"operation"`
	Session    uint64     `json:"session"`
	Wire       []any      `json:"wire"`
	Deliveries []delivery `json:"deliveries"`
	Events     []event    `json:"events,omitempty"`
}
type scenario struct {
	Engine string `json:"engine"`
	Steps  []step `json:"steps"`
}

func must(e error) {
	if e != nil {
		panic(e)
	}
}

func details(id uint64) *proto.SessionDetails {
	return proto.NewSessionDetails(id, "realm", fmt.Sprintf("user%d", id), "user", "", false, nil, nil)
}

func main() {
	var cases []scenario
	for _, matching := range []string{"exact", "prefix", "wildcard"} {
		b := proto.NewBroker()
		b.AutoDisclosePublisher(true)
		for id := uint64(1); id <= 3; id++ {
			must(b.AddSession(details(id)))
		}
		s := scenario{Engine: "broker"}
		pattern := "foo.bar"
		if matching == "prefix" {
			pattern = "foo."
		}
		if matching == "wildcard" {
			pattern = "foo.*"
		}
		sub := messages.NewSubscribe(1, map[string]any{"match": matching}, pattern)
		r, e := b.ReceiveMessage(2, sub)
		must(e)
		s.Steps = append(
			s.Steps,
			step{
				Operation:  "receive",
				Session:    2,
				Wire:       sub.Marshal(),
				Deliveries: []delivery{{r.Recipient, r.Message.Marshal()}},
			},
		)
		p := messages.NewPublish(
			2,
			map[string]any{"acknowledge": true},
			"foo.bar",
			[]any{"payload"},
			map[string]any{"x": 42},
		)
		pub, e := b.ReceivePublish(1, p)
		must(e)
		s.Steps = append(
			s.Steps,
			step{
				Operation:  "publish",
				Session:    1,
				Wire:       p.Marshal(),
				Deliveries: []delivery{{pub.Ack.Recipient, pub.Ack.Message.Marshal()}},
				Events:     []event{{pub.Recipients, pub.Event.Marshal()}},
			},
		)
		unsub := messages.NewUnsubscribe(3, r.Message.(*messages.Subscribed).SubscriptionID())
		r, e = b.ReceiveMessage(2, unsub)
		must(e)
		s.Steps = append(
			s.Steps,
			step{
				Operation:  "receive",
				Session:    2,
				Wire:       unsub.Marshal(),
				Deliveries: []delivery{{r.Recipient, r.Message.Marshal()}},
			},
		)
		cases = append(cases, s)
	}
	for _, policy := range []string{"single", "first", "last", "roundrobin"} {
		d := proto.NewDealer()
		d.AutoDiscloseCaller(true)
		for id := uint64(1); id <= 3; id++ {
			must(d.AddSession(details(id)))
		}
		s := scenario{Engine: "dealer"}
		receive := func(id uint64, m messages.Message) *proto.MessageWithRecipient {
			r, e := d.ReceiveMessage(id, m)
			must(e)
			s.Steps = append(
				s.Steps,
				step{
					Operation:  "receive",
					Session:    id,
					Wire:       m.Marshal(),
					Deliveries: []delivery{{r.Recipient, r.Message.Marshal()}},
				},
			)
			return r
		}
		receive(2, messages.NewRegister(1, map[string]any{"invoke": policy}, "add"))
		if policy != "single" {
			receive(3, messages.NewRegister(2, map[string]any{"invoke": policy}, "add"))
		}
		for request := uint64(3); request < 6; request++ {
			r := receive(1, messages.NewCall(request, map[string]any{}, "add", []any{1, 2}, nil))
			inv := r.Message.(*messages.Invocation)
			receive(r.Recipient, messages.NewYield(inv.RequestID(), nil, []any{3}, nil))
		}
		cases = append(cases, s)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	must(enc.Encode(cases))
}
