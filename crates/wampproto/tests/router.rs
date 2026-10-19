use serde_json::{Value as Json, json};
use wampproto::{
    broker::Broker,
    dealer::Dealer,
    messages::{cancel::Cancel, message::Message, publish::Publish},
    serializers::{json::JSONSerializer, serializer::Serializer},
    types::SessionDetails,
};

fn msg(wire: Json) -> Box<dyn Message> {
    JSONSerializer {}
        .deserialize(serde_json::to_vec(&wire).unwrap())
        .unwrap()
}

fn wire(m: &dyn Message) -> Json {
    serde_json::to_value(m.marshal()).unwrap()
}

fn session(id: i64) -> SessionDetails {
    SessionDetails::new(id, "realm".into(), format!("user{id}"), "user".into(), false)
}

fn route(d: &mut Dealer, id: i64, input: Json) -> wampproto::routing::RoutedMessage {
    d.receive_message(id, msg(input).as_ref()).unwrap().unwrap()
}

#[test]
fn broker_shared_pattern_fanout_cleanup_and_acknowledgement() {
    let mut b = Broker::new();
    for id in 1..=3 {
        b.add_session(session(id)).unwrap();
    }
    assert!(b.add_session(session(1)).is_err());
    let exact = b
        .receive_message(1, msg(json!([32, 1, {}, "foo.bar"])).as_ref())
        .unwrap();
    let exact_id = wire(exact.message.as_ref())[2].as_i64().unwrap();
    let prefix = b
        .receive_message(2, msg(json!([32,2,{"match":"prefix"},"foo."])).as_ref())
        .unwrap();
    let prefix_id = wire(prefix.message.as_ref())[2].as_i64().unwrap();
    let shared = b
        .receive_message(3, msg(json!([32,3,{"match":"prefix"},"foo."])).as_ref())
        .unwrap();
    assert_eq!(wire(shared.message.as_ref())[2], prefix_id);
    b.receive_message(3, msg(json!([32,4,{"match":"wildcard"},"foo.*"])).as_ref())
        .unwrap();
    let p = msg(json!([16,5,{"acknowledge":true,"exclude_me":true},"foo.bar",["payload"],{"x":42}]));
    let publish = p.as_any().downcast_ref::<Publish>().unwrap();
    let result = b.receive_publish(1, publish).unwrap();
    assert_eq!(result.events.len(), 2);
    assert!(result.events.iter().all(|e| !e.recipients.contains(&1)));
    assert_eq!(wire(result.ack.as_ref().unwrap().message.as_ref())[1], 5);
    for e in &result.events {
        assert_eq!(wire(e.event.as_ref())[4], json!(["payload"]));
        assert_eq!(wire(e.event.as_ref())[5], json!({"x":42}));
    }
    b.remove_session(2).unwrap();
    let result = b.receive_publish(1, publish).unwrap();
    assert_eq!(result.events.len(), 2);
    assert!(result.events.iter().all(|e| e.recipients == vec![3]));
    let denied = b.receive_message(1, msg(json!([34, 6, prefix_id])).as_ref()).unwrap();
    assert_eq!(wire(denied.message.as_ref())[0], 8);
    b.remove_session(3).unwrap();
    assert!(b.receive_publish(1, publish).unwrap().events.is_empty());
    assert!(b.has_subscription("foo.bar"));
    b.receive_message(1, msg(json!([34, 7, exact_id])).as_ref()).unwrap();
    assert!(!b.has_subscription("foo.bar"));
    assert!(
        b.receive_message(1, msg(json!([6, {}, "wamp.close.normal"])).as_ref())
            .is_err()
    );
}

#[test]
fn broker_distinguishes_policies_and_discloses_publisher() {
    let mut b = Broker::new();
    b.add_session(session(1)).unwrap();
    for (i, policy) in ["exact", "prefix", "wildcard"].iter().enumerate() {
        b.receive_message(1, msg(json!([32,i+1,{"match":policy},"foo.bar"])).as_ref())
            .unwrap();
    }
    assert_eq!(b.subscriptions().count(), 3);
    b.auto_disclose_publisher(true);
    let p = msg(json!([16, 10, {"exclude_me": false}, "foo.bar"]));
    let p = p.as_any().downcast_ref::<Publish>().unwrap();
    let result = b.receive_publish(1, p).unwrap();
    assert_eq!(result.events.len(), 3);
    for e in result.events {
        let v = wire(e.event.as_ref());
        assert_eq!(v[3]["publisher"], 1);
        assert_eq!(v[3]["publisher_authid"], "user1");
        assert_eq!(v[3]["topic"], "foo.bar");
    }
    b.remove_session(1).unwrap();
    assert_eq!(b.subscriptions().count(), 0);
    assert!(b.receive_publish(1, p).is_err());
}

fn dealer() -> Dealer {
    let mut d = Dealer::new();
    for id in 1..=3 {
        d.add_session(session(id)).unwrap();
    }
    route(&mut d, 2, json!([64, 1, {}, "add"]));
    d
}

#[test]
fn dealer_replies_are_correlated_and_authenticated() {
    let mut d = dealer();
    let invocation = route(&mut d, 1, json!([48,7,{"receive_progress":true},"add",[1,2]]));
    let inv = wire(invocation.message.as_ref())[1].as_i64().unwrap();
    assert_eq!(invocation.recipient, 2);
    assert!(
        d.receive_message(3, msg(json!([70, inv, {}, ["spoof"]])).as_ref())
            .is_err()
    );
    assert_eq!(d.pending_invocations().count(), 1);
    let progress = route(&mut d, 2, json!([70,inv,{"progress":true},[3]]));
    assert_eq!(progress.recipient, 1);
    assert_eq!(wire(progress.message.as_ref()), json!([50,7,{"progress":true},[3]]));
    assert_eq!(d.pending_invocations().count(), 1);
    let done = route(&mut d, 2, json!([70, inv, {}, [3]]));
    assert_eq!(wire(done.message.as_ref()), json!([50, 7, {}, [3]]));
    assert_eq!(d.pending_invocations().count(), 0);
    assert!(
        d.receive_message(2, msg(json!([70, inv, {}])).as_ref())
            .unwrap()
            .is_none()
    );
    let invocation = route(&mut d, 1, json!([48, 8, {}, "add"]));
    let inv = wire(invocation.message.as_ref())[1].as_i64().unwrap();
    let error = route(&mut d, 2, json!([8,68,inv,{"detail":true},"app.error",["bad"],{"x":1}]));
    assert_eq!(
        wire(error.message.as_ref()),
        json!([8,48,8,{"detail":true},"app.error",["bad"],{"x":1}])
    );
}

#[test]
fn dealer_shared_selection_progressive_identity_and_unregister_cleanup() {
    let mut d = Dealer::new();
    for id in 1..=3 {
        d.add_session(session(id)).unwrap();
    }
    let reg = route(&mut d, 2, json!([64,1,{"invoke":"roundrobin"},"add"]));
    let reg_id = wire(reg.message.as_ref())[2].as_i64().unwrap();
    route(&mut d, 3, json!([64,2,{"invoke":"roundrobin"},"add"]));
    let first = route(&mut d, 1, json!([48,5,{"progress":true},"add",[1]]));
    let inv = wire(first.message.as_ref())[1].as_i64().unwrap();
    assert_eq!(first.recipient, 2);
    for progress in [true, true, false] {
        let part = route(&mut d, 1, json!([48,5,{"progress":progress},"add",[2]]));
        assert_eq!(part.recipient, 2);
        assert_eq!(wire(part.message.as_ref())[1], inv);
    }
    route(&mut d, 2, json!([70, inv, {}, [3]]));
    let next = route(&mut d, 1, json!([48, 6, {}, "add"]));
    assert_eq!(next.recipient, 3);
    route(&mut d, 3, json!([66, 10, reg_id]));
    assert!(d.has_procedure("add"));
    let next = route(&mut d, 1, json!([48, 11, {}, "add"]));
    assert_eq!(next.recipient, 2);
    d.remove_session(3).unwrap();
    assert_eq!(d.registrations().next().unwrap().callees, vec![2]);
}

#[test]
fn dealer_cancel_modes_and_late_responses() {
    for mode in ["skip", "killnowait", "kill"] {
        let mut d = dealer();
        let invocation = route(&mut d, 1, json!([48,7,{"receive_progress":true},"add"]));
        let inv = wire(invocation.message.as_ref())[1].as_i64().unwrap();
        let cancel = msg(json!([49,7,{"mode":mode}]));
        let out = d
            .receive_cancel(1, cancel.as_any().downcast_ref::<Cancel>().unwrap())
            .unwrap();
        assert_eq!(out.len(), if mode == "killnowait" { 2 } else { 1 });
        if mode == "kill" {
            assert_eq!(out[0].recipient, 2);
            let result = route(&mut d, 2, json!([70, inv, {}, [3]]));
            assert_eq!(wire(result.message.as_ref())[4], "wamp.error.canceled");
        } else {
            assert!(
                d.receive_message(2, msg(json!([70,inv,{"progress":true},[2]])).as_ref())
                    .unwrap()
                    .is_none()
            );
            assert_eq!(d.pending_invocations().count(), 1);
            assert!(
                d.receive_message(2, msg(json!([70, inv, {}, [3]])).as_ref())
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(d.pending_invocations().count(), 0);
    }
}

#[test]
fn dealer_matching_errors_disclosure_and_disconnect_cleanup() {
    let mut d = dealer();
    let missing = route(&mut d, 1, json!([48, 1, {}, "absent"]));
    assert_eq!(wire(missing.message.as_ref())[4], "wamp.error.no_such_procedure");
    for policy in ["prefix", "wildcard"] {
        let pattern = if policy == "prefix" { "foo." } else { "bar.*test" };
        route(&mut d, 2, json!([64,2,{"match":policy},pattern]));
        let uri = if policy == "prefix" {
            "foo.method"
        } else {
            "bar.alpha.test"
        };
        let result = route(&mut d, 1, json!([48, 3, {}, uri]));
        let inv = wire(result.message.as_ref())[1].as_i64().unwrap();
        assert_eq!(wire(result.message.as_ref())[3]["procedure"], uri);
        route(&mut d, 2, json!([70, inv, {}]));
    }
    d.auto_disclose_caller(true);
    let inv = route(&mut d, 1, json!([48, 4, {}, "add"]));
    assert_eq!(wire(inv.message.as_ref())[3]["caller_authid"], "user1");
    let lost = d.remove_session(2).unwrap();
    assert_eq!(lost.len(), 1);
    assert_eq!(lost[0].recipient, 1);
    assert_eq!(d.pending_invocations().count(), 0);
    assert!(!d.has_procedure("add"));
    assert!(d.remove_session(2).is_err());
    assert!(d.receive_message(2, msg(json!([48, 5, {}, "add"])).as_ref()).is_err());
}

#[test]
fn matching_handles_components_and_legacy_globs() {
    use wampproto::routing::wildcard_match;
    assert!(wildcard_match("foo..method", "foo.bar.method"));
    assert!(!wildcard_match("foo..method", "foo.bar.baz.method"));
    assert!(wildcard_match("foo.*method", "foo.bar.method"));
    assert!(!wildcard_match("foo.*", "foo.a/b"));
    assert!(!wildcard_match("?", ""));
    assert!(wildcard_match("*", ""));
    assert!(wildcard_match("foo.[a-c]", "foo.b"));
    assert!(!wildcard_match("foo.[^a-c]", "foo.b"));
    assert!(wildcard_match(r"foo.\*", "foo.*"));
    assert!(!wildcard_match("foo.[", "foo.a"));
}

#[test]
fn outputs_match_independently_generated_go_router_transcripts() {
    let cases: Json = serde_json::from_str(include_str!("fixtures/router-reference.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let mut b = Broker::new();
        let mut d = Dealer::new();
        b.auto_disclose_publisher(true);
        d.auto_disclose_caller(true);
        for id in 1..=3 {
            b.add_session(session(id)).unwrap();
            d.add_session(session(id)).unwrap();
        }
        for step in case["steps"].as_array().unwrap() {
            let session = step["session"].as_i64().unwrap();
            let message = msg(step["wire"].clone());
            if case["engine"] == "dealer" {
                let result = d.receive_message(session, message.as_ref()).unwrap().unwrap();
                assert_eq!(
                    json!([{"recipient":result.recipient,"wire":wire(result.message.as_ref())}]),
                    step["deliveries"]
                );
            } else if step["operation"] == "publish" {
                let result = b
                    .receive_publish(session, message.as_any().downcast_ref::<Publish>().unwrap())
                    .unwrap();
                let deliveries: Vec<_> = result
                    .ack
                    .iter()
                    .map(|r| json!({"recipient":r.recipient,"wire":wire(r.message.as_ref())}))
                    .collect();
                let events: Vec<_> = result
                    .events
                    .iter()
                    .map(|e| json!({"recipients":e.recipients,"wire":wire(e.event.as_ref())}))
                    .collect();
                assert_eq!(json!(deliveries), step["deliveries"]);
                assert_eq!(json!(events), step["events"]);
            } else {
                let result = b.receive_message(session, message.as_ref()).unwrap();
                assert_eq!(
                    json!([{"recipient":result.recipient,"wire":wire(result.message.as_ref())}]),
                    step["deliveries"]
                );
            }
        }
    }
}

#[test]
fn caller_disconnect_and_invalid_replies_do_not_leak_pending_calls() {
    let mut d = dealer();
    let r = route(&mut d, 1, json!([48, 1, {}, "add"]));
    let inv = wire(r.message.as_ref())[1].as_i64().unwrap();
    assert!(
        d.receive_message(2, msg(json!([70,inv,{"progress":true}])).as_ref())
            .is_err()
    );
    assert_eq!(d.pending_invocations().count(), 1);
    let out = d.remove_session(1).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].recipient, 2);
    assert_eq!(
        wire(out[0].message.as_ref()),
        json!([69,inv,{"mode":"killnowait","reason":"wamp.error.canceled"}])
    );
    assert_eq!(d.pending_invocations().count(), 0);
}

#[test]
fn dealer_random_policy_always_routes_to_an_attached_registrant() {
    let mut d = Dealer::new();
    for id in 1..=3 {
        d.add_session(session(id)).unwrap();
    }
    for id in [2, 3] {
        route(&mut d, id, json!([64,id,{"invoke":"random"},"add"]));
    }
    for request in 10..40 {
        let out = route(&mut d, 1, json!([48, request, {}, "add"]));
        assert!([2, 3].contains(&out.recipient));
        let inv = wire(out.message.as_ref())[1].as_i64().unwrap();
        route(&mut d, out.recipient, json!([70, inv, {}]));
    }
    assert_eq!(d.pending_invocations().count(), 0);
    let out = route(&mut d, 1, json!([66, 40, 999]));
    assert_eq!(wire(out.message.as_ref())[4], "wamp.error.no_such_registration");
}

#[test]
fn late_canceled_result_cannot_erase_a_new_call_reusing_the_request_id() {
    for mode in ["skip", "killnowait"] {
        let mut d = dealer();
        let old = route(&mut d, 1, json!([48, 7, {}, "add"]));
        let old_inv = wire(old.message.as_ref())[1].as_i64().unwrap();
        let cancel = msg(json!([49,7,{"mode":mode}]));
        d.receive_cancel(1, cancel.as_any().downcast_ref::<Cancel>().unwrap())
            .unwrap();
        let new = route(&mut d, 1, json!([48, 7, {}, "add"]));
        assert_eq!(wire(new.message.as_ref())[0], 68);
        let new_inv = wire(new.message.as_ref())[1].as_i64().unwrap();
        assert_ne!(old_inv, new_inv);
        assert!(
            d.receive_message(2, msg(json!([70, old_inv, {}, ["old"]])).as_ref())
                .unwrap()
                .is_none()
        );
        let cancel = msg(json!([49,7,{"mode":"kill"}]));
        let out = d
            .receive_cancel(1, cancel.as_any().downcast_ref::<Cancel>().unwrap())
            .unwrap();
        assert_eq!(wire(out[0].message.as_ref())[1], new_inv);
        let result = route(&mut d, 2, json!([70, new_inv, {}, ["new"]]));
        assert_eq!(wire(result.message.as_ref())[4], "wamp.error.canceled");
        assert_eq!(d.pending_invocations().count(), 0);
    }
}

#[test]
fn dealer_discards_late_replies_after_caller_disconnect() {
    let mut d = dealer();
    let invocation = route(&mut d, 1, json!([48, 71, {}, "add"]));
    let id = wire(invocation.message.as_ref())[1].as_i64().unwrap();
    let interrupts = d.remove_session(1).unwrap();
    assert_eq!(interrupts.len(), 1);
    assert_eq!(
        wire(interrupts[0].message.as_ref()),
        json!([69, id, {"mode":"killnowait", "reason":"wamp.error.canceled"}])
    );
    for reply in [json!([70, id, {}, [42]]), json!([8, 68, id, {}, "wamp.error.canceled"])] {
        assert!(d.receive_message(2, msg(reply).as_ref()).unwrap().is_none());
    }
    assert_eq!(d.pending_invocations().count(), 0);
    let invocation = route(&mut d, 3, json!([48, 72, {}, "add"]));
    let id = wire(invocation.message.as_ref())[1].as_i64().unwrap();
    assert_eq!(route(&mut d, 2, json!([70, id, {}, [43]])).recipient, 3);
}

#[test]
fn publisher_is_excluded_unless_explicitly_included() {
    let mut broker = Broker::new();
    for id in 1..=2 {
        broker.add_session(session(id)).unwrap();
        broker
            .receive_message(id, msg(json!([32, id, {}, "topic"])).as_ref())
            .unwrap();
    }
    for (options, expected) in [
        (json!({}), vec![2]),
        (json!({"exclude_me": true}), vec![2]),
        (json!({"exclude_me": false}), vec![1, 2]),
    ] {
        let message = msg(json!([16, 10, options, "topic"]));
        let publication = broker
            .receive_publish(1, message.as_any().downcast_ref::<Publish>().unwrap())
            .unwrap();
        assert_eq!(publication.events[0].recipients, expected);
    }
}

#[test]
fn publisher_identity_filters_intersect_and_exclusions_win() {
    let mut broker = Broker::new();
    for (id, authid, role) in [(1, "publisher", "admin"), (2, "alice", "user"), (3, "bob", "guest")] {
        broker
            .add_session(SessionDetails::new(
                id,
                "realm".into(),
                authid.into(),
                role.into(),
                false,
            ))
            .unwrap();
        broker
            .receive_message(id, msg(json!([32, id, {}, "topic"])).as_ref())
            .unwrap();
    }
    for (filters, expected) in [
        (json!({"exclude_authid": ["alice"]}), vec![3]),
        (json!({"eligible_authid": ["alice"]}), vec![2]),
        (json!({"exclude_authrole": ["guest"]}), vec![2]),
        (json!({"eligible_authrole": ["guest"]}), vec![3]),
        (json!({"eligible_authid": []}), vec![]),
        (json!({"eligible_authrole": ["user", "guest"], "exclude": [2]}), vec![3]),
        (
            json!({"eligible": [2,3], "eligible_authid": ["alice"], "eligible_authrole": ["guest"]}),
            vec![],
        ),
        (
            json!({"eligible_authid": ["alice"], "exclude_authrole": ["user"]}),
            vec![],
        ),
    ] {
        let message = msg(json!([16, 10, filters, "topic"]));
        let publication = broker
            .receive_publish(1, message.as_any().downcast_ref::<Publish>().unwrap())
            .unwrap();
        let recipients: Vec<_> = publication
            .events
            .iter()
            .flat_map(|event| event.recipients.iter().copied())
            .collect();
        assert_eq!(recipients, expected);
    }
}

#[test]
fn cancellation_errors_are_correlated_to_call() {
    let mut dealer = dealer();
    for options in [json!({}), json!({"mode": "invalid"})] {
        let message = msg(json!([49, 99, options]));
        let replies = dealer
            .receive_cancel(1, message.as_any().downcast_ref::<Cancel>().unwrap())
            .unwrap();
        assert_eq!(
            wire(replies[0].message.as_ref()),
            json!([8, 48, 99, {}, "wamp.error.invalid_argument"])
        );
    }
}

#[test]
fn completed_progressive_calls_discard_late_inputs_without_retaining_call_records() {
    for completion in ["yield", "error", "departure", "cancel"] {
        let mut dealer = dealer();
        let first = route(&mut dealer, 1, json!([48,7,{"progress":true},"add",[1]]));
        let invocation = wire(first.message.as_ref())[1].as_i64().unwrap();
        match completion {
            "yield" => {
                route(&mut dealer, 2, json!([70, invocation, {}, ["done"]]));
            }
            "error" => {
                route(&mut dealer, 2, json!([8, 68, invocation, {}, "app.failed"]));
            }
            "departure" => {
                dealer.remove_session(2).unwrap();
            }
            "cancel" => {
                let message = msg(json!([49,7,{"mode":"killnowait"}]));
                dealer
                    .receive_cancel(1, message.as_any().downcast_ref::<Cancel>().unwrap())
                    .unwrap();
                dealer
                    .receive_message(2, msg(json!([70, invocation, {}])).as_ref())
                    .unwrap();
            }
            _ => unreachable!(),
        }
        for progress in [true, false, true] {
            assert!(
                dealer
                    .receive_message(1, msg(json!([48,7,{"progress":progress},"add",[2]])).as_ref())
                    .unwrap()
                    .is_none(),
                "{completion}"
            );
        }
        assert_eq!(dealer.pending_invocations().count(), 0);
        if completion == "departure" {
            dealer.add_session(session(2)).unwrap();
            route(&mut dealer, 2, json!([64, 1, {}, "add"]));
        }
        let next = route(&mut dealer, 1, json!([48, 8, {}, "add"]));
        assert_eq!(next.recipient, 2);
        dealer.remove_session(1).unwrap();
        dealer.add_session(session(1)).unwrap();
        assert_eq!(route(&mut dealer, 1, json!([48, 1, {}, "add"])).recipient, 2);
    }
}

#[test]
fn progressive_completion_does_not_expire_after_many_new_calls() {
    let mut dealer = dealer();
    let first = route(&mut dealer, 1, json!([48,1,{"progress":true},"add"]));
    let invocation = wire(first.message.as_ref())[1].as_i64().unwrap();
    route(&mut dealer, 2, json!([70, invocation, {}]));
    for request in 2..5000 {
        let next = route(&mut dealer, 1, json!([48, request, {}, "add"]));
        let invocation = wire(next.message.as_ref())[1].as_i64().unwrap();
        route(&mut dealer, 2, json!([70, invocation, {}]));
    }
    assert!(
        dealer
            .receive_message(1, msg(json!([48, 1, {}, "add"])).as_ref())
            .unwrap()
            .is_none()
    );
    assert_eq!(dealer.pending_invocations().count(), 0);
}
