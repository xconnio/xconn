use std::collections::HashMap;
use wampproto::{
    dealer::Dealer,
    messages::{
        message::Message,
        payload::PayloadMessage,
        types::{ByteArray, Value},
    },
    serializers::{
        json::JSONSerializer,
        payload::{self, OpaquePayload, Payload},
        serializer::Serializer,
    },
    types::SessionDetails,
};

fn message(wire: serde_json::Value) -> Box<dyn Message> {
    JSONSerializer {}
        .deserialize(serde_json::to_vec(&wire).unwrap())
        .unwrap()
}

fn known_payloads() -> Vec<(u8, Vec<u8>)> {
    vec![
        (1, br#"[[1,2],{"x":true}]"#.to_vec()),
        (2, hex::decode("9292010281a178c3").unwrap()),
        (3, hex::decode("82820102a16178f5").unwrap()),
    ]
}

#[test]
fn payload_envelopes_match_independent_wire_bytes() {
    let expected = Payload {
        args: Some(vec![Value::Int(1), Value::Int(2)]),
        kwargs: Some(HashMap::from([("x".into(), Value::Bool(true))])),
    };
    for (id, data) in known_payloads() {
        assert_eq!(payload::decode(id, &data).unwrap(), expected);
        let encoded = payload::encode(id, &expected).unwrap().unwrap();
        assert_eq!(payload::decode(id, &encoded).unwrap(), expected);
    }
    let binary = Payload {
        args: Some(vec![Value::Bytes(ByteArray(b"opaque".to_vec()))]),
        kwargs: None,
    };
    assert_eq!(payload::encode(0, &binary).unwrap().unwrap(), b"opaque");
    assert_eq!(payload::decode(0, b"opaque").unwrap(), binary);
    for id in 0..=3 {
        assert_eq!(payload::encode(id, &Payload::default()).unwrap(), None);
    }
    for input in [b"[1]".as_slice(), b"[[],[]]", b"[[],{},{}]"] {
        assert!(payload::decode(1, input).is_err());
    }
    assert!(payload::encode(1, &binary).is_err());
    assert!(OpaquePayload::new(255, vec![]).is_err());
}

#[test]
fn opaque_rpc_routes_preserve_bytes_and_expand_for_nonstatic_peers() {
    for static_peer in [false, true] {
        for (id, data) in known_payloads() {
            let mut dealer = Dealer::new();
            for session in [1, 2] {
                dealer
                    .add_session(SessionDetails::new(
                        session,
                        "realm".into(),
                        "alice".into(),
                        "user".into(),
                        static_peer,
                    ))
                    .unwrap();
            }
            dealer
                .receive_message(2, message(serde_json::json!([64, 1, {}, "add"])).as_ref())
                .unwrap();
            let opaque = OpaquePayload::new(id, data.clone()).unwrap();
            let call = PayloadMessage::new(message(serde_json::json!([48, 2, {}, "add"])), opaque.clone()).unwrap();
            let invocation = dealer.receive_message(1, &call).unwrap().unwrap();
            assert_eq!(invocation.recipient, 2);
            let fields = invocation.message.marshal();
            assert_eq!(fields[4], Value::List(vec![Value::Int(1), Value::Int(2)]));
            if static_peer {
                let payload = invocation.message.opaque_payload().unwrap();
                assert_eq!(payload.serializer(), id);
                assert_eq!(payload.data(), data);
            } else {
                assert!(invocation.message.opaque_payload().is_none());
            }
            let Value::Int(invocation_id) = fields[1] else {
                panic!("missing invocation ID")
            };
            let yielded = PayloadMessage::new(message(serde_json::json!([70, invocation_id, {}])), opaque).unwrap();
            let result = dealer.receive_message(2, &yielded).unwrap().unwrap();
            assert_eq!(result.recipient, 1);
            assert_eq!(result.message.opaque_payload().is_some(), static_peer);
            if let Some(payload) = result.message.opaque_payload() {
                assert_eq!(payload.data(), data);
            }
            // A standard serializer always sees usable decoded arguments, even for opaque messages.
            let bytes = JSONSerializer {}.serialize(result.message.as_ref()).unwrap();
            let result = JSONSerializer {}.deserialize(bytes).unwrap();
            assert_eq!(result.marshal()[3], Value::List(vec![Value::Int(1), Value::Int(2)]));
        }
    }
}

#[test]
fn raw_binary_cannot_be_silently_coerced_by_json() {
    let payload = OpaquePayload::new(0, b"valid UTF8 bytes".to_vec()).unwrap();
    let call = PayloadMessage::new(message(serde_json::json!([48, 1, {}, "add"])), payload).unwrap();
    assert!(JSONSerializer {}.serialize(&call).is_err());
    let mut dealer = Dealer::new();
    for id in [1, 2] {
        dealer
            .add_session(SessionDetails::new(
                id,
                "realm".into(),
                "alice".into(),
                "user".into(),
                true,
            ))
            .unwrap();
    }
    dealer
        .receive_message(2, message(serde_json::json!([64, 1, {}, "add"])).as_ref())
        .unwrap();
    let out = dealer.receive_message(1, &call).unwrap().unwrap();
    assert_eq!(out.message.opaque_payload().unwrap().data(), b"valid UTF8 bytes");
}
