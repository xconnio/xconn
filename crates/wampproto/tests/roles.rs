use serde_json::{Value as Json, json};
use wampproto::{
    acceptor::Acceptor,
    authenticators::{anonymous::AnonymousAuthenticator, server},
    joiner::Joiner,
    serializers::{cbor::CBORSerializer, json::JSONSerializer, msgpack::MsgPackSerializer, serializer::Serializer},
};

fn client_roles() -> Json {
    json!({
        "caller": {"features": {"call_canceling": true, "caller_identification": true, "progressive_call_invocations": true, "progressive_call_results": true}},
        "callee": {"features": {
            "call_canceling": true, "caller_identification": true,
            "pattern_based_registration": true, "shared_registration": true,
            "progressive_call_invocations": true, "progressive_call_results": true,
        }},
        "publisher": {"features": {
            "publisher_exclusion": true, "publisher_identification": true,
            "subscriber_blackwhite_listing": true,
        }},
        "subscriber": {"features": {"pattern_based_subscription": true, "publisher_identification": true}},
    })
}

fn router_roles(meta_api: bool) -> Json {
    let mut roles = json!({
        "dealer": {"features": {
            "call_canceling": true, "caller_identification": true,
            "pattern_based_registration": true, "shared_registration": true,
            "progressive_call_invocations": true, "progressive_call_results": true,
        }},
        "broker": {"features": {
            "pattern_based_subscription": true, "publisher_exclusion": true,
            "publisher_identification": true, "subscriber_blackwhite_listing": true,
        }},
    });
    if meta_api {
        for name in ["dealer", "broker"] {
            roles[name]["features"]["session_meta_api"] = json!(true);
        }
    }
    roles
}

#[test]
fn serialized_hello_and_welcome_advertise_only_supported_features() {
    for codec in [
        Box::new(JSONSerializer {}) as Box<dyn Serializer>,
        Box::new(CBORSerializer {}),
        Box::new(MsgPackSerializer {}),
    ] {
        for meta_api in [false, true] {
            let mut client = Joiner::new("realm", codec.clone(), Box::new(AnonymousAuthenticator::default()));
            let mut router =
                Acceptor::new(codec.clone(), Box::new(server::AnonymousAuthenticator)).with_session_meta_api(meta_api);
            let hello = client.send_hello().unwrap();
            let wire = codec.deserialize(hello.clone()).unwrap().marshal();
            let wire: Json = serde_json::to_value(wire).unwrap();
            assert_eq!(wire[2]["roles"], client_roles());

            let (welcome, welcomed) = router.receive(hello).unwrap();
            assert!(welcomed);
            let wire: Json = serde_json::to_value(codec.deserialize(welcome.clone()).unwrap().marshal()).unwrap();
            assert_eq!(wire[2]["roles"], router_roles(meta_api));
            assert_eq!(
                serde_json::to_value(&router.session_details().unwrap().roles).unwrap(),
                router_roles(meta_api)
            );

            client.receive(welcome).unwrap();
            let details = client.session_details().unwrap().unwrap();
            assert_eq!(serde_json::to_value(&details.roles).unwrap(), router_roles(meta_api));
        }
    }
}
