//! Role declarations shared by native hosts and the browser adapter.
use crate::messages::types::Value;
use std::collections::HashMap;

fn role(features: &[&str]) -> Value {
    Value::Dict(HashMap::from([(
        "features".into(),
        Value::Dict(
            features
                .iter()
                .map(|name| ((*name).into(), Value::Bool(true)))
                .collect(),
        ),
    )]))
}

pub(crate) fn client_roles() -> HashMap<String, Value> {
    HashMap::from([
        (
            "caller".into(),
            role(&[
                "call_canceling",
                "caller_identification",
                "progressive_call_invocations",
                "progressive_call_results",
            ]),
        ),
        (
            "callee".into(),
            role(&[
                "progressive_call_invocations",
                "progressive_call_results",
                "call_canceling",
                "caller_identification",
                "pattern_based_registration",
                "shared_registration",
            ]),
        ),
        (
            "publisher".into(),
            role(&[
                "publisher_exclusion",
                "publisher_identification",
                "subscriber_blackwhite_listing",
            ]),
        ),
        (
            "subscriber".into(),
            role(&["pattern_based_subscription", "publisher_identification"]),
        ),
    ])
}

pub(crate) fn router_roles(session_meta_api: bool) -> HashMap<String, Value> {
    let mut dealer = vec![
        "progressive_call_invocations",
        "progressive_call_results",
        "call_canceling",
        "caller_identification",
        "pattern_based_registration",
        "shared_registration",
    ];
    let mut broker = vec![
        "pattern_based_subscription",
        "publisher_exclusion",
        "publisher_identification",
        "subscriber_blackwhite_listing",
    ];

    if session_meta_api {
        dealer.push("session_meta_api");
        broker.push("session_meta_api");
    }

    HashMap::from([("dealer".into(), role(&dealer)), ("broker".into(), role(&broker))])
}
