use wampproto::authenticators::{
    anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator, ticket::TicketAuthenticator,
};
use wampproto::joiner::Joiner;
use wampproto::serializers::json::JSONSerializer;

fn joiner(auth: Box<dyn ClientAuthenticator>) -> Joiner {
    Joiner::new("realm", Box::new(JSONSerializer {}), auth)
}

fn welcome(realm: Option<&str>) -> Vec<u8> {
    let mut details = serde_json::json!({"roles":{"broker":{}}, "authid":"alice", "authrole":"user"});
    if let Some(realm) = realm {
        details["realm"] = realm.into();
    }
    serde_json::to_vec(&serde_json::json!([2, 123, details])).unwrap()
}

#[test]
fn anonymous_join_advertises_identity_and_roles_and_records_welcome() {
    for realm in [None, Some("assigned-realm")] {
        let mut client = joiner(Box::new(AnonymousAuthenticator::new("alice", Default::default())));
        assert!(client.session_details().is_err());
        let hello: serde_json::Value = serde_json::from_slice(&client.send_hello().unwrap()).unwrap();
        assert_eq!(hello[0], 1);
        assert_eq!(hello[1], "realm");
        assert_eq!(hello[2]["authid"], "alice");
        assert_eq!(hello[2]["authmethods"], serde_json::json!(["anonymous"]));
        for role in ["caller", "callee", "publisher", "subscriber"] {
            assert!(hello[2]["roles"][role].is_object());
        }
        assert!(client.receive(welcome(realm)).unwrap().is_none());
        let session = client.session_details().unwrap().unwrap();
        assert_eq!(session.id, 123);
        assert_eq!(session.realm, realm.unwrap_or("realm"));
        assert_eq!(session.authid, "alice");
        assert_eq!(session.auth_role, "user");
    }
}

#[test]
fn ticket_challenge_emits_authenticate_then_accepts_welcome() {
    let mut client = joiner(Box::new(TicketAuthenticator::new(
        "alice",
        "secret",
        Default::default(),
    )));
    client.send_hello().unwrap();
    let response = client.receive(br#"[4,"ticket",{}]"#.to_vec()).unwrap().unwrap();
    let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response, serde_json::json!([5, "secret", {}]));
    assert!(client.session_details().is_err());
    assert!(client.receive(br#"[4,"ticket",{}]"#.to_vec()).is_err());
    assert!(client.receive(welcome(None)).unwrap().is_none());
    assert!(client.receive(welcome(None)).is_err());
}

#[test]
fn unsolicited_handshake_messages_are_rejected() {
    let mut client = joiner(Box::new(AnonymousAuthenticator::default()));
    assert!(client.receive(welcome(None)).is_err());
    assert!(client.receive(br#"[4,"ticket",{}]"#.to_vec()).is_err());
    assert!(client.session_details().is_err());
}

#[test]
fn abort_authentication_failure_and_malformed_input_do_not_establish_session() {
    for input in [
        br#"[3,{},"wamp.error.not_authorized"]"#.as_slice(),
        br#"[4,"anonymous",{}]"#,
        b"not json",
        b"[17,1,2]",
    ] {
        let mut client = joiner(Box::new(AnonymousAuthenticator::default()));
        client.send_hello().unwrap();
        let error = client.receive(input.to_vec()).expect_err("must reject input");
        if input[1] == b'3' {
            assert!(error.to_string().contains("wamp.error.not_authorized"));
        }
        assert!(client.session_details().is_err());
    }
}

#[test]
fn checking_async_authentication_does_not_advance_handshake() {
    let mut client = joiner(Box::new(TicketAuthenticator::new(
        "alice",
        "secret",
        Default::default(),
    )));
    assert!(client.check_challenge().is_err());
    client.send_hello().unwrap();
    assert!(client.check_challenge().is_ok());
    assert!(client.check_challenge().is_ok());
    let response = client.receive(br#"[4,"ticket",{}]"#.to_vec()).unwrap().unwrap();
    let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response[1], "secret");
    assert!(client.check_challenge().is_err());
    client.receive(welcome(None)).unwrap();
    assert!(client.check_challenge().is_err());
}
