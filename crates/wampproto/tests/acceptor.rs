use std::collections::HashMap;
use wampproto::{
    acceptor::{Acceptor, AcceptorState},
    authenticators::{
        anonymous::AnonymousAuthenticator,
        authenticator::ClientAuthenticator,
        cryptosign::{CryptoSignAuthenticator, sign_cryptosign_challenge},
        server::{AuthRequest, AuthResponse, CRAKey, Salt, ServerAuthenticator},
        ticket::TicketAuthenticator,
        wampcra::WAMPCRAAuthenticator,
    },
    joiner::Joiner,
    messages::{
        authenticate::Authenticate,
        types::{Error, Value},
    },
    serializers::{cbor::CBORSerializer, json::JSONSerializer, msgpack::MsgPackSerializer, serializer::Serializer},
};
const SEED: &str = "022b089bed5ab78808365e82dd12c796c835aeb98b4a5a9e099d3e72cb719516";
const PUBLIC: &str = "2b7ec216daa877c7f4c9439db8a722ea2340eacad506988db2564e258284f895";
struct Policy {
    salted: bool,
}
impl ServerAuthenticator for Policy {
    fn methods(&self) -> Vec<String> {
        ["anonymous", "ticket", "wampcra", "cryptosign"]
            .map(String::from)
            .to_vec()
    }

    fn authenticate(&self, r: &AuthRequest) -> Result<AuthResponse, Error> {
        if r.realm != "realm" || r.authid != "alice" {
            return Err(Error::new("unknown identity"));
        }
        if r.method == "ticket" && r.ticket.as_deref() != Some("secret") {
            return Err(Error::new("invalid ticket"));
        }
        if r.method == "cryptosign" && r.public_key.as_deref() != Some(PUBLIC) {
            return Err(Error::new("unknown key"));
        }
        let mut response = AuthResponse::new("alice", "user");
        if r.method == "wampcra" {
            let salt = self.salted.then(|| Salt {
                salt: "salt".into(),
                iterations: 1000,
                key_length: 32,
            });
            response.cra = Some(CRAKey::from_secret("secret", salt)?);
        }
        response.extra.insert("server".into(), Value::Bool(true));
        Ok(response)
    }
}

fn client(method: &str, secret: &str) -> Box<dyn ClientAuthenticator> {
    match method {
        "anonymous" => Box::new(AnonymousAuthenticator::new("alice", HashMap::new())),
        "ticket" => Box::new(TicketAuthenticator::new("alice", secret, HashMap::new())),
        "wampcra" => Box::new(WAMPCRAAuthenticator::new("alice", secret, HashMap::new())),
        "cryptosign" => Box::new(CryptoSignAuthenticator::try_new("alice", SEED, HashMap::new()).unwrap()),
        _ => unreachable!(),
    }
}

fn acceptor(s: Box<dyn Serializer>, salted: bool) -> Acceptor {
    Acceptor::with_clock(
        s,
        Box::new(Policy { salted }),
        Box::new(|| "2026-10-20T00:00:00.000Z".into()),
    )
}

#[test]
fn every_client_authenticator_joins_through_every_codec() {
    for serializer in [
        Box::new(JSONSerializer {}) as Box<dyn Serializer>,
        Box::new(MsgPackSerializer {}),
        Box::new(CBORSerializer {}),
    ] {
        for method in ["anonymous", "ticket", "wampcra", "cryptosign"] {
            for salted in [false, true] {
                let mut joiner = Joiner::new("realm", serializer.clone(), client(method, "secret"));
                let mut acceptor = acceptor(serializer.clone(), salted);
                assert!(acceptor.session_details().is_err());
                let (mut reply, mut welcomed) = acceptor.receive(joiner.send_hello().unwrap()).unwrap();
                if !welcomed {
                    assert_eq!(acceptor.state(), AcceptorState::ChallengeSent);
                    let authenticate = joiner.receive(reply).unwrap().unwrap();
                    (reply, welcomed) = acceptor.receive(authenticate).unwrap();
                }
                assert!(welcomed, "method {method}, salted {salted}");
                assert!(joiner.receive(reply).unwrap().is_none());
                let a = acceptor.session_details().unwrap();
                let c = joiner.session_details().unwrap().unwrap();
                assert_eq!(a.id, c.id);
                assert_eq!(a.realm, "realm");
                assert_eq!(a.authid, "alice");
                assert_eq!(a.auth_role, "user");
                assert_eq!(a.auth_method, method);
                assert_eq!(c.auth_method, method);
                assert_eq!(a.created_at, "2026-10-20T00:00:00.000Z");
                assert!(!c.roles.is_empty());
                assert!(acceptor.receive(joiner.send_hello().unwrap()).is_err());
            }
        }
    }
}

#[test]
fn wrong_secrets_abort_and_make_handshake_terminal() {
    for method in ["ticket", "wampcra"] {
        let mut j = Joiner::new("realm", Box::new(JSONSerializer {}), client(method, "wrong"));
        let mut a = acceptor(Box::new(JSONSerializer {}), false);
        let (challenge, _) = a.receive(j.send_hello().unwrap()).unwrap();
        let response = j.receive(challenge).unwrap().unwrap();
        let (abort, welcomed) = a.receive(response.clone()).unwrap();
        assert!(!welcomed);
        assert_eq!(a.state(), AcceptorState::Aborted);
        assert!(j.receive(abort).is_err());
        assert!(a.session_details().is_err());
        assert!(a.receive(response).is_err());
    }
}

#[test]
fn cryptosign_must_sign_this_acceptors_challenge() {
    let mut j = Joiner::new("realm", Box::new(JSONSerializer {}), client("cryptosign", ""));
    let mut a = acceptor(Box::new(JSONSerializer {}), false);
    a.receive(j.send_hello().unwrap()).unwrap();
    let seed = hex::decode(SEED).unwrap();
    let signature = sign_cryptosign_challenge(&"00".repeat(32), &seed).unwrap();
    let response = Authenticate {
        signature,
        extra: HashMap::new(),
    };
    let abort = a.receive_message(&response).unwrap();
    assert_eq!(abort.message_type(), Value::Int(3));
    assert!(a.session_details().is_err());
}

#[test]
fn unsolicited_authenticate_and_unnegotiated_anonymous_are_rejected() {
    let mut a = Acceptor::default();
    assert!(
        a.receive_message(&Authenticate {
            signature: "secret".into(),
            extra: HashMap::new()
        })
        .is_err()
    );
    assert_eq!(a.state(), AcceptorState::None);
    struct TicketOnly;
    impl ServerAuthenticator for TicketOnly {
        fn methods(&self) -> Vec<String> {
            vec!["ticket".into()]
        }

        fn authenticate(&self, _: &AuthRequest) -> Result<AuthResponse, Error> {
            panic!("policy must not run before negotiation")
        }
    }
    let mut a = Acceptor::new(Box::new(JSONSerializer {}), Box::new(TicketOnly));
    let hello = wampproto::messages::hello::Hello::new(
        "realm",
        "alice",
        HashMap::new(),
        wampproto::joiner::get_client_roles(),
        vec![],
    );
    let result = a.receive_message(&hello).unwrap();
    assert_eq!(result.message_type(), Value::Int(3));
}

#[test]
fn deferred_policy_keeps_state_in_rust_and_rejects_stale_completions() {
    use wampproto::acceptor::AcceptorAction;
    let mut a = Acceptor::default();
    let mut j = Joiner::new("realm", Box::new(JSONSerializer {}), client("anonymous", ""));
    let hello = j.send_hello_message(None);
    let AcceptorAction::Authenticate(request) = a.begin_message(hello.as_ref()).unwrap() else {
        panic!("expected host policy request")
    };
    assert_eq!(request.authid, "alice");
    assert_eq!(a.state(), AcceptorState::AuthenticationPending);
    assert!(a.session_details().is_err());
    assert!(a.begin_message(hello.as_ref()).is_err());
    let welcome = a
        .complete_authentication(Ok(AuthResponse::new("alice", "user")))
        .unwrap();
    assert_eq!(welcome.message_type(), Value::Int(2));
    assert!(
        a.complete_authentication(Ok(AuthResponse::new("other", "other")))
            .is_err()
    );
    assert_eq!(a.session_details().unwrap().authid, "alice");
}
