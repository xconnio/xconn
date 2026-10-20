#![cfg(feature = "async")]
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use wampproto::authenticators::{
    anonymous::AnonymousAuthenticator,
    authenticator::ClientAuthenticator,
    cryptosign::CryptoSignAuthenticator,
    server::{AuthRequest, AuthResponse, CRAKey, Salt, ServerAuthenticator},
    ticket::TicketAuthenticator,
    wampcra::WAMPCRAAuthenticator,
};
use xconn_core::Result as RpcResult;
use xconn_core::{
    async_::{
        client::Client,
        router::{RealmConfig, Router},
        server::Server,
        session::Session,
        types::{RegisterRequest, SubscribeRequest},
    },
    *,
};

fn credentials(method: &str, bad: bool) -> Box<dyn ClientAuthenticator + Send + Sync> {
    let secret = if bad { "wrong" } else { "secret" };
    match method {
        "anonymous" => Box::new(AnonymousAuthenticator::default()),
        "ticket" => Box::new(TicketAuthenticator::new("interop", secret, HashMap::new())),
        "wampcra" | "salted" => Box::new(WAMPCRAAuthenticator::new(
            if method == "salted" { "salted" } else { "interop" },
            secret,
            HashMap::new(),
        )),
        "cryptosign" => Box::new(
            CryptoSignAuthenticator::try_new("interop", &if bad { "22" } else { "11" }.repeat(32), HashMap::new())
                .unwrap(),
        ),
        _ => panic!("unknown method"),
    }
}

fn specs() -> Vec<Box<dyn SerializerSpec>> {
    vec![
        Box::new(JSONSerializerSpec),
        Box::new(CBORSerializerSpec),
        Box::new(MsgPackSerializerSpec),
    ]
}

async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(90), future)
        .await
        .expect("independent scenario timed out")
}

async fn verify_reference() -> String {
    let binary = std::env::var("XCONN_REFERENCE_GO_BIN")
        .expect("set XCONN_REFERENCE_GO_BIN to an independent pure-Go reference executable");
    let output = tokio::process::Command::new(&binary)
        .arg("provenance")
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let provenance: HashMap<String, Value> = wampproto::serializers::codec::Codec::Json
        .decode(&output.stdout)
        .unwrap();
    assert_eq!(provenance["kind"], Value::Str("independent-pure-go".into()));
    assert_eq!(provenance["cgo_enabled"], Value::Bool(false));
    let Value::Dict(revisions) = &provenance["revisions"] else {
        panic!("no reference revisions")
    };
    assert_eq!(
        revisions["wampproto-go"],
        Value::Str("ecb54c6c2318d7d3184f77ca95cf5f49c779edae".into())
    );
    assert_eq!(
        revisions["xconn-go"],
        Value::Str("191a086e99b94bcfb50e2c8ee3667c9cd23015b4".into())
    );
    binary
}

async fn go_server() -> (tokio::process::Child, HashMap<String, String>) {
    let binary = verify_reference().await;
    let mut child = tokio::process::Command::new(binary)
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .expect("Go fixture exited before startup");
    (
        child,
        wampproto::serializers::codec::Codec::Json
            .decode(line.as_bytes())
            .unwrap(),
    )
}

async fn call(session: &Session, procedure: &str, args: Vec<Value>) -> RpcResult {
    let result = tokio::time::timeout(
        Duration::from_secs(4),
        session.execute_call(CallRequest::new(procedure).args(args)),
    )
    .await
    .expect("RPC timeout")
    .unwrap();
    assert!(result.error.is_none(), "{procedure}: {:?}", result.error);
    result
}

async fn state(session: &Session, token: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let result = call(session, "suite.state", vec![Value::Str(token.into())]).await;
            if result.args == Some(vec![Value::Str(expected.into())]) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("remote invocation state not observed");
}

#[tokio::test]
#[ignore = "requires XCONN_REFERENCE_GO_BIN, independent pure-Go engine"]
async fn original_go_authentication_and_payload_matrix() {
    bounded(async {
        let (mut child, info) = go_server().await;
        for transport in ["ws", "rs"] {
            for spec in specs() {
                for method in ["anonymous", "ticket", "wampcra", "salted", "cryptosign"] {
                    let session = Client::new(spec.clone(), credentials(method, false))
                        .connect(&info[transport], "realm")
                        .await
                        .unwrap_or_else(|e| panic!("{transport}/{} {method}: {e}", spec.subprotocol()));
                    assert_eq!(
                        session.details().auth_method(),
                        if method == "salted" { "wampcra" } else { method }
                    );
                    assert_eq!(session.details().auth_role(), "trusted");
                    let response = session
                        .execute_call(
                            CallRequest::new("suite.payload")
                                .arg("hello")
                                .arg(Value::List(vec![Value::Bool(true), Value::Int(42)]))
                                .kwarg(
                                    "nested",
                                    Value::Dict(HashMap::from([("unicode".into(), Value::Str("مرحبا 🦀".into()))])),
                                ),
                        )
                        .await
                        .unwrap();
                    assert!(response.error.is_none());
                    assert_eq!(
                        response.args.unwrap(),
                        vec![
                            Value::Str("hello".into()),
                            Value::List(vec![Value::Bool(true), Value::Int(42)])
                        ]
                    );
                    assert!(response.kwargs.unwrap().contains_key("nested"));
                    session.leave().await.unwrap();
                    eprintln!("PASS original-go {transport}/{} auth/{method}", spec.subprotocol());
                }
                for method in ["ticket", "wampcra", "salted", "cryptosign"] {
                    assert!(
                        Client::new(spec.clone(), credentials(method, true))
                            .connect(&info[transport], "realm")
                            .await
                            .is_err(),
                        "accepted wrong {method} credential"
                    );
                }
                let missing = Client::new(spec.clone(), credentials("anonymous", false))
                    .connect(&info[transport], "missing")
                    .await;
                if transport == "ws" {
                    assert!(missing.is_err());
                } else {
                    // The pinned Go acceptor welcomes before checking realm
                    // membership and leaves the unattached connection open.
                    let accepted = missing.expect("reference RawSocket behavior changed");
                    accepted.close().await.unwrap();
                }
            }
        }
        child.kill().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires XCONN_REFERENCE_GO_BIN, independent pure-Go engine"]
async fn original_go_pubsub_errors_and_progressive_results() {
    bounded(async {
        let (mut child, info) = go_server().await;
        for transport in ["ws", "rs"] {
            for spec in specs() {
                let session = Client::new(spec.clone(), credentials("anonymous", false))
                    .connect(&info[transport], "realm")
                    .await
                    .unwrap();
                let error = session
                    .execute_call(CallRequest::new("suite.error"))
                    .await
                    .unwrap()
                    .error
                    .unwrap();
                assert_eq!(error.uri, "example.failure");
                assert_eq!(error.args, Some(vec![Value::Str("detail".into())]));
                assert_eq!(error.kwargs.unwrap()["reason"], Value::Str("test".into()));
                let mut stream = session.call_stream(CallRequest::new("suite.progress")).await.unwrap();
                for number in 1..=3 {
                    let response = stream.next().await.unwrap().unwrap();
                    assert!(response.error.is_none());
                    assert_eq!(response.args, Some(vec![Value::Int(number)]));
                    assert_eq!(response.details.get("progress") == Some(&Value::Bool(true)), number < 3);
                }
                assert!(stream.next().await.unwrap().is_none());
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let event_tx = tx.clone();
                let sub = session
                    .subscribe_handle(SubscribeRequest::new("suite.from-go", move |event| {
                        let tx = event_tx.clone();
                        async move {
                            let _ = tx.send(event);
                        }
                    }))
                    .await
                    .unwrap();
                assert!(
                    session
                        .execute_publish(
                            PublishRequest::new("suite.to-go")
                                .arg("event")
                                .kwarg("token", "value")
                                .option("acknowledge", true)
                        )
                        .await
                        .unwrap()
                        .unwrap()
                        .error
                        .is_none()
                );
                let event = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(event.args, vec![Value::Str("event".into())]);
                assert_eq!(event.kwargs["token"], Value::Str("value".into()));
                sub.unsubscribe().await.unwrap();
                for (topic, matching) in [
                    ("suite.events.item", "exact"),
                    ("suite.events.", "prefix"),
                    ("suite.*.item", "wildcard"),
                ] {
                    let tx = tx.clone();
                    let sub = session
                        .subscribe_handle(
                            SubscribeRequest::new(topic, move |event| {
                                let tx = tx.clone();
                                async move {
                                    let _ = tx.send(event);
                                }
                            })
                            .with_option("match", matching),
                        )
                        .await
                        .unwrap();
                    call(&session, "suite.publish", vec![Value::Str(matching.into())]).await;
                    let event = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(event.args, vec![Value::Str(matching.into())]);
                    sub.unsubscribe().await.unwrap();
                }
                session.leave().await.unwrap();
                eprintln!(
                    "PASS original-go {transport}/{} PubSub/errors/progressive-results",
                    spec.subprotocol()
                );
            }
        }
        child.kill().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires XCONN_REFERENCE_GO_BIN, independent pure-Go engine"]
async fn original_go_cancellation_modes_and_session_survival() {
    bounded(async {
        let (mut child, info) = go_server().await;
        for transport in ["ws", "rs"] {
            for spec in specs() {
                let session = Client::new(spec.clone(), credentials("anonymous", false))
                    .connect(&info[transport], "realm")
                    .await
                    .unwrap();
                for mode in ["skip", "kill", "killnowait"] {
                    let token = format!("{transport}-{}-{mode}", spec.subprotocol());
                    let mut stream = session
                        .call_stream(CallRequest::new("suite.wait").arg(token.clone()))
                        .await
                        .unwrap();
                    state(&session, &token, "started").await;
                    stream.cancel(mode).await.unwrap();
                    let response = tokio::time::timeout(Duration::from_secs(3), stream.next())
                        .await
                        .expect("cancellation reply timeout")
                        .unwrap()
                        .unwrap();
                    assert_eq!(response.error.unwrap().uri, "wamp.error.canceled");
                    if mode == "skip" {
                        state(&session, &token, "started").await;
                        call(&session, "suite.release", vec![Value::Str(token.clone())]).await;
                        state(&session, &token, "released").await;
                    } else {
                        state(&session, &token, "cancelled").await;
                    }
                    call(&session, "suite.payload", vec![Value::Str("still-alive".into())]).await;
                }
                session.leave().await.unwrap();
            }
        }
        child.kill().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires XCONN_REFERENCE_GO_BIN, independent pure-Go engine"]
async fn original_go_progressive_inputs() {
    bounded(async {
        let (mut child, info) = go_server().await;
        for transport in ["ws", "rs"] {
            for spec in specs() {
                let session = Client::new(spec.clone(), credentials("anonymous", false))
                    .connect(&info[transport], "realm")
                    .await
                    .unwrap();
                let token = format!("{transport}-{}", spec.subprotocol());
                let mut stream = session
                    .call_stream(
                        CallRequest::new("suite.inputs")
                            .arg(token.clone())
                            .arg(1)
                            .option("progress", true),
                    )
                    .await
                    .unwrap();
                assert_eq!(stream.next().await.unwrap().unwrap().args, Some(vec![Value::Int(1)]));
                stream
                    .send_input(vec![Value::Str(token.clone()), Value::Int(2)], HashMap::new(), true)
                    .await
                    .unwrap();
                assert_eq!(stream.next().await.unwrap().unwrap().args, Some(vec![Value::Int(3)]));
                stream
                    .send_input(vec![Value::Str(token), Value::Int(3)], HashMap::new(), false)
                    .await
                    .unwrap();
                let result = tokio::time::timeout(Duration::from_secs(3), stream.next())
                    .await
                    .expect("final progressive input timed out")
                    .unwrap()
                    .unwrap();
                assert!(result.error.is_none());
                assert_eq!(result.args, Some(vec![Value::Int(6)]));
                assert!(stream.next().await.unwrap().is_none());
                session.leave().await.unwrap();
            }
        }
        child.kill().await.unwrap();
    })
    .await;
}

struct Policy;
impl ServerAuthenticator for Policy {
    fn methods(&self) -> Vec<String> {
        vec![
            "anonymous".into(),
            "ticket".into(),
            "wampcra".into(),
            "cryptosign".into(),
        ]
    }

    fn authenticate(
        &self,
        request: &AuthRequest,
    ) -> std::result::Result<AuthResponse, wampproto::messages::types::Error> {
        let mut response = AuthResponse::new(
            if request.authid.is_empty() {
                "interop"
            } else {
                &request.authid
            },
            "trusted",
        );
        match request.method.as_str() {
            "ticket" if request.ticket.as_deref() != Some("secret") => {
                return Err(wampproto::messages::types::Error::new("bad ticket"));
            }
            "wampcra" => {
                response.cra = Some(CRAKey::from_secret(
                    "secret",
                    if request.authid == "salted" {
                        Some(Salt {
                            salt: "interop-salt".into(),
                            iterations: 1000,
                            key_length: 32,
                        })
                    } else {
                        None
                    },
                )?)
            }
            "cryptosign" => {
                let expected = credentials("cryptosign", false).auth_extra();
                if request.public_key.as_ref().map(|s| Value::Str(s.clone())).as_ref() != expected.get("pubkey") {
                    return Err(wampproto::messages::types::Error::new("unknown key"));
                }
            }
            _ => {}
        }
        Ok(response)
    }
}

async fn install_services(session: &Session) {
    let reply = session
        .execute_register(RegisterRequest::new("suite.payload", |inv| async move {
            RpcResult::new(inv.args, inv.kwargs)
        }))
        .await
        .unwrap();
    assert!(reply.error.is_none());
    session
        .execute_register(RegisterRequest::new("suite.error", |_| async {
            RpcResult {
                args: Some(vec![]),
                kwargs: Some(HashMap::new()),
                details: HashMap::new(),
                error: Some(WampError {
                    uri: "example.failure".into(),
                    args: Some(vec![Value::Str("detail".into())]),
                    kwargs: Some(HashMap::from([("reason".into(), Value::Str("test".into()))])),
                }),
            }
        }))
        .await
        .unwrap();
    session
        .register_context("suite.progress", HashMap::new(), |context| async move {
            context
                .send_progress(vec![Value::Int(1)], HashMap::new())
                .await
                .unwrap();
            context
                .send_progress(vec![Value::Int(2)], HashMap::new())
                .await
                .unwrap();
            RpcResult::arg(3)
        })
        .await
        .unwrap();

    let inputs = Arc::new(Mutex::new(HashMap::<String, (i64, i64)>::new()));
    session
        .register_context("suite.inputs", HashMap::new(), move |context| {
            let inputs = inputs.clone();
            async move {
                let [Value::Str(token), Value::Int(number)] = context.invocation.args.as_slice() else {
                    return RpcResult::error("wamp.error.invalid_argument");
                };
                let more = context.invocation.details.get("progress") == Some(&Value::Bool(true));
                let sum = {
                    let mut inputs = inputs.lock().unwrap();
                    let value = inputs.entry(token.clone()).or_insert((context.request_id, 0));
                    if value.0 != context.request_id {
                        return RpcResult::error("example.invocation_id_changed");
                    }
                    value.1 += number;
                    let sum = value.1;
                    if !more {
                        inputs.remove(token);
                    }
                    sum
                };
                if more {
                    context
                        .send_progress(vec![Value::Int(sum)], HashMap::new())
                        .await
                        .unwrap();
                    RpcResult::no_result()
                } else {
                    RpcResult::arg(sum)
                }
            }
        })
        .await
        .unwrap();
    let published = session.clone();
    session
        .execute_register(RegisterRequest::new("suite.publish", move |inv| {
            let session = published.clone();
            async move {
                session
                    .execute_publish(
                        PublishRequest::new("suite.events.item")
                            .args(inv.args)
                            .kwargs(inv.kwargs)
                            .option("acknowledge", true),
                    )
                    .await
                    .unwrap();
                RpcResult::default()
            }
        }))
        .await
        .unwrap();
    let published = session.clone();
    session
        .execute_subscribe(SubscribeRequest::new("suite.to-go", move |event| {
            let session = published.clone();
            async move {
                session
                    .execute_publish(
                        PublishRequest::new("suite.from-go")
                            .args(event.args)
                            .kwargs(event.kwargs)
                            .option("acknowledge", true),
                    )
                    .await
                    .unwrap();
            }
        }))
        .await
        .unwrap();
    let states = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let waiting = states.clone();
    session
        .register_context("suite.wait", HashMap::new(), move |context| {
            let states = waiting.clone();
            async move {
                let Some(Value::Str(token)) = context.invocation.args.first() else {
                    return RpcResult::error("wamp.error.invalid_argument");
                };
                let token = token.clone();
                states.lock().unwrap().insert(token.clone(), "started".into());
                context.cancelled().await;
                states.lock().unwrap().insert(token, "cancelled".into());
                RpcResult::error("wamp.error.canceled")
            }
        })
        .await
        .unwrap();
    session
        .execute_register(RegisterRequest::new("suite.state", move |inv| {
            let states = states.clone();
            async move {
                let Some(Value::Str(token)) = inv.args.first() else {
                    return RpcResult::error("wamp.error.invalid_argument");
                };
                let value = states.lock().unwrap().get(token).cloned().unwrap_or_default();
                RpcResult::arg(value)
            }
        }))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires XCONN_REFERENCE_GO_BIN, independent pure-Go engine"]
async fn original_go_client_against_rust_router() {
    bounded(async {
        let binary = verify_reference().await;
        let router = Router::new();
        router
            .add_realm(
                "realm",
                RealmConfig {
                    authentication: Arc::new(|| Box::new(Policy)),
                    ..Default::default()
                },
            )
            .unwrap();
        let local = router
            .connect_internal("realm", "service", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        install_services(&local).await;
        let ws = Server::websocket(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let rs = Server::rawsocket(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        for uri in [format!("ws://{}", ws.local_addr), format!("rs://{}", rs.local_addr)] {
            let output = tokio::process::Command::new(&binary)
                .arg("suite-client")
                .arg(&uri)
                .kill_on_drop(true)
                .output()
                .await
                .unwrap();
            assert!(
                output.status.success(),
                "{uri}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        }
        local.leave().await.unwrap();
        router.close();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires XCONN_REFERENCE_GO_BIN, independent pure-Go engine"]
async fn client_transport_loss_releases_pending_requests() {
    bounded(async {
        let (mut child, info) = go_server().await;
        for transport in ["ws", "rs"] {
            for spec in specs() {
                let observer = Client::new(spec.clone(), credentials("anonymous", false))
                    .connect(&info[transport], "realm")
                    .await
                    .unwrap();
                let caller = Client::new(spec.clone(), credentials("anonymous", false))
                    .connect(&info[transport], "realm")
                    .await
                    .unwrap();
                let token = format!("loss-{transport}-{}", spec.subprotocol());
                let mut pending = caller
                    .call_stream(CallRequest::new("suite.wait").arg(token.clone()))
                    .await
                    .unwrap();
                state(&observer, &token, "started").await;
                caller.close().await.unwrap();
                let failure = tokio::time::timeout(Duration::from_secs(2), pending.next())
                    .await
                    .expect("pending RPC survived transport loss");
                assert!(failure.is_err());
                // The legacy router does not issue disconnect interrupts; explicitly release
                // this test handler so reference behavior cannot leave a blocked callee.
                call(&observer, "suite.release", vec![Value::Str(token)]).await;
                observer.leave().await.unwrap();
            }
        }
        child.kill().await.unwrap();
    })
    .await;
}
