#![cfg(feature = "async")]
use std::{collections::HashMap, sync::Arc, time::Duration};
use wampproto::authenticators::{
    anonymous::AnonymousAuthenticator,
    authenticator::ClientAuthenticator,
    cryptosign::CryptoSignAuthenticator,
    server::{AuthRequest, AuthResponse, CRAKey, ServerAuthenticator},
    ticket::TicketAuthenticator,
    wampcra::WAMPCRAAuthenticator,
};
use xconn_core::Result as RpcResult;
use xconn_core::{
    async_::{
        client::Client,
        router::{RealmConfig, Router},
        server::Server,
        types::{RegisterRequest, SubscribeRequest},
    },
    *,
};

fn auth() -> Box<dyn ClientAuthenticator + Send + Sync> {
    Box::new(AnonymousAuthenticator::default())
}

async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(8), future)
        .await
        .expect("operation timed out")
}

fn specs() -> Vec<Box<dyn SerializerSpec>> {
    vec![
        Box::new(JSONSerializerSpec),
        Box::new(CBORSerializerSpec),
        Box::new(MsgPackSerializerSpec),
    ]
}

#[tokio::test]
async fn client_router_matrix() {
    bounded(async {
        for transport in ["memory", "ws", "rs"] {
            for spec in specs() {
                let router = Router::new();
                router.add_realm("realm", RealmConfig::default()).unwrap();
                let server = match transport {
                    "ws" => Some(
                        Server::websocket(router.clone(), "127.0.0.1:0".parse().unwrap())
                            .await
                            .unwrap(),
                    ),
                    "rs" => Some(
                        Server::rawsocket(router.clone(), "127.0.0.1:0".parse().unwrap())
                            .await
                            .unwrap(),
                    ),
                    _ => None,
                };
                let connect = || {
                    let router = router.clone();
                    let spec = spec.clone();
                    let uri = server.as_ref().map(|s| format!("{transport}://{}", s.local_addr));
                    async move {
                        if let Some(uri) = uri {
                            Client::new(spec, auth()).connect(&uri, "realm").await.unwrap()
                        } else {
                            router.connect("realm", spec, auth()).await.unwrap()
                        }
                    }
                };
                let callee = connect().await;
                let caller = connect().await;
                let registration = callee
                    .execute_register(RegisterRequest::new("echo", |inv| async move {
                        RpcResult::new(inv.args, inv.kwargs)
                    }))
                    .await
                    .unwrap();
                assert!(registration.error.is_none());
                let reply = caller
                    .execute_call(CallRequest::new("echo").arg("hello"))
                    .await
                    .unwrap();
                assert_eq!(reply.args.unwrap(), vec![Value::Str("hello".into())]);
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let sub = callee
                    .execute_subscribe(SubscribeRequest::new("test", move |event| {
                        let tx = tx.clone();
                        async move {
                            let _ = tx.send(event.args);
                        }
                    }))
                    .await
                    .unwrap();
                assert!(sub.error.is_none());
                let ack = caller
                    .execute_publish(PublishRequest::new("test").arg(42).option("acknowledge", true))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(ack.error.is_none());
                assert_eq!(rx.recv().await.unwrap(), vec![Value::Int(42)]);
                assert!(callee.unsubscribe(sub.subscription_id).await.unwrap().is_none());
                assert!(callee.unregister(registration.registration_id).await.unwrap().is_none());
                let reply = caller.execute_call(CallRequest::new("echo")).await.unwrap();
                assert_eq!(reply.error.unwrap().uri, "wamp.error.no_such_procedure");
                // Remote request errors must be surfaced immediately rather than timing out.
                assert!(callee.unregister(registration.registration_id).await.unwrap().is_some());
                assert!(callee.unsubscribe(sub.subscription_id).await.unwrap().is_some());
                caller.leave().await.unwrap();
                callee.leave().await.unwrap();
                router.close();
            }
        }
    })
    .await;
}

#[tokio::test]
async fn progressive_results_errors_and_cancel() {
    bounded(async {
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let callee = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let caller = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        callee
            .register_context("progress", HashMap::new(), |context| async move {
                context
                    .send_progress(vec![Value::Int(1)], HashMap::new())
                    .await
                    .unwrap();
                RpcResult::arg(2)
            })
            .await
            .unwrap();
        let mut call = caller.call_stream(CallRequest::new("progress")).await.unwrap();
        assert_eq!(call.next().await.unwrap().unwrap().args.unwrap(), vec![Value::Int(1)]);
        assert_eq!(call.next().await.unwrap().unwrap().args.unwrap(), vec![Value::Int(2)]);
        assert!(call.next().await.unwrap().is_none());
        callee
            .execute_register(RegisterRequest::new("error", |_| async {
                RpcResult::error("example.failed")
            }))
            .await
            .unwrap();
        assert_eq!(
            caller
                .execute_call(CallRequest::new("error"))
                .await
                .unwrap()
                .error
                .unwrap()
                .uri,
            "example.failed"
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        callee
            .register_context("wait", HashMap::new(), move |context| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(0);
                    context.cancelled().await;
                    let _ = tx.send(1);
                    RpcResult::arg(0)
                }
            })
            .await
            .unwrap();
        let mut call = caller.call_stream(CallRequest::new("wait")).await.unwrap();
        assert_eq!(rx.recv().await, Some(0));
        call.cancel("killnowait").await.unwrap();
        assert!(call.next().await.unwrap().unwrap().error.is_some());
        assert_eq!(rx.recv().await, Some(1));
        caller.leave().await.unwrap();
        callee.leave().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn disconnect_unblocks_pending_call() {
    bounded(async {
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let callee = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let caller = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        callee
            .register_context("wait", HashMap::new(), move |context| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(());
                    context.cancelled().await;
                    RpcResult::arg(0)
                }
            })
            .await
            .unwrap();
        let mut call = caller.call_stream(CallRequest::new("wait")).await.unwrap();
        rx.recv().await.unwrap();
        callee.close().await.unwrap();
        assert!(call.next().await.unwrap().unwrap().error.is_some());
        caller.leave().await.unwrap();
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
        if request.method == "ticket" && request.ticket.as_deref() != Some("secret") {
            return Err(wampproto::messages::types::Error::new("bad ticket"));
        }
        let mut response = AuthResponse::new("user", "trusted");
        if request.method == "wampcra" {
            response.cra = Some(CRAKey::from_secret("secret", None)?);
        }
        Ok(response)
    }
}

#[tokio::test]
async fn authentication_matrix_and_authorization() {
    bounded(async {
        let router = Router::new();
        let config = RealmConfig {
            authentication: Arc::new(|| Box::new(Policy)),
            ..Default::default()
        };
        router.add_realm("realm", config).unwrap();
        for spec in specs() {
            let methods: Vec<Box<dyn ClientAuthenticator + Send + Sync>> = vec![
                auth(),
                Box::new(TicketAuthenticator::new("user", "secret", HashMap::new())),
                Box::new(WAMPCRAAuthenticator::new("user", "secret", HashMap::new())),
                Box::new(CryptoSignAuthenticator::try_new("user", &"11".repeat(32), HashMap::new()).unwrap()),
            ];
            for auth in methods {
                let session = router.connect("realm", spec.clone(), auth).await.unwrap();
                session.leave().await.unwrap();
            }
        }
        assert!(
            router
                .connect(
                    "realm",
                    Box::new(JSONSerializerSpec),
                    Box::new(TicketAuthenticator::new("user", "bad", HashMap::new()))
                )
                .await
                .is_err()
        );
        assert!(
            router
                .connect("missing", Box::new(JSONSerializerSpec), auth())
                .await
                .is_err()
        );
        let config = RealmConfig {
            roles: HashMap::new(),
            ..Default::default()
        };
        router.add_realm("denied", config).unwrap();
        let denied = router
            .connect("denied", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let reply = denied.execute_call(CallRequest::new("echo")).await.unwrap();
        assert_eq!(reply.error.unwrap().uri, "wamp.error.authorization_failed");
        denied.leave().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn progressive_input_and_kill_acknowledgement() {
    bounded(async {
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let callee = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let caller = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let total = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let sum = total.clone();
        let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
        callee
            .register_context("sum", HashMap::new(), move |context| {
                let total = sum.clone();
                let seen = seen.clone();
                async move {
                    if let Some(Value::Int(v)) = context.invocation.args.first() {
                        total.fetch_add(*v, std::sync::atomic::Ordering::SeqCst);
                    }
                    if context.invocation.details.get("progress") == Some(&Value::Bool(true)) {
                        let _ = seen.send(context.request_id);
                        RpcResult::no_result()
                    } else {
                        RpcResult::arg(total.load(std::sync::atomic::Ordering::SeqCst))
                    }
                }
            })
            .await
            .unwrap();
        let mut call = caller
            .call_stream(CallRequest::new("sum").arg(1).option("progress", true))
            .await
            .unwrap();
        let id = received.recv().await.unwrap();
        call.send_input(vec![Value::Int(2)], HashMap::new(), true)
            .await
            .unwrap();
        assert_eq!(received.recv().await.unwrap(), id);
        call.send_input(vec![Value::Int(3)], HashMap::new(), false)
            .await
            .unwrap();
        assert_eq!(call.next().await.unwrap().unwrap().args.unwrap(), vec![Value::Int(6)]);
        let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
        callee
            .register_context("wait-kill", HashMap::new(), move |context| {
                let seen = seen.clone();
                async move {
                    let _ = seen.send(());
                    context.cancelled().await;
                    RpcResult::arg(0)
                }
            })
            .await
            .unwrap();
        let mut call = caller.call_stream(CallRequest::new("wait-kill")).await.unwrap();
        received.recv().await.unwrap();
        call.cancel("kill").await.unwrap();
        assert_eq!(
            call.next().await.unwrap().unwrap().error.unwrap().uri,
            "wamp.error.canceled"
        );
        caller.leave().await.unwrap();
        callee.leave().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn session_meta_api_queries_events_and_kill() {
    bounded(async {
        let router = Router::new();
        router
            .add_realm(
                "realm",
                RealmConfig {
                    meta_api: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let admin = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        admin
            .execute_subscribe(SubscribeRequest::new("wamp.session.on_join", move |event| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(event.args);
                }
            }))
            .await
            .unwrap();
        let client = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        assert!(matches!(rx.recv().await.unwrap()[0], Value::Dict(_)));
        let count = admin
            .execute_call(CallRequest::new("wamp.session.count"))
            .await
            .unwrap();
        assert_eq!(count.args.unwrap(), vec![Value::Int(2)]);
        let details = admin
            .execute_call(CallRequest::new("wamp.session.get").arg(client.details().id()))
            .await
            .unwrap();
        assert!(matches!(details.args.unwrap()[0], Value::Dict(_)));
        assert!(
            admin
                .execute_call(CallRequest::new("wamp.session.kill").arg(admin.details().id()))
                .await
                .unwrap()
                .error
                .is_some()
        );
        assert!(
            admin
                .execute_call(CallRequest::new("wamp.session.kill").arg(client.details().id()))
                .await
                .unwrap()
                .error
                .is_none()
        );
        client.wait_disconnect().await;
        admin.leave().await.unwrap();
        router.close();
    })
    .await;
}

#[tokio::test]
async fn shared_subscription_handles_and_cancelled_futures() {
    bounded(async {
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let session = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let first_tx = tx.clone();
        let first = session
            .subscribe_handle(SubscribeRequest::new("topic", move |_| {
                let tx = first_tx.clone();
                async move {
                    let _ = tx.send(1);
                }
            }))
            .await
            .unwrap();
        let second = session
            .subscribe_handle(SubscribeRequest::new("topic", move |_| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(2);
                }
            }))
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
        first.unsubscribe().await.unwrap();
        session
            .execute_publish(
                PublishRequest::new("topic")
                    .option("acknowledge", true)
                    .option("exclude_me", false),
            )
            .await
            .unwrap();
        assert_eq!(rx.recv().await, Some(2));
        assert!(rx.try_recv().is_err());
        second.unsubscribe().await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        session
            .register_context("wait", HashMap::new(), move |context| {
                let tx = tx.clone();
                async move {
                    context.cancelled().await;
                    let _ = tx.send(());
                    RpcResult::arg(0)
                }
            })
            .await
            .unwrap();
        let call = session.call_stream(CallRequest::new("wait")).await.unwrap();
        drop(call);
        rx.recv().await.unwrap();
        assert!(session.connected());
        session.leave().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn portable_management_is_protected_and_controls_sessions() {
    bounded(async {
        use xconn_core::async_::management::*;
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let management = Management::start(router.clone(), None).await.unwrap();
        let guest = router
            .connect(REALM, Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        assert!(
            guest
                .execute_call(CallRequest::new(LIST_REALMS))
                .await
                .unwrap()
                .error
                .is_some()
        );
        let admin = router
            .connect_internal(REALM, "admin", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let user = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let response = admin.execute_call(CallRequest::new(LIST_REALMS)).await.unwrap();
        assert!(response.error.is_none());
        let response = admin
            .execute_call(CallRequest::new(LIST_SESSIONS).arg("realm").kwarg("limit", 1))
            .await
            .unwrap();
        assert!(response.error.is_none());
        assert_eq!(response.kwargs.unwrap()["total"], Value::Int(1));
        let response = admin
            .execute_call(CallRequest::new(KILL_SESSION).arg("realm").arg(user.details().id()))
            .await
            .unwrap();
        assert!(response.error.is_none());
        user.wait_disconnect().await;
        guest.leave().await.unwrap();
        admin.leave().await.unwrap();
        management.close().await;
        router.close();
    })
    .await;
}

#[tokio::test]
async fn authorization_callbacks_can_query_router() {
    bounded(async {
        let router = Router::new();
        let policy_router = router.clone();
        let config = RealmConfig {
            roles: HashMap::new(),
            authorizer: Some(Arc::new(move |details, _| {
                policy_router.sessions(&details.realm).is_ok()
            })),
            ..Default::default()
        };
        router.add_realm("realm", config).unwrap();
        let client = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let registration = client
            .execute_register(RegisterRequest::new("echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        assert!(registration.error.is_none());
        assert!(
            client
                .execute_call(CallRequest::new("echo"))
                .await
                .unwrap()
                .error
                .is_none()
        );
        client.leave().await.unwrap();
        router.close();
    })
    .await;
}

#[tokio::test]
async fn pattern_fanout_disclosure_and_shared_registration() {
    bounded(async {
        let router = Router::new();
        router
            .add_realm(
                "realm",
                RealmConfig {
                    disclose_caller: true,
                    disclose_publisher: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let subscriber = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let publisher = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for (topic, matching) in [
            ("com.example.event", "exact"),
            ("com.example.", "prefix"),
            ("com..event", "wildcard"),
        ] {
            let tx = tx.clone();
            subscriber
                .execute_subscribe(
                    SubscribeRequest::new(topic, move |event| {
                        let tx = tx.clone();
                        async move {
                            let _ = tx.send(event.details);
                        }
                    })
                    .with_option("match", matching),
                )
                .await
                .unwrap();
        }
        publisher
            .execute_publish(PublishRequest::new("com.example.event").option("acknowledge", true))
            .await
            .unwrap();
        for _ in 0..3 {
            let details = rx.recv().await.unwrap();
            assert_eq!(details["publisher"], Value::Int(publisher.details().id()));
        }
        let first = subscriber
            .execute_register(
                RegisterRequest::new("shared", |inv| async move {
                    assert!(inv.details.contains_key("caller"));
                    RpcResult::arg(1)
                })
                .with_option("invoke", "roundrobin"),
            )
            .await
            .unwrap();
        let second = publisher
            .execute_register(
                RegisterRequest::new("shared", |_| async { RpcResult::arg(2) }).with_option("invoke", "roundrobin"),
            )
            .await
            .unwrap();
        assert!(first.error.is_none());
        assert!(second.error.is_none());
        let mut replies = Vec::new();
        for _ in 0..2 {
            replies.push(
                publisher
                    .execute_call(CallRequest::new("shared"))
                    .await
                    .unwrap()
                    .args
                    .unwrap()[0]
                    .clone(),
            );
        }
        assert!(replies.contains(&Value::Int(1)));
        assert!(replies.contains(&Value::Int(2)));
        subscriber.unregister(first.registration_id).await.unwrap();
        assert_eq!(
            publisher
                .execute_call(CallRequest::new("shared"))
                .await
                .unwrap()
                .args
                .unwrap(),
            vec![Value::Int(2)]
        );
        subscriber.leave().await.unwrap();
        publisher.leave().await.unwrap();
        router.close();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progressive_chunks_start_in_wire_order_without_serializing_completion() {
    bounded(async {
        let router = Router::new();
        router.add_realm("ordered", RealmConfig::default()).unwrap();
        let callee = router
            .connect("ordered", Box::new(CBORSerializerSpec), auth())
            .await
            .unwrap();
        let caller = router
            .connect("ordered", Box::new(CBORSerializerSpec), auth())
            .await
            .unwrap();
        let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(tokio::sync::Notify::new());
        let release = gate.clone();
        callee
            .register_context("chunks", HashMap::new(), move |context| {
                let seen = seen.clone();
                let gate = gate.clone();
                async move {
                    seen.send(context.invocation.args[0].clone()).unwrap();
                    if context.invocation.details.get("progress") == Some(&Value::Bool(true)) {
                        gate.notified().await;
                        RpcResult::no_result()
                    } else {
                        RpcResult::arg("done")
                    }
                }
            })
            .await
            .unwrap();
        let mut call = caller
            .call_stream(CallRequest::new("chunks").arg(0).option("progress", true))
            .await
            .unwrap();
        for chunk in 1..128 {
            call.send_input(vec![Value::Int(chunk)], HashMap::new(), chunk < 127)
                .await
                .unwrap();
        }
        for expected in 0..128 {
            assert_eq!(received.recv().await.unwrap(), Value::Int(expected));
        }
        assert_eq!(
            call.next().await.unwrap().unwrap().args.unwrap(),
            vec![Value::Str("done".into())]
        );
        release.notify_waiters();
        caller.close().await.unwrap();
        callee.close().await.unwrap();
        router.close();
    })
    .await;
}

#[tokio::test]
async fn trusted_attachment_retains_identity_and_survives_id_reuse() {
    bounded(async {
        use wampproto::messages::{register::Register, registered::Registered};
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let details =
            wampproto::types::SessionDetails::new(123, "realm".into(), "local".into(), "trusted".into(), false);
        let attach = || router.connect_internal_peer_with_details(details.clone(), Box::new(JSONSerializerSpec), 16);
        let (old, metadata) = attach().await.unwrap();
        assert_eq!(metadata.id(), 123);
        assert_eq!(metadata.authid(), "local");
        assert!(attach().await.is_err());
        router.kill_session("realm", 123, "wamp.close.normal").unwrap();
        let (replacement, _) = attach().await.unwrap();
        old.close().await.unwrap();
        // Allow both normal completion and the old RAII guard to run.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let codec = JSONSerializerSpec.serializer();
        let request = Register {
            request_id: 1,
            options: HashMap::new(),
            procedure: "echo".into(),
        };
        replacement.write(codec.serialize(&request).unwrap()).await.unwrap();
        let response = codec.deserialize(replacement.read().await.unwrap()).unwrap();
        assert!(response.as_any().is::<Registered>(), "{:?}", response.marshal());
        replacement.close().await.unwrap();
        router.close();
        let mut invalid = details.clone();
        invalid.id = 0;
        assert!(
            router
                .connect_internal_peer_with_details(invalid, Box::new(JSONSerializerSpec), 16)
                .await
                .is_err()
        );
    })
    .await;
}

#[test]
fn websocket_negotiation_honors_offered_order_and_supported_filter() {
    use xconn_core::async_::server::{select_websocket_protocol, websocket_protocols};
    let supported = websocket_protocols();
    assert_eq!(
        select_websocket_protocol(" unknown, wamp.2.cbor, wamp.2.json ", &supported),
        Some("wamp.2.cbor")
    );
    assert_eq!(
        select_websocket_protocol("wamp.2.cbor, wamp.2.json", &["wamp.2.json".into()]),
        Some("wamp.2.json")
    );
    assert_eq!(select_websocket_protocol("", &supported), None);
    assert_eq!(select_websocket_protocol("unknown", &["unknown".into()]), None);
    assert_eq!(select_websocket_protocol("wamp.2.json", &[]), None);
}

#[tokio::test]
async fn welcome_meta_api_features_follow_the_selected_realm() {
    bounded(async {
        let router = Router::new();
        for enabled in [false, true] {
            let realm = if enabled { "meta" } else { "basic" };
            router
                .add_realm(
                    realm,
                    RealmConfig {
                        meta_api: enabled,
                        ..Default::default()
                    },
                )
                .unwrap();
            for codec in specs() {
                let session = router.connect(realm, codec, auth()).await.unwrap();
                for name in ["broker", "dealer"] {
                    let Value::Dict(role) = &session.details().roles()[name] else {
                        panic!("invalid role")
                    };
                    let Value::Dict(features) = &role["features"] else {
                        panic!("invalid features")
                    };
                    assert_eq!(features.get("session_meta_api"), enabled.then_some(&Value::Bool(true)));
                }
                let count = session.call("wamp.session.count").execute().await.unwrap();
                if enabled {
                    assert!(count.error.is_none());
                    assert_eq!(count.args, Some(vec![Value::Int(1)]));
                } else {
                    assert_eq!(count.error.unwrap().uri, "wamp.error.no_such_procedure");
                }
                session.leave().await.unwrap();
            }
        }
        router.close();
    })
    .await;
}

#[tokio::test]
async fn unsolicited_intermediate_result_fails_the_call_and_closes_the_transport() {
    use xconn_core::async_::peer::MemoryPeer;
    bounded(async {
        let (peer, remote) = MemoryPeer::pair(8);
        let session = xconn_core::async_::session::Session::new(
            SessionDetails::new(1, "realm".into(), String::new(), String::new()),
            peer,
            JSONSerializerSpec.serializer(),
        );
        let calling = session.clone();
        let call = tokio::spawn(async move { calling.call("echo").option("receive_progress", false).execute().await });
        let wire = JSONSerializerSpec
            .serializer()
            .deserialize(remote.read().await.unwrap())
            .unwrap()
            .marshal();
        let Value::Int(id) = wire[1] else {
            panic!("missing request ID")
        };
        remote
            .write(format!("[50,{id},{{\"progress\":true}},[1]]").into_bytes())
            .await
            .unwrap();
        assert!(call.await.unwrap().is_err());
        session.wait_disconnect().await;
        assert!(!session.connected());
    })
    .await;
}
