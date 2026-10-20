use std::time::Duration;

use bytes::Bytes;
use xconn_webrtc::Connection;

async fn forward_candidates(from: Connection, to: Connection) {
    while let Some(candidate) = from.next_candidate().await {
        let complete = candidate.candidate.is_empty();
        to.add_candidate(candidate).await.unwrap();
        if complete {
            break;
        }
    }
}

#[tokio::test]
async fn trickle_connects_without_candidates_in_initial_sdp_and_buffers_early_candidates() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let a = Connection::new(vec![]).await.unwrap();
        let b = Connection::new(vec![]).await.unwrap();
        let local = a.open_raw_channel("file", Default::default()).await.unwrap();
        assert!(a.wait_connected(Duration::from_millis(5)).await.is_err());
        let offer = a.offer_trickle().await.unwrap();
        assert!(!offer.description.sdp.contains("a=candidate:"));
        let candidate = a.next_candidate().await.unwrap();
        assert!(!candidate.candidate.is_empty());
        // Candidates can arrive before the corresponding remote description.
        b.add_candidate(candidate).await.unwrap();
        let answer = b.answer_trickle(offer).await.unwrap();
        assert!(!answer.description.sdp.contains("a=candidate:"));
        let candidate = b.next_candidate().await.unwrap();
        assert!(!candidate.candidate.is_empty());
        a.add_candidate(candidate).await.unwrap();
        a.set_remote(answer).await.unwrap();
        let a_to_b = tokio::spawn(forward_candidates(a.clone(), b.clone()));
        let b_to_a = tokio::spawn(forward_candidates(b.clone(), a.clone()));
        let remote = b.accept_channel().await.unwrap();
        local.wait_open(Duration::from_secs(5)).await.unwrap();
        remote.wait_open(Duration::from_secs(5)).await.unwrap();
        assert_eq!(remote.protocol(), xconn_webrtc::RAW_CHANNEL_PROTOCOL);
        remote.send_packet(Bytes::from_static(b"server first")).await.unwrap();
        assert_eq!(local.read_packet().await.unwrap().as_ref(), b"server first");
        a_to_b.await.unwrap();
        b_to_a.await.unwrap();
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn receive_overflow_closes_the_channel_without_closing_its_connection() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let a = Connection::new(vec![]).await.unwrap();
        let b = Connection::new(vec![]).await.unwrap();
        let sender = a.open_raw_channel("overflow", Default::default()).await.unwrap();
        let answer = b.answer(a.offer().await.unwrap()).await.unwrap();
        a.set_remote(answer).await.unwrap();
        let receiver = b.accept_channel().await.unwrap();
        sender.wait_open(Duration::from_secs(5)).await.unwrap();
        for _ in 0..100 {
            if sender.send_packet(Bytes::from_static(b"unconsumed")).await.is_err() {
                break;
            }
        }
        receiver.closed().await;
        assert_eq!(
            receiver.failure().await.as_deref(),
            Some("WebRTC receive queue overflow")
        );
        loop {
            match receiver.read_packet().await {
                Ok(_) => {}
                Err(error) => {
                    assert!(error.to_string().contains("overflow"));
                    break;
                }
            }
        }
        sender.closed().await;
        let healthy = a.open_raw_channel("healthy", Default::default()).await.unwrap();
        let remote = b.accept_channel().await.unwrap();
        healthy.wait_open(Duration::from_secs(5)).await.unwrap();
        healthy
            .send_packet(Bytes::from_static(b"still connected"))
            .await
            .unwrap();
        assert_eq!(remote.read_packet().await.unwrap().as_ref(), b"still connected");
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[cfg(feature = "wamp")]
mod wamp {
    use super::*;
    use std::sync::Arc;
    use xconn_core::{
        JSONSerializerSpec,
        async_::router::{RealmConfig, Router},
        wampproto::authenticators::{anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator},
    };
    use xconn_webrtc::DataChannel;
    use xconn_webrtc::wamp::{Timeouts, WebRTCSession, accept_channel_with_timeout};

    fn auth() -> Box<dyn ClientAuthenticator + Send + Sync> {
        Box::new(AnonymousAuthenticator::default())
    }

    async fn pair() -> (Connection, Connection, Arc<DataChannel>, Arc<DataChannel>) {
        let a = Connection::new(vec![]).await.unwrap();
        let b = Connection::new(vec![]).await.unwrap();
        let local = a.open_channel("data").await.unwrap();
        let answer = b.answer(a.offer().await.unwrap()).await.unwrap();
        a.set_remote(answer).await.unwrap();
        let remote = b.accept_channel().await.unwrap();
        local.wait_open(Duration::from_secs(5)).await.unwrap();
        remote.wait_open(Duration::from_secs(5)).await.unwrap();
        (a, b, local, remote)
    }

    #[tokio::test]
    async fn raw_delivery_requires_successful_wamp_authentication() {
        use xconn_webrtc::wamp::serve_connection_with_raw;

        tokio::time::timeout(Duration::from_secs(15), async {
            for realm in ["realm", "missing"] {
                let router = Router::new();
                router.add_realm("realm", RealmConfig::default()).unwrap();
                let client = Connection::new(vec![]).await.unwrap();
                let server = Connection::new(vec![]).await.unwrap();
                let raw = client.open_raw_channel("file", Default::default()).await.unwrap();
                let answer = server.answer(client.offer().await.unwrap()).await.unwrap();
                client.set_remote(answer).await.unwrap();
                let (raw_tx, mut raw_rx) = tokio::sync::mpsc::channel(1);
                let worker = tokio::spawn(serve_connection_with_raw(router, server.clone(), Some(raw_tx)));
                raw.wait_open(Duration::from_secs(5)).await.unwrap();
                raw.send_text("before authentication").await.unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_millis(30), raw_rx.recv())
                        .await
                        .is_err()
                );

                let channel = client.open_channel("data").await.unwrap();
                let joined =
                    WebRTCSession::join(client.clone(), channel, realm, Box::new(JSONSerializerSpec), auth()).await;
                if realm == "realm" {
                    let session = joined.unwrap();
                    let accepted = raw_rx.recv().await.unwrap().into_channel().await;
                    let packet = accepted.read_data_packet().await.unwrap();
                    assert!(packet.is_string);
                    assert_eq!(packet.data.as_ref(), b"before authentication");
                    let legacy = client.open_channel("legacy").await.unwrap();
                    legacy.wait_open(Duration::from_secs(5)).await.unwrap();
                    legacy
                        .send_packet(Bytes::from_static(b"binary first packet"))
                        .await
                        .unwrap();
                    let accepted_legacy = raw_rx.recv().await.unwrap().into_channel().await;
                    let first = accepted_legacy.read_data_packet().await.unwrap();
                    assert!(!first.is_string);
                    assert_eq!(first.data.as_ref(), b"binary first packet");
                    session.close().await.unwrap();
                    raw.send_text("after session close").await.unwrap();
                    assert_eq!(accepted.read_packet().await.unwrap().as_ref(), b"after session close");
                } else {
                    assert!(joined.is_err());
                    assert!(
                        tokio::time::timeout(Duration::from_millis(30), raw_rx.recv())
                            .await
                            .is_err()
                    );
                }
                client.close().await.unwrap();
                server.close().await.unwrap();
                let _ = worker.await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn session_timeout_and_cancellation_close_only_the_pending_channel() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let (a, b, local, remote) = pair().await;
            let result = WebRTCSession::join_with_timeouts(
                a.clone(),
                local,
                "realm",
                Box::new(JSONSerializerSpec),
                auth(),
                Timeouts {
                    open_session: Duration::from_millis(30),
                    ..Timeouts::default()
                },
            )
            .await;
            assert!(result.err().unwrap().to_string().contains("timeout"));
            // The initial handshake was sent; no WAMP reply was supplied.
            assert_eq!(remote.read_packet().await.unwrap().len(), 4);
            assert!(remote.read_packet().await.is_err());

            let local = a.open_channel("cancelled").await.unwrap();
            let remote = b.accept_channel().await.unwrap();
            let task = tokio::spawn(WebRTCSession::join(
                a.clone(),
                local,
                "realm",
                Box::new(JSONSerializerSpec),
                auth(),
            ));
            assert_eq!(remote.read_packet().await.unwrap().len(), 4);
            task.abort();
            assert!(task.await.err().unwrap().is_cancelled());
            assert!(remote.read_packet().await.is_err());

            // An independent channel on the same connection remains usable.
            let local = a.open_raw_channel("healthy", Default::default()).await.unwrap();
            let remote = b.accept_channel().await.unwrap();
            local.wait_open(Duration::from_secs(5)).await.unwrap();
            remote.wait_open(Duration::from_secs(5)).await.unwrap();
            local.write_message(b"still connected").await.unwrap();
            assert_eq!(remote.read_message().await.unwrap(), b"still connected");
            a.close().await.unwrap();
            b.close().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stalled_signaling_can_time_out_or_be_cancelled_and_then_retry() {
        use xconn_core::{CallRequest, Result as RpcResult, async_::types::RegisterRequest};
        use xconn_webrtc::wamp::{Provider, SignalingConfig, connect_with_timeouts};

        fn config(procedure: &str, provider: bool) -> SignalingConfig {
            SignalingConfig {
                offer_procedure: procedure.into(),
                remote_candidates_topic: if provider { "to-provider" } else { "to-client" }.into(),
                local_candidates_topic: if provider { "to-client" } else { "to-provider" }.into(),
                ice_servers: vec![],
            }
        }

        tokio::time::timeout(Duration::from_secs(15), async {
            let router = Router::new();
            router.add_realm("realm", RealmConfig::default()).unwrap();
            let provider_session = router
                .connect("realm", Box::new(JSONSerializerSpec), auth())
                .await
                .unwrap();
            let client = router
                .connect("realm", Box::new(JSONSerializerSpec), auth())
                .await
                .unwrap();
            let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
            let release = Arc::new(tokio::sync::Notify::new());
            let handler_release = release.clone();
            provider_session
                .execute_register(RegisterRequest::new("hang", move |_| {
                    let started = started_tx.clone();
                    let release = handler_release.clone();
                    async move {
                        started.send(()).unwrap();
                        release.notified().await;
                        RpcResult::arg("released")
                    }
                }))
                .await
                .unwrap();
            let signal = client.clone();
            let attempt = tokio::spawn(async move {
                connect_with_timeouts(
                    &signal,
                    config("hang", false),
                    "realm",
                    Box::new(JSONSerializerSpec),
                    auth(),
                    Timeouts {
                        connect: Duration::from_millis(100),
                        ..Timeouts::default()
                    },
                )
                .await
            });
            started_rx.recv().await.unwrap();
            let error = attempt.await.unwrap().err().unwrap();
            assert!(error.to_string().contains("connect timeout"));

            let signal = client.clone();
            let attempt = tokio::spawn(async move {
                connect_with_timeouts(
                    &signal,
                    config("hang", false),
                    "realm",
                    Box::new(JSONSerializerSpec),
                    auth(),
                    Timeouts::default(),
                )
                .await
            });
            started_rx.recv().await.unwrap();
            attempt.abort();
            assert!(attempt.await.err().unwrap().is_cancelled());
            release.notify_waiters();

            // Both signaling sessions remain usable for a real subsequent connection.
            let provider = Provider::start(provider_session.clone(), router.clone(), config("offer", true))
                .await
                .unwrap();
            provider_session
                .execute_register(RegisterRequest::new("echo", |inv| async move {
                    RpcResult::new(inv.args, inv.kwargs)
                }))
                .await
                .unwrap();
            let session = connect_with_timeouts(
                &client,
                config("offer", false),
                "realm",
                Box::new(JSONSerializerSpec),
                auth(),
                Timeouts {
                    connect: Duration::from_secs(5),
                    ..Timeouts::default()
                },
            )
            .await
            .unwrap();
            let response = session
                .execute_call(CallRequest::new("echo").arg("retry"))
                .await
                .unwrap();
            assert!(response.error.is_none());
            assert_eq!(response.args.unwrap(), vec![xconn_core::Value::Str("retry".into())]);
            session.close().await.unwrap();
            session.connection.close().await.unwrap();
            provider.close().await;
            client.leave().await.unwrap();
            provider_session.leave().await.unwrap();
            router.close();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn incoming_handshake_timeout_closes_silent_channel() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let (a, b, local, remote) = pair().await;
            let router = Router::new();
            router.add_realm("realm", RealmConfig::default()).unwrap();
            let error = accept_channel_with_timeout(router.clone(), remote, Duration::from_millis(20))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("handshake timeout"));
            assert!(local.read_packet().await.is_err());
            a.close().await.unwrap();
            b.close().await.unwrap();
            router.close();
        })
        .await
        .unwrap();
    }
}
