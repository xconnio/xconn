use std::time::Duration;
use xconn_core::Result as RpcResult;
use xconn_webrtc::{Connection, MTU};

async fn pair() -> (
    Connection,
    Connection,
    std::sync::Arc<xconn_webrtc::DataChannel>,
    std::sync::Arc<xconn_webrtc::DataChannel>,
) {
    let a = Connection::new(vec![]).await.unwrap();
    let b = Connection::new(vec![]).await.unwrap();
    let local = a.open_channel("data").await.unwrap();
    let offer = a.offer().await.unwrap();
    let answer = b.answer(offer).await.unwrap();
    a.set_remote(answer).await.unwrap();
    let remote = b.accept_channel().await.unwrap();
    local.wait_open(Duration::from_secs(10)).await.unwrap();
    remote.wait_open(Duration::from_secs(10)).await.unwrap();
    (a, b, local, remote)
}

#[tokio::test]
async fn real_data_channels_and_multiple_streams() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (a, b, local, remote) = pair().await;
        let payload = vec![42; MTU * 5];
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            local.write_message(&payload).await.unwrap();
            local
        });
        assert_eq!(remote.read_message().await.unwrap(), expected);
        let local = writer.await.unwrap();
        remote.write_message(&[]).await.unwrap();
        assert!(local.read_message().await.unwrap().is_empty());
        let extra = a.open_channel("file").await.unwrap();
        let other = b.accept_channel().await.unwrap();
        extra.wait_open(Duration::from_secs(5)).await.unwrap();
        extra.send_packet(bytes::Bytes::from_static(b"raw data")).await.unwrap();
        assert_eq!(other.read_packet().await.unwrap().as_ref(), b"raw data");
        local.close().await.unwrap();
        extra
            .send_packet(bytes::Bytes::from_static(b"still alive"))
            .await
            .unwrap();
        assert_eq!(other.read_packet().await.unwrap().as_ref(), b"still alive");
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[cfg(feature = "wamp")]
#[tokio::test]
async fn multiple_wamp_sessions_share_connection() {
    tokio::time::timeout(Duration::from_secs(30), async {
        use xconn_core::{
            async_::{
                router::{RealmConfig, Router},
                types::RegisterRequest,
            },
            *,
        };
        use xconn_webrtc::wamp::{WebRTCSession, accept_channel};
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let (a, b, local, remote) = pair().await;
        let first_router = router.clone();
        let first = tokio::spawn(async move { accept_channel(first_router, remote).await });
        let auth = || {
            Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
                as Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync>
        };
        let session = WebRTCSession::join(a.clone(), local, "realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let second_router = router.clone();
        let remote_connection = b.clone();
        let second = tokio::spawn(async move {
            let channel = remote_connection.accept_channel().await.unwrap();
            accept_channel(second_router, channel).await
        });
        let another = session
            .open_session("realm", Box::new(CBORSerializerSpec), auth())
            .await
            .unwrap();
        another
            .execute_register(RegisterRequest::new("echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        let result = session
            .execute_call(CallRequest::new("echo").arg("hello"))
            .await
            .unwrap();
        assert_eq!(result.args.unwrap(), vec![Value::Str("hello".into())]);
        session.close().await.unwrap();
        first.await.unwrap().unwrap();
        assert!(another.connected());
        another.close().await.unwrap();
        second.await.unwrap().unwrap();
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[cfg(feature = "wamp")]
#[tokio::test]
async fn wamp_signaling_provider_and_client() {
    tokio::time::timeout(Duration::from_secs(30), async {
        use xconn_core::{
            async_::{
                router::{RealmConfig, Router},
                types::RegisterRequest,
            },
            *,
        };
        use xconn_webrtc::wamp::{Provider, SignalingConfig, connect};
        let signaling_router = Router::new();
        signaling_router.add_realm("signal", RealmConfig::default()).unwrap();
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let auth = || {
            Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
                as Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync>
        };
        let provider_session = signaling_router
            .connect("signal", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let client_session = signaling_router
            .connect("signal", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let provider = Provider::start(
            provider_session.clone(),
            router.clone(),
            SignalingConfig {
                offer_procedure: "offer".into(),
                remote_candidates_topic: "to-provider".into(),
                local_candidates_topic: "to-client".into(),
                ice_servers: vec![],
            },
        )
        .await
        .unwrap();
        let local = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        local
            .execute_register(RegisterRequest::new("echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        let client = connect(
            &client_session,
            SignalingConfig {
                offer_procedure: "offer".into(),
                remote_candidates_topic: "to-client".into(),
                local_candidates_topic: "to-provider".into(),
                ice_servers: vec![],
            },
            "realm",
            Box::new(JSONSerializerSpec),
            auth(),
        )
        .await
        .unwrap();
        assert_eq!(
            client
                .execute_call(CallRequest::new("echo").arg(42))
                .await
                .unwrap()
                .args
                .unwrap(),
            vec![Value::Int(42)]
        );
        let explicit_raw = client.open_channel("server-first", Default::default()).await.unwrap();
        let accepted = tokio::time::timeout(Duration::from_secs(2), provider.accept_raw())
            .await
            .unwrap()
            .unwrap();
        assert!(accepted.first_packet.is_none());
        accepted
            .channel
            .send_packet(bytes::Bytes::from_static(b"server first"))
            .await
            .unwrap();
        assert_eq!(explicit_raw.read_packet().await.unwrap().as_ref(), b"server first");
        let reverse = accepted
            .connection
            .open_raw_channel("reverse", Default::default())
            .await
            .unwrap();
        let incoming_reverse = client.accept_channel().await.unwrap();
        reverse.wait_open(Duration::from_secs(2)).await.unwrap();
        incoming_reverse.wait_open(Duration::from_secs(2)).await.unwrap();
        reverse.write_message(b"provider opened").await.unwrap();
        assert_eq!(incoming_reverse.read_message().await.unwrap(), b"provider opened");
        reverse.close().await.unwrap();
        explicit_raw.close().await.unwrap();
        assert!(accepted.channel.read_packet().await.is_err());
        let raw = client.connection.open_channel("file").await.unwrap();
        raw.wait_open(Duration::from_secs(5)).await.unwrap();
        raw.send_packet(bytes::Bytes::from_static(b"first")).await.unwrap();
        let incoming = provider.accept_raw().await.unwrap();
        assert_eq!(incoming.first_packet.as_ref().unwrap().as_ref(), b"first");
        incoming
            .channel
            .send_packet(bytes::Bytes::from_static(b"response"))
            .await
            .unwrap();
        assert_eq!(raw.read_packet().await.unwrap().as_ref(), b"response");
        client.close().await.unwrap();
        raw.send_packet(bytes::Bytes::from_static(b"still open")).await.unwrap();
        assert_eq!(incoming.channel.read_packet().await.unwrap().as_ref(), b"still open");
        client.connection.close().await.unwrap();
        let waiting = provider.accept_raw();
        let (accepted, ()) = tokio::join!(waiting, provider.close());
        assert!(accepted.is_none());
        local.leave().await.unwrap();
        provider_session.leave().await.unwrap();
        client_session.leave().await.unwrap();
        router.close();
        signaling_router.close();
    })
    .await
    .unwrap();
}
