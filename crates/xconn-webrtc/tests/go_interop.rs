#![cfg(feature = "wamp")]
use std::{collections::HashMap, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};
use xconn_core::Result as RpcResult;
use xconn_core::{async_::client::Client, *};
use xconn_webrtc::wamp::{SignalingConfig, connect};

fn auth() -> Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync> {
    Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
}

#[tokio::test]
#[ignore = "requires XCONN_GO_INTEROP_BIN from the Go interoperability test harness"]
async fn rust_client_to_go_webrtc_provider() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let binary = std::env::var("XCONN_GO_INTEROP_BIN").unwrap();
        let mut go = tokio::process::Command::new(binary)
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut output = BufReader::new(go.stdout.take().unwrap()).lines();
        let line = output.next_line().await.unwrap().unwrap();
        let info: HashMap<String, String> = wampproto::serializers::codec::Codec::Json
            .decode(line.as_bytes())
            .unwrap();
        let signaling = Client::new(Box::new(JSONSerializerSpec), auth())
            .connect(&info["ws"], "realm")
            .await
            .unwrap();
        let session = connect(
            &signaling,
            SignalingConfig {
                offer_procedure: "offer".into(),
                remote_candidates_topic: "to-client".into(),
                local_candidates_topic: "to-provider".into(),
                ice_servers: vec![],
            },
            "realm",
            Box::new(CBORSerializerSpec),
            auth(),
        )
        .await
        .unwrap();
        let response = session
            .execute_call(CallRequest::new("echo").arg("hello over WebRTC"))
            .await
            .unwrap();
        assert!(response.error.is_none());
        assert_eq!(response.args.unwrap(), vec![Value::Str("hello over WebRTC".into())]);
        let another = session
            .open_session("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        session.close().await.unwrap();
        assert!(
            another
                .execute_call(CallRequest::new("echo").arg(42))
                .await
                .unwrap()
                .error
                .is_none()
        );
        another.close().await.unwrap();
        session.connection.close().await.unwrap();
        signaling.leave().await.unwrap();
        go.kill().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires XCONN_GO_INTEROP_BIN from the Go interoperability test harness"]
async fn go_client_to_rust_webrtc_provider() {
    tokio::time::timeout(Duration::from_secs(45), async {
        use xconn_core::async_::{
            router::{RealmConfig, Router},
            server::Server,
            types::RegisterRequest,
        };
        use xconn_webrtc::wamp::Provider;
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let local = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        local
            .execute_register(RegisterRequest::new("rust.echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        let provider = Provider::start(
            local.clone(),
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
        let server = Server::websocket(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let result = tokio::process::Command::new(std::env::var("XCONN_GO_INTEROP_BIN").unwrap())
            .arg("rtc-client")
            .arg(format!("ws://{}", server.local_addr))
            .kill_on_drop(true)
            .status()
            .await
            .unwrap();
        assert!(result.success());
        provider.close().await;
        local.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}
