#![cfg(feature = "async")]
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xconn_core::Result as RpcResult;
use xconn_core::{
    async_::{
        router::{RealmConfig, Router},
        types::RegisterRequest,
    },
    *,
};

fn auth() -> Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync> {
    Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
}

#[cfg(feature = "quic")]
#[tokio::test]
async fn quic_sessions_and_authenticated_raw_stream() {
    tokio::time::timeout(Duration::from_secs(15), async {
        use xconn_core::async_::quic::*;
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let server = Listener::bind(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let connection = Connection::connect(server.local_addr, "localhost", server.client_config().unwrap())
            .await
            .unwrap();
        let first = connection
            .open_session("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let second = connection
            .open_session("realm", Box::new(CBORSerializerSpec), auth())
            .await
            .unwrap();
        second
            .execute_register(RegisterRequest::new("echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        assert_eq!(
            first
                .execute_call(CallRequest::new("echo").arg(7))
                .await
                .unwrap()
                .args
                .unwrap(),
            vec![Value::Int(7)]
        );
        let mut raw = connection.open_raw().await.unwrap();
        raw.write_all(b"raw").await.unwrap();
        let mut received = server.accept_raw().await.unwrap();
        assert_eq!(received.session.id, first.details().id());
        let mut data = [0; 3];
        received.stream.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"raw");
        first.leave().await.unwrap();
        assert!(second.connected());
        second.leave().await.unwrap();
        connection.close();
        server.close();
        router.close();
    })
    .await
    .unwrap();
}

#[cfg(feature = "webtransport")]
#[tokio::test]
async fn webtransport_sessions_and_authenticated_raw_stream() {
    tokio::time::timeout(Duration::from_secs(15), async {
        use xconn_core::async_::webtransport::*;
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let server = Listener::bind(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let connection = Connection::connect(
            &format!("https://{}", server.local_addr),
            server.client_config().unwrap(),
        )
        .await
        .unwrap();
        let first = connection
            .open_session("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let second = connection
            .open_session("realm", Box::new(CBORSerializerSpec), auth())
            .await
            .unwrap();
        second
            .execute_register(RegisterRequest::new("echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        assert_eq!(
            first
                .execute_call(CallRequest::new("echo").arg(9))
                .await
                .unwrap()
                .args
                .unwrap(),
            vec![Value::Int(9)]
        );
        let mut raw = connection.open_raw().await.unwrap();
        raw.write_all(b"raw").await.unwrap();
        let mut received = server.accept_raw().await.unwrap();
        assert_eq!(received.session.id, first.details().id());
        let mut data = [0; 3];
        received.stream.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"raw");
        first.leave().await.unwrap();
        assert!(second.connected());
        second.leave().await.unwrap();
        connection.close();
        server.close();
        router.close();
    })
    .await
    .unwrap();
}
