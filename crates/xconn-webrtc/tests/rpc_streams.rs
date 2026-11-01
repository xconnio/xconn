#![cfg(feature = "rpc-streams")]

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xconn_core::{
    CBORSerializerSpec, Result as RpcResult,
    async_::router::{RealmConfig, Router},
    wampproto::authenticators::anonymous::AnonymousAuthenticator,
};
use xconn_webrtc::{
    Connection,
    wamp::{WebRTCSession, serve_connection},
};

#[tokio::test]
async fn concurrent_joins_agree_with_the_router_on_the_rpc_stream_owner() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let router = Router::new();
        router.add_realm("example", RealmConfig::default()).unwrap();
        let backend = router
            .connect_internal("example", "application", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let registered = backend
            .register_context("app.upload", Default::default(), |inv| async move {
                let mut stream = inv.arg_stream(0).await.unwrap();
                let mut body = Vec::new();
                stream.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, vec![42; 256 * 1024]);
                RpcResult::arg(true)
            })
            .await
            .unwrap();
        assert!(registered.error.is_none());
        let client = Connection::new(vec![]).await.unwrap();
        let server = Connection::new(vec![]).await.unwrap();
        let first_channel = client.open_channel("first").await.unwrap();
        let offer = client.offer().await.unwrap();
        let answer = server.answer(offer).await.unwrap();
        client.set_remote(answer).await.unwrap();
        let host = tokio::spawn(serve_connection(router.clone(), server.clone()));
        let second_channel = client.open_channel("second").await.unwrap();
        let (first, second) = tokio::join!(
            WebRTCSession::join(
                client.clone(),
                first_channel,
                "example",
                Box::new(CBORSerializerSpec),
                Box::new(AnonymousAuthenticator::default())
            ),
            WebRTCSession::join(
                client.clone(),
                second_channel,
                "example",
                Box::new(CBORSerializerSpec),
                Box::new(AnonymousAuthenticator::default())
            ),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        let (first_offer, second_offer) = tokio::join!(first.offer_stream(), second.offer_stream());
        let (owner, reference, mut stream) = match (first_offer, second_offer) {
            (Ok((reference, stream)), Err(_)) => (&first, reference, stream),
            (Err(_), Ok((reference, stream))) => (&second, reference, stream),
            _ => panic!("exactly one authenticated session must own RPC streams"),
        };
        let writer = tokio::spawn(async move {
            stream.write_all(&vec![42; 256 * 1024]).await.unwrap();
            stream.shutdown().await.unwrap();
            stream
        });
        let result = owner.call("app.upload").arg(reference).execute().await.unwrap();
        assert!(result.error.is_none());
        assert_eq!(result.args.unwrap(), vec![true.into()]);
        drop(writer.await.unwrap());
        first.close().await.unwrap();
        second.close().await.unwrap();
        client.close().await.unwrap();
        host.abort();
        let _ = host.await;
        server.close().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}
