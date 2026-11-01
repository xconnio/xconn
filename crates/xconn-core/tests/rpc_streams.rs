#![cfg(all(feature = "rpc-streams", feature = "quic"))]

use std::{collections::HashMap, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xconn_core::{
    JSONSerializerSpec, Result as RpcResult,
    async_::{
        quic::{Connection, Listener},
        router::{RealmConfig, Router},
    },
    wampproto::authenticators::anonymous::AnonymousAuthenticator,
};

#[tokio::test]
async fn rust_invocation_opens_the_stream_at_its_argument_index() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let router = Router::new();
        router.add_realm("example", RealmConfig::default()).unwrap();
        let listener = Listener::bind(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let backend = Connection::connect(listener.local_addr, "localhost", listener.client_config().unwrap())
            .await
            .unwrap();
        let service = backend
            .open_session(
                "example",
                Box::new(JSONSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .unwrap();
        let registered = service
            .register_context("app.upload", HashMap::new(), |inv| async move {
                assert_eq!(inv.invocation.args[0], "report.pdf".into());
                let mut stream = inv.arg_stream(1).await.unwrap();
                let mut body = Vec::new();
                stream.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, vec![42; 256 * 1024]);
                RpcResult::arg(true)
            })
            .await
            .unwrap();
        assert!(registered.error.is_none());
        let client = Connection::connect(listener.local_addr, "localhost", listener.client_config().unwrap())
            .await
            .unwrap();
        let caller = client
            .open_session(
                "example",
                Box::new(JSONSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .unwrap();
        let (reference, mut outgoing) = caller.offer_stream().await.unwrap();
        let writer = tokio::spawn(async move {
            outgoing.write_all(&vec![42; 256 * 1024]).await.unwrap();
            outgoing.shutdown().await.unwrap();
            outgoing
        });
        let result = caller
            .call("app.upload")
            .arg("report.pdf")
            .arg(reference)
            .execute()
            .await
            .unwrap();
        assert!(result.error.is_none());
        assert_eq!(result.args.unwrap(), vec![true.into()]);
        drop(writer.await.unwrap());
        caller.close().await.unwrap();
        service.close().await.unwrap();
        client.close();
        backend.close();
        listener.close();
        router.close();
    })
    .await
    .unwrap();
}
