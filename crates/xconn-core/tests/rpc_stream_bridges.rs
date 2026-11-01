#![cfg(all(feature = "services", feature = "rpc-streams"))]

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xconn_core::{
    CBORSerializerSpec, Result as RpcResult,
    async_::{
        router::{RealmConfig, Router},
        services::bridge::{Bridge, BridgeRoute},
    },
};

#[tokio::test]
async fn bridge_translates_input_and_result_streams_without_network_transports() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let remote = Router::new();
        remote.add_realm("remote", RealmConfig::default()).unwrap();
        let local = Router::new();
        local.add_realm("local", RealmConfig::default()).unwrap();
        let source = remote
            .connect_internal("remote", "edge", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let caller = remote
            .connect_internal("remote", "caller", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let target = local
            .connect_internal("local", "edge", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let backend = local
            .connect_internal("local", "application", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let output_session = backend.clone();
        let (finished, mut observed) = tokio::sync::mpsc::channel(1);
        let registered = backend
            .register_context("app.transfer", Default::default(), move |inv| {
                let output_session = output_session.clone();
                let finished = finished.clone();
                async move {
                    let mut input = inv.arg_stream(1).await.unwrap();
                    let mut body = Vec::new();
                    input.read_to_end(&mut body).await.unwrap();
                    let (reference, mut output) = output_session.offer_stream().await.unwrap();
                    tokio::spawn(async move {
                        output.write_all(&body).await.unwrap();
                        output.shutdown().await.unwrap();
                        let _ = tokio::io::copy(&mut output, &mut tokio::io::sink()).await;
                        drop(output);
                        let _ = finished.send(()).await;
                    });
                    RpcResult::new(vec![inv.invocation.args[0].clone(), reference], Default::default())
                }
            })
            .await
            .unwrap();
        assert!(registered.error.is_none());
        let bridge = Bridge::new(source.clone(), target.clone(), vec![BridgeRoute::prefix("app.")], None)
            .await
            .unwrap();
        let payload = vec![42; 512 * 1024];
        let expected = payload.clone();
        let (reference, mut input) = caller.offer_stream().await.unwrap();
        let writer = tokio::spawn(async move {
            input.write_all(&payload).await.unwrap();
            input.shutdown().await.unwrap();
            input
        });
        let result = caller
            .call("app.transfer")
            .arg("name")
            .arg(reference)
            .execute()
            .await
            .unwrap();
        assert!(result.error.is_none());
        drop(writer.await.unwrap());
        let args = result.args.unwrap();
        assert_eq!(args[0], "name".into());
        let mut output = caller.open_stream_argument(&args[1]).await.unwrap();
        let mut received = Vec::new();
        output.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, expected);
        drop(output);
        observed.recv().await.unwrap();
        bridge.close().await.unwrap();
        assert!(source.connected());
        assert!(target.connected());
        remote.close();
        local.close();
    })
    .await
    .unwrap();
}
