#![cfg(all(feature = "services", feature = "rpc-streams"))]

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xconn::{
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

#[tokio::test]
async fn final_input_waits_for_an_earlier_chunks_stream_setup() {
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::{Notify, mpsc};
    use xconn::{StreamOptions, async_::session::Session};

    async fn join(router: &Router, realm: &str) -> Session {
        router
            .connect_internal(realm, "client", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap()
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        let remote = Router::new();
        remote.add_realm("remote", RealmConfig::default()).unwrap();
        let local = Router::new();
        local.add_realm("local", RealmConfig::default()).unwrap();
        let source = join(&remote, "remote").await;
        let caller = join(&remote, "remote").await;
        let backend = join(&local, "local").await;
        let opening = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let target = join(&local, "local").await.with_stream_opener({
            let opening = opening.clone();
            let release = release.clone();
            Arc::new(move || {
                let opening = opening.clone();
                let release = release.clone();
                Box::pin(async move {
                    opening.notify_one();
                    release.notified().await;
                    // Model a slow transport opening an otherwise writable stream.
                    // Byte transfer itself is covered by the end-to-end test above.
                    let (stream, mut remote) = tokio::io::duplex(1024);
                    tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut remote, &mut tokio::io::sink()).await;
                    });
                    Ok(Box::new(stream) as xconn::async_::rawsocket::BoxStream)
                })
            })
        });
        let (observed, mut chunks) = mpsc::channel(3);
        backend
            .register_context("app.ordered", HashMap::new(), move |invocation| {
                let observed = observed.clone();
                async move {
                    observed.send(invocation.invocation.args[0].clone()).await.unwrap();
                    if invocation.invocation.details.get("progress") == Some(&true.into()) {
                        RpcResult::no_result()
                    } else {
                        RpcResult::arg("finished")
                    }
                }
            })
            .await
            .unwrap();
        let bridge = Bridge::new(
            source.clone(),
            target.clone(),
            vec![BridgeRoute::exact("app.ordered")],
            None,
        )
        .await
        .unwrap();
        let call = caller
            .call("app.ordered")
            .arg("start")
            .stream_with(StreamOptions {
                progressive_input: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(chunks.recv().await, Some("start".into()));

        let (reference, input) = caller.offer_stream().await.unwrap();
        call.send(vec!["middle".into(), reference], HashMap::new())
            .await
            .unwrap();
        opening.notified().await;
        call.finish(vec!["end".into()], HashMap::new()).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), chunks.recv())
                .await
                .is_err(),
            "the final chunk must not overtake stream setup"
        );

        release.notify_one();
        assert_eq!(chunks.recv().await, Some("middle".into()));
        assert_eq!(chunks.recv().await, Some("end".into()));
        let result = call.receive().await.unwrap().unwrap();
        assert_eq!(result.args, Some(vec!["finished".into()]));

        drop(input);
        bridge.close().await.unwrap();
        caller.close().await.unwrap();
        source.close().await.unwrap();
        target.close().await.unwrap();
        backend.close().await.unwrap();
        remote.close();
        local.close();
    })
    .await
    .unwrap();
}
