#![cfg(feature = "services")]
use xconn_core::_SerializerSpec as _;

use std::{collections::HashMap, sync::Arc, time::Duration};
use xconn_core::async_::{
    router::{RealmConfig, Router},
    services::bridge::{Bridge, BridgeRoute},
    types::RegisterRequest,
};
use xconn_core::{CBORSerializerSpec, CallRequest, JSONSerializerSpec, Result as RpcResult, Value};

#[tokio::test]
async fn bridge_preserves_errors_payloads_and_releases_registrations() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        router.add_realm("source", RealmConfig::default()).unwrap();
        router.add_realm("target", RealmConfig::default()).unwrap();
        let connect = |realm| router.connect_internal(realm, "test", "trusted", Box::new(CBORSerializerSpec));
        let source = connect("source").await.unwrap();
        let target = connect("target").await.unwrap();
        let caller = connect("source").await.unwrap();
        target
            .execute_register(RegisterRequest::new("example.echo", |invocation| async move {
                RpcResult::new(invocation.args, invocation.kwargs)
            }))
            .await
            .unwrap();
        target
            .execute_register(RegisterRequest::new("example.failure", |_| async {
                RpcResult::error("example.failure")
            }))
            .await
            .unwrap();
        let bridge = Bridge::new(
            source.clone(),
            target.clone(),
            vec![BridgeRoute::prefix("example.")],
            None,
        )
        .await
        .unwrap();
        let response = caller
            .execute_call(
                CallRequest::new("example.echo")
                    .arg(Value::from(vec![1_u8, 2, 3]))
                    .kwarg("format", "bytes"),
            )
            .await
            .unwrap();
        assert_eq!(response.kwargs.unwrap()["format"], Value::Str("bytes".into()));
        assert_eq!(response.args, Some(vec![Value::from(vec![1_u8, 2, 3])]));
        assert!(response.error.is_none());
        assert_eq!(
            caller
                .execute_call(CallRequest::new("example.failure"))
                .await
                .unwrap()
                .error
                .unwrap()
                .uri,
            "example.failure"
        );
        bridge.close().await.unwrap();
        assert_eq!(
            caller
                .execute_call(CallRequest::new("example.echo"))
                .await
                .unwrap()
                .error
                .unwrap()
                .uri,
            "wamp.error.no_such_procedure"
        );
        assert!(target.connected());
        assert!(source.connected());
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_partial_bridge_start_rolls_back_only_its_own_registrations() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        router.add_realm("lab", RealmConfig::default()).unwrap();
        let source = router
            .connect_internal("lab", "source", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let target = router
            .connect_internal("lab", "backend", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let caller = router
            .connect_internal("lab", "caller", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        target
            .execute_register(RegisterRequest::new("reserved", |_| async {
                RpcResult::arg("original")
            }))
            .await
            .unwrap();
        let result = Bridge::new(
            source.clone(),
            target.clone(),
            vec![BridgeRoute::exact("forwarded"), BridgeRoute::exact("reserved")],
            None,
        )
        .await;
        assert!(result.is_err());
        loop {
            let response = caller.execute_call(CallRequest::new("forwarded")).await.unwrap();
            if response
                .error
                .is_some_and(|error| error.uri == "wamp.error.no_such_procedure")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let response = caller.execute_call(CallRequest::new("reserved")).await.unwrap();
        assert_eq!(response.args, Some(vec![Value::Str("original".into())]));
        assert!(source.connected());
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bridge_forwards_progressive_inputs_and_independent_results() {
    use std::sync::Mutex;
    use xconn_core::StreamOptions;

    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        for realm in ["source", "target"] {
            router.add_realm(realm, RealmConfig::default()).unwrap();
        }
        let source = router
            .connect_internal("source", "bridge", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let target = router
            .connect_internal("target", "backend", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let caller = router
            .connect_internal("source", "caller", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap();
        let values = Arc::new(Mutex::new(Vec::new()));
        target
            .register_context("example.process", HashMap::new(), move |context| {
                let values = values.clone();
                async move {
                    values.lock().unwrap().extend(context.invocation.args.clone());
                    if context.invocation.details.get("receive_progress") == Some(&Value::Bool(true)) {
                        context
                            .send_progress(context.invocation.args.clone(), HashMap::new())
                            .await
                            .unwrap();
                    }
                    if context.invocation.details.get("progress") == Some(&Value::Bool(true)) {
                        RpcResult::no_result()
                    } else {
                        RpcResult::args(std::mem::take(&mut *values.lock().unwrap()))
                    }
                }
            })
            .await
            .unwrap();
        let bridge = Bridge::new(source, target.clone(), vec![BridgeRoute::prefix("example.")], None)
            .await
            .unwrap();
        for progressive_results in [false, true] {
            let call = caller
                .call("example.process")
                .arg("first")
                .stream_with(StreamOptions {
                    progressive_input: true,
                    progressive_results,
                    ..Default::default()
                })
                .await
                .unwrap();
            call.send(vec![Value::Str("second".into())], HashMap::new())
                .await
                .unwrap();
            call.finish(vec![Value::Str("last".into())], HashMap::new())
                .await
                .unwrap();
            let mut updates = 0;
            let mut final_results = 0;
            while let Some(result) = call.receive().await.unwrap() {
                if result.details.get("progress") == Some(&Value::Bool(true)) {
                    updates += 1;
                } else {
                    final_results += 1;
                    assert_eq!(result.args, Some(vec!["first".into(), "second".into(), "last".into()]));
                }
            }
            assert_eq!(updates, if progressive_results { 3 } else { 0 });
            assert_eq!(final_results, 1);
        }
        bridge.close().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelling_a_bridged_stream_cancels_the_backend_handler() {
    struct Cancelled(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Cancelled {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        for realm in ["source", "target"] {
            router.add_realm(realm, RealmConfig::default()).unwrap();
        }
        let source = router
            .connect_internal("source", "bridge", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let target = router
            .connect_internal("target", "backend", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let caller = router
            .connect_internal("source", "caller", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let (sender, cancelled) = tokio::sync::oneshot::channel();
        let sender = Arc::new(std::sync::Mutex::new(Some(sender)));
        target
            .register_context("example.wait", HashMap::new(), move |context| {
                let guard = Cancelled(sender.lock().unwrap().take());
                async move {
                    let _guard = guard;
                    context
                        .send_progress(vec![Value::Str("started".into())], HashMap::new())
                        .await
                        .unwrap();
                    std::future::pending::<RpcResult>().await
                }
            })
            .await
            .unwrap();
        let bridge = Bridge::new(source, target.clone(), vec![BridgeRoute::exact("example.wait")], None)
            .await
            .unwrap();
        let call = caller.call("example.wait").stream().await.unwrap();
        assert!(call.receive().await.unwrap().is_some());
        call.cancel();
        cancelled.await.unwrap();
        assert!(target.connected());
        assert!(caller.connected());
        bridge.close().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn an_early_bridged_result_does_not_reinvoke_for_queued_input_chunks() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use xconn_core::{
        Error, StreamOptions, TransportType,
        async_::{peer::Peer, session::Session},
    };

    #[derive(Debug)]
    struct FinalReplyGate {
        peer: Box<dyn Peer>,
        blocked: Arc<Notify>,
        release: tokio::sync::watch::Receiver<bool>,
    }
    #[async_trait::async_trait]
    impl Peer for FinalReplyGate {
        fn kind(&self) -> TransportType {
            self.peer.kind()
        }

        async fn read(&self) -> Result<Vec<u8>, Error> {
            self.peer.read().await
        }

        async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
            let message = JSONSerializerSpec.serializer().deserialize(data.clone())?;
            if message.message_type() == Value::Int(70) {
                self.blocked.notify_one();
                let mut release = self.release.clone();
                let _ = release.wait_for(|open| *open).await;
            }
            self.peer.write(data).await
        }

        async fn close(&self) -> Result<(), Error> {
            self.peer.close().await
        }
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        for realm in ["source", "target"] {
            router.add_realm(realm, RealmConfig::default()).unwrap();
        }
        let (peer, details) = router
            .connect_internal_peer("source", "bridge", "trusted", Box::new(JSONSerializerSpec), 16)
            .await
            .unwrap();
        let blocked = Arc::new(Notify::new());
        let (release, released) = tokio::sync::watch::channel(false);
        let source = Session::new(
            details,
            Box::new(FinalReplyGate {
                peer,
                blocked: blocked.clone(),
                release: released,
            }),
            JSONSerializerSpec.serializer(),
        );
        let target = router
            .connect_internal("target", "backend", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let caller = router
            .connect_internal("source", "caller", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let invocations = Arc::new(AtomicUsize::new(0));
        let observed = invocations.clone();
        target
            .execute_register(RegisterRequest::new("early", move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                async { RpcResult::arg("done") }
            }))
            .await
            .unwrap();
        let bridge = Bridge::new(source, target.clone(), vec![BridgeRoute::exact("early")], None)
            .await
            .unwrap();
        let call = caller
            .call("early")
            .arg("first")
            .stream_with(StreamOptions {
                progressive_input: true,
                ..Default::default()
            })
            .await
            .unwrap();
        blocked.notified().await;
        // The backend has completed, but the source dealer has not received its
        // final reply. Continuations can still be dispatched to the bridge.
        call.send(vec!["queued".into()], HashMap::new()).await.unwrap();
        call.finish(vec!["last".into()], HashMap::new()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let count = invocations.load(Ordering::SeqCst);
        release.send_replace(true);
        assert_eq!(call.receive().await.unwrap().unwrap().args, Some(vec!["done".into()]));
        assert!(call.receive().await.unwrap().is_none());
        bridge.close().await.unwrap();
        router.close();
        assert_eq!(count, 1, "queued chunks must never create another backend invocation");
    })
    .await
    .unwrap();
}
