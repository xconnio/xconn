#![cfg(feature = "async")]
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use xconn_core::{
    JSONSerializerSpec, Payload, Result as RpcResult, StreamOptions, Value,
    async_::{
        router::{RealmConfig, Router},
        session::Session,
    },
    wampproto::authenticators::anonymous::AnonymousAuthenticator,
};

async fn sessions() -> (Router, Session, Session) {
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default()).unwrap();
    let callee = router
        .connect(
            "realm",
            Box::new(JSONSerializerSpec),
            Box::new(AnonymousAuthenticator::default()),
        )
        .await
        .unwrap();
    let caller = router
        .connect(
            "realm",
            Box::new(JSONSerializerSpec),
            Box::new(AnonymousAuthenticator::default()),
        )
        .await
        .unwrap();
    (router, callee, caller)
}

#[tokio::test]
async fn duplex_inputs_and_final_result_are_independent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (router, callee, caller) = sessions().await;
        let total = Arc::new(AtomicI64::new(0));
        callee
            .register_context("sum", HashMap::new(), move |context| {
                let total = total.clone();
                async move {
                    let n = context.invocation.arg_int(0).unwrap();
                    let sum = total.fetch_add(n, Ordering::SeqCst) + n;
                    context
                        .send_progress(vec![Value::Int(sum)], HashMap::new())
                        .await
                        .unwrap();
                    if context.invocation.details.get("progress") == Some(&Value::Bool(true)) {
                        RpcResult::no_result()
                    } else {
                        RpcResult::arg(sum)
                    }
                }
            })
            .await
            .unwrap();
        let call = caller
            .call("sum")
            .arg(1)
            .stream_with(StreamOptions {
                progressive_input: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(call.receive().await.unwrap().unwrap().is_progress());
        let receiver = call.clone();
        let pending = tokio::spawn(async move { receiver.receive().await });
        call.send(vec![Value::Int(2)], HashMap::new()).await.unwrap();
        assert_eq!(pending.await.unwrap().unwrap().unwrap().arg_int(0).unwrap(), 3);
        call.finish(vec![Value::Int(3)], HashMap::new()).await.unwrap();
        assert_eq!(call.receive().await.unwrap().unwrap().arg_int(0).unwrap(), 6);
        let final_result = call.receive().await.unwrap().unwrap();
        assert!(!final_result.is_progress());
        assert_eq!(final_result.arg_int(0).unwrap(), 6);
        assert!(call.receive().await.unwrap().is_none());
        assert!(call.finish(vec![], HashMap::new()).await.is_err());
        callee.leave().await.unwrap();
        caller.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stream_errors_overflow_and_deadlines_do_not_break_the_session() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (router, callee, caller) = sessions().await;
        callee
            .register("echo", |inv| async move { RpcResult::new(inv.args, inv.kwargs) })
            .execute()
            .await
            .unwrap();
        callee
            .register("error", |_| async { RpcResult::error("example.failure") })
            .execute()
            .await
            .unwrap();
        let call = caller.call("error").stream().await.unwrap();
        let error = call.receive().await.unwrap_err().application.unwrap();
        assert_eq!(error.uri, "example.failure");
        assert!(call.receive().await.unwrap().is_none());
        let call = caller.call("echo").arg(7).stream().await.unwrap();
        assert!(call.send(vec![], HashMap::new()).await.is_err());
        assert_eq!(call.receive().await.unwrap().unwrap().arg_int(0).unwrap(), 7);
        assert!(call.receive().await.unwrap().is_none());
        let (sent, mut seen) = tokio::sync::mpsc::unbounded_channel();
        callee
            .register_context("flood", HashMap::new(), move |context| {
                let sent = sent.clone();
                async move {
                    for n in 0..8 {
                        let _ = context.send_progress(vec![Value::Int(n)], HashMap::new()).await;
                    }
                    let _ = sent.send(());
                    RpcResult::arg("done")
                }
            })
            .await
            .unwrap();
        let call = caller
            .call("flood")
            .stream_with(StreamOptions {
                buffer_size: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        seen.recv().await.unwrap();
        while !call.is_finished() {
            tokio::task::yield_now().await;
        }
        assert!(call.receive().await.unwrap().unwrap().is_progress());
        assert!(call.receive().await.unwrap_err().message.contains("overflow"));
        assert!(call.receive().await.unwrap().is_none());
        callee
            .register_context("wait", HashMap::new(), |context| async move {
                context.cancelled().await;
                RpcResult::no_result()
            })
            .await
            .unwrap();
        let call = caller
            .call("wait")
            .stream_with(StreamOptions {
                timeout: Some(Duration::from_millis(20)),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(call.receive().await.unwrap_err().message.contains("timed out"));
        assert_eq!(
            caller.call("echo").arg(9).execute().await.unwrap().arg_int(0).unwrap(),
            9
        );
        assert!(
            caller
                .call("echo")
                .stream_with(StreamOptions {
                    buffer_size: 0,
                    ..Default::default()
                })
                .await
                .is_err()
        );
        callee.leave().await.unwrap();
        caller.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_and_dropping_consumers_cancel_remote_work() {
    use futures_util::StreamExt;
    tokio::time::timeout(Duration::from_secs(5), async {
        let (router, callee, caller) = sessions().await;
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let (cancelled, mut cancellations) = tokio::sync::mpsc::unbounded_channel();
        callee
            .register_context("wait", HashMap::new(), move |context| {
                let started = started.clone();
                let cancelled = cancelled.clone();
                async move {
                    let _ = started.send(());
                    context
                        .send_progress(vec![Value::Int(1)], HashMap::new())
                        .await
                        .unwrap();
                    context.cancelled().await;
                    let _ = cancelled.send(());
                    RpcResult::no_result()
                }
            })
            .await
            .unwrap();
        let call = caller.call("wait").stream().await.unwrap();
        starts.recv().await.unwrap();
        call.receive().await.unwrap().unwrap();
        let mut waiting = Box::pin(call.receive());
        assert!(futures_util::poll!(&mut waiting).is_pending());
        assert!(call.receive().await.unwrap_err().message.contains("pending"));
        call.cancel();
        assert!(waiting.await.is_err());
        cancellations.recv().await.unwrap();
        assert!(call.receive().await.unwrap().is_none());
        let call = caller.call("wait").stream().await.unwrap();
        let mut results = Box::pin(call.results());
        assert!(results.next().await.unwrap().unwrap().is_progress());
        drop(results);
        cancellations.recv().await.unwrap();
        assert!(call.is_finished());
        callee.leave().await.unwrap();
        caller.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn underlying_reply_queue_reports_overflow_explicitly() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (router, callee, caller) = sessions().await;
        let (sent, mut seen) = tokio::sync::mpsc::unbounded_channel();
        callee
            .register_context("burst", HashMap::new(), move |context| {
                let sent = sent.clone();
                async move {
                    for n in 0..80 {
                        let _ = context.send_progress(vec![Value::Int(n)], HashMap::new()).await;
                    }
                    let _ = sent.send(());
                    RpcResult::arg("done")
                }
            })
            .await
            .unwrap();
        let mut call = caller.call_stream(xconn_core::CallRequest::new("burst")).await.unwrap();
        seen.recv().await.unwrap();
        // Synchronize with the session reader after the burst, without draining the call.
        assert!(caller.call("missing").execute().await.unwrap().error.is_some());
        loop {
            match call.next().await {
                Ok(Some(result)) => assert!(result.is_progress()),
                Err(error) => {
                    assert!(error.message.contains("overflow"));
                    break;
                }
                Ok(None) => panic!("overflow incorrectly completed successfully"),
            }
        }
        callee.leave().await.unwrap();
        caller.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn progressive_inputs_and_results_are_independent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for progressive_input in [false, true] {
            for progressive_results in [false, true] {
                let (router, callee, caller) = sessions().await;
                callee
                    .register_context("modes", HashMap::new(), move |context| async move {
                        let requested = context.invocation.details.get("receive_progress") == Some(&Value::Bool(true));
                        assert_eq!(requested, progressive_results);
                        let sent = context
                            .send_progress(vec![Value::from("partial")], HashMap::new())
                            .await;
                        assert_eq!(sent.is_ok(), progressive_results);

                        if context.invocation.details.get("progress") == Some(&Value::Bool(true)) {
                            RpcResult::no_result()
                        } else {
                            RpcResult::arg("final")
                        }
                    })
                    .await
                    .unwrap();
                let call = caller
                    .call("modes")
                    .option("receive_progress", !progressive_results)
                    .stream_with(StreamOptions {
                        progressive_input,
                        progressive_results,
                        ..Default::default()
                    })
                    .await
                    .unwrap();

                if progressive_input {
                    if progressive_results {
                        assert!(call.receive().await.unwrap().unwrap().is_progress());
                    }
                    call.finish(Vec::new(), HashMap::new()).await.unwrap();
                }
                if progressive_results {
                    assert!(call.receive().await.unwrap().unwrap().is_progress());
                }

                let result = call.receive().await.unwrap().unwrap();
                assert!(!result.is_progress());
                assert_eq!(result.arg_str(0).unwrap(), "final");
                assert!(call.receive().await.unwrap().is_none());

                caller.close().await.unwrap();
                callee.close().await.unwrap();
                router.close();
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_call_ids_follow_wire_order() {
    use xconn_core::{_SerializerSpec, Error, SessionDetails, async_::peer::Peer};
    #[derive(Debug)]
    struct DelayedPeer {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        written: tokio::sync::mpsc::UnboundedSender<i64>,
    }
    #[async_trait::async_trait]
    impl Peer for DelayedPeer {
        fn kind(&self) -> usize {
            xconn_core::TRANSPORT_WEB_SOCKET
        }

        async fn read(&self) -> Result<Vec<u8>, Error> {
            std::future::pending().await
        }

        async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
            let codec = JSONSerializerSpec.serializer();
            let wire = codec.deserialize(data).unwrap().marshal();
            if wire[0] != Value::Int(48) {
                return Ok(());
            }
            let Value::Int(id) = wire[1] else {
                panic!("missing call ID")
            };
            if id == 1 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.written.send(id).unwrap();
            Ok(())
        }
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (written, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let session = Session::new(
            SessionDetails::new(1, "realm".into(), "caller".into(), "user".into()),
            Box::new(DelayedPeer {
                entered: entered.clone(),
                release: release.clone(),
                written,
            }),
            JSONSerializerSpec.serializer(),
        );
        let first = session.call_stream(xconn_core::CallRequest::new("first"));
        tokio::pin!(first);
        assert!(futures_util::poll!(&mut first).is_pending());
        entered.notified().await;
        let second = session.call_stream(xconn_core::CallRequest::new("second"));
        tokio::pin!(second);
        assert!(futures_util::poll!(&mut second).is_pending());
        assert!(observed.try_recv().is_err());
        release.notify_one();
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_eq!(observed.recv().await, Some(1));
        assert_eq!(observed.recv().await, Some(2));
        drop(first);
        drop(second);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unrequested_progress_is_a_protocol_error_not_a_delivered_result() {
    use xconn_core::{_SerializerSpec as _, SessionDetails, async_::peer::MemoryPeer};
    tokio::time::timeout(Duration::from_secs(5), async {
        let (peer, remote) = MemoryPeer::pair(8);
        let session = Session::new(
            SessionDetails::new(1, "realm".into(), String::new(), String::new()),
            peer,
            JSONSerializerSpec.serializer(),
        );
        let call = session
            .call("echo")
            .stream_with(StreamOptions {
                progressive_results: false,
                ..Default::default()
            })
            .await
            .unwrap();
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
        let error = call.receive().await.unwrap_err();
        assert!(error.to_string().contains("unsolicited progressive result"));
        session.wait_disconnect().await;
        assert!(!session.connected());
    })
    .await
    .unwrap();
}
