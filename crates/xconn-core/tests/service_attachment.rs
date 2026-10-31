#![cfg(feature = "services")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;
use xconn_core::{
    Error, JSONSerializerSpec, Result as RpcResult,
    async_::{
        router::{RealmConfig, Router},
        services::{
            attachment::Attachment,
            bridge::{Bridge, BridgeRoute},
        },
    },
};

#[tokio::test]
async fn losing_either_upstream_session_closes_the_attachment_but_not_the_backend() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for failed_realm in ["device", "control"] {
            let router = Router::new();
            for realm in ["device", "control", "backend"] {
                router.add_realm(realm, RealmConfig::default()).unwrap();
            }
            let connect = |realm| router.connect_internal(realm, "service", "trusted", Box::new(JSONSerializerSpec));
            let device = connect("device").await.unwrap();
            let control = connect("control").await.unwrap();
            let backend = connect("backend").await.unwrap();
            backend
                .register("example.echo", |invocation| async move {
                    RpcResult::new(invocation.args, invocation.kwargs)
                })
                .execute()
                .await
                .unwrap();
            let bridge = Bridge::new(
                device.clone(),
                backend.clone(),
                vec![BridgeRoute::exact("example.echo")],
                None,
            )
            .await
            .unwrap();
            let caller = connect("device").await.unwrap();
            assert!(caller.call("example.echo").execute().await.unwrap().error.is_none());
            let closed = Arc::new(AtomicUsize::new(0));
            let observed = closed.clone();
            let attachment = Attachment::with_shutdown(
                vec![device.clone(), control.clone()],
                vec![bridge],
                move || async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .unwrap();
            if failed_realm == "device" {
                device.close().await.unwrap();
            } else {
                control.close().await.unwrap();
            }
            attachment.wait().await.unwrap();
            attachment.close().await.unwrap();
            device.wait_disconnect().await;
            control.wait_disconnect().await;
            assert_eq!(closed.load(Ordering::SeqCst), 1);
            assert!(backend.call("example.echo").execute().await.unwrap().error.is_none());
            assert_eq!(
                caller.call("example.echo").execute().await.unwrap().error.unwrap().uri,
                "wamp.error.no_such_procedure"
            );
            caller.close().await.unwrap();
            backend.close().await.unwrap();
            router.close();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelling_close_does_not_cancel_cleanup_or_lose_its_error() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        router.add_realm("example", RealmConfig::default()).unwrap();
        let session = router
            .connect_internal("example", "test", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let (started, entered) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let attachment = Arc::new(
            Attachment::with_shutdown(vec![session.clone()], Vec::new(), move || async move {
                started.send(()).unwrap();
                finished.await.unwrap();
                Err(Error::new("transport cleanup failed"))
            })
            .unwrap(),
        );
        let closing = attachment.clone();
        let close = tokio::spawn(async move { closing.close().await });
        entered.await.unwrap();
        close.abort();
        assert!(close.await.unwrap_err().is_cancelled());
        finish.send(()).unwrap();
        for _ in 0..2 {
            assert_eq!(
                attachment.close().await.unwrap_err().message,
                "transport cleanup failed"
            );
        }
        session.wait_disconnect().await;
        router.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_the_attachment_requests_cleanup() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        router.add_realm("example", RealmConfig::default()).unwrap();
        let session = router
            .connect_internal("example", "test", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        let (closed, observed) = oneshot::channel();
        let attachment = Attachment::with_shutdown(vec![session.clone()], Vec::new(), move || async move {
            closed.send(()).unwrap();
            Ok(())
        })
        .unwrap();
        drop(attachment);
        observed.await.unwrap();
        session.wait_disconnect().await;
        router.close();
    })
    .await
    .unwrap();
}

#[cfg(feature = "quic")]
#[tokio::test]
async fn attachment_owns_two_sessions_on_one_quic_connection() {
    use xconn_core::async_::quic::{Connection, Listener};
    use xconn_core::wampproto::authenticators::anonymous::AnonymousAuthenticator;

    tokio::time::timeout(Duration::from_secs(10), async {
        let router = Router::new();
        for realm in ["device", "control"] {
            router.add_realm(realm, RealmConfig::default()).unwrap();
        }
        let listener = Listener::bind(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let connection = Connection::connect(listener.local_addr, "localhost", listener.client_config().unwrap())
            .await
            .unwrap();
        let mut sessions = Vec::new();
        for realm in ["device", "control"] {
            sessions.push(
                connection
                    .open_session(
                        realm,
                        Box::new(JSONSerializerSpec),
                        Box::new(AnonymousAuthenticator::default()),
                    )
                    .await
                    .unwrap(),
            );
        }
        let control = sessions[1].clone();
        let closing = connection.clone();
        let attachment = Attachment::with_shutdown(sessions, Vec::new(), move || async move {
            closing.close();
            Ok(())
        })
        .unwrap();
        control.close().await.unwrap();
        attachment.wait().await.unwrap();
        connection.connection.closed().await;
        assert!(connection.open_raw().await.is_err());
        listener.close();
        router.close();
    })
    .await
    .unwrap();
}
