//! Expose a local service through an upstream router and restore it on reconnect.
use std::sync::Arc;
use xconn_core::{
    Error, JSONSerializerSpec, Result as RpcResult,
    async_::{
        client::connect_anonymous,
        router::{RealmConfig, Router},
        server::Server,
        services::{
            attachment::{Attachment, AttachmentOutcome, RetryPolicy, supervise},
            bridge::{Bridge, BridgeRoute},
        },
        session::Session,
    },
};

const REALM: &str = "example";
const PROCEDURE: &str = "example.echo";
const ADDRESS: &str = "127.0.0.1:8080";
const URI: &str = "ws://127.0.0.1:8080";

async fn local_service() -> Result<(Router, Session), Error> {
    let router = Router::new();
    router.add_realm(REALM, RealmConfig::default())?;
    let session = router
        .connect_internal(REALM, "backend", "trusted", Box::new(JSONSerializerSpec))
        .await?;
    let registration = session
        .register(PROCEDURE, |invocation| async move {
            RpcResult::new(invocation.args, invocation.kwargs)
        })
        .execute()
        .await?;
    if let Some(error) = registration.error {
        return Err(Error::application(error));
    }
    Ok((router, session))
}

async fn attach(uri: &str, target: &Session, policy: RetryPolicy) -> Result<(), Error> {
    supervise(
        policy,
        || attachment_cycle(uri, target.clone()),
        |error| eprintln!("attachment: {error}"),
    )
    .await
}

async fn attachment_cycle(uri: &str, target: Session) -> AttachmentOutcome {
    let upstream = match connect_anonymous(uri, REALM).await {
        Ok(session) => session,
        Err(error) => return AttachmentOutcome::Retry(error),
    };
    let bridge = match Bridge::new(upstream.clone(), target, vec![BridgeRoute::exact(PROCEDURE)], None).await {
        Ok(bridge) => bridge,
        Err(error) => {
            let _ = upstream.close().await;
            return AttachmentOutcome::Retry(error);
        }
    };
    println!("exposing {PROCEDURE} through {uri}");
    let attachment = match Attachment::new(vec![upstream], vec![Arc::new(bridge)]) {
        Ok(attachment) => attachment,
        Err(error) => return AttachmentOutcome::Retry(error),
    };
    match attachment.wait().await {
        Ok(()) => AttachmentOutcome::Disconnected(Error::new("upstream session disconnected")),
        Err(error) => AttachmentOutcome::Disconnected(error),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::args().nth(1).as_deref() {
        Some("router") => {
            let router = Router::new();
            router.add_realm(REALM, RealmConfig::default())?;
            let server = Server::websocket(router.clone(), ADDRESS.parse()?).await?;
            println!("listening on {URI}, realm {REALM}");
            tokio::signal::ctrl_c().await?;
            server.close();
            router.close();
        }
        Some("backend") => {
            let (router, session) = local_service().await?;
            tokio::select! {
                result = attach(URI, &session, RetryPolicy::default()) => result?,
                result = tokio::signal::ctrl_c() => result?,
            }
            session.close().await?;
            router.close();
        }
        Some("call") => {
            let session = connect_anonymous(URI, REALM).await?;
            let result = session.call(PROCEDURE).arg("hello").execute().await?;
            session.close().await?;
            if let Some(error) = result.error {
                return Err(Error::application(error).into());
            }
            println!("{:?}", result.args);
        }
        _ => return Err("usage: service_attachment router|backend|call".into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use xconn_core::Value;

    async fn wait_for_echo(caller: &Session) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let result = caller.call(PROCEDURE).arg("hello").execute().await.unwrap();
                match result.error {
                    None => {
                        assert_eq!(result.args, Some(vec![Value::Str("hello".into())]));
                        return;
                    }
                    Some(error) => assert_eq!(error.uri, "wamp.error.no_such_procedure"),
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("attachment did not expose the local procedure");
    }

    #[tokio::test]
    async fn reconnect_restores_exposure_and_cancellation_preserves_the_backend() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let upstream = Router::new();
            upstream.add_realm(REALM, RealmConfig::default()).unwrap();
            let server = Server::websocket(upstream.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let uri = format!("ws://{}", server.local_addr);
            let (backend, target) = local_service().await.unwrap();
            let attached_target = target.clone();
            let attached_uri = uri.clone();
            let attachment = tokio::spawn(async move {
                attach(
                    &attached_uri,
                    &attached_target,
                    RetryPolicy {
                        initial: Duration::from_millis(10),
                        maximum: Duration::from_millis(20),
                    },
                )
                .await
            });
            let mut caller = connect_anonymous(&uri, REALM).await.unwrap();
            wait_for_echo(&caller).await;

            // End every upstream session, preserving only the listening socket.
            // The backend must reconnect and register again against a fresh realm.
            for _ in 0..2 {
                upstream.close();
                caller.wait_disconnect().await;
                upstream.add_realm(REALM, RealmConfig::default()).unwrap();
                caller = connect_anonymous(&uri, REALM).await.unwrap();
                wait_for_echo(&caller).await;
                assert!(target.connected());
            }

            attachment.abort();
            assert!(attachment.await.unwrap_err().is_cancelled());
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let result = caller.call(PROCEDURE).execute().await.unwrap();
                    if let Some(error) = result.error {
                        if error.uri == "wamp.error.no_such_procedure" {
                            break;
                        }
                        // A call racing bridge shutdown can be cancelled before
                        // the router acknowledges removal of the registration.
                        assert_eq!(error.uri, "wamp.error.canceled");
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("cancelled attachment retained its upstream registration");

            let local = target.call(PROCEDURE).arg("still running").execute().await.unwrap();
            assert!(local.error.is_none());
            assert_eq!(local.args, Some(vec![Value::Str("still running".into())]));
            caller.close().await.unwrap();
            target.close().await.unwrap();
            server.close();
            upstream.close();
            backend.close();
        })
        .await
        .unwrap();
    }
}
