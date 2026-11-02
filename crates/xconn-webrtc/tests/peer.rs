#![cfg(feature = "services")]

use std::time::Duration;
use xconn_core::{
    CBORSerializerSpec, Result as RpcResult,
    async_::{
        router::{RealmConfig, Router},
        services::peer::RetryPolicy,
        session::Session,
    },
    wampproto::authenticators::anonymous::AnonymousAuthenticator,
};
use xconn_webrtc::peer::{self, Config, Endpoint};

async fn join(router: &Router) -> Session {
    router
        .connect_internal("edge", "client", "trusted", Box::new(CBORSerializerSpec))
        .await
        .unwrap()
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rust_discovery_advertisement_and_recovery_need_no_binding_callbacks() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let hub = Router::new();
        hub.add_realm("edge", RealmConfig::default()).unwrap();
        let edge = Router::new();
        edge.add_realm("edge", RealmConfig::default()).unwrap();
        let upstream = join(&hub).await;
        let backend = join(&edge).await;
        let caller = join(&hub).await;
        assert!(
            upstream
                .register("app.path", |_| async { RpcResult::arg("routed") })
                .execute()
                .await
                .unwrap()
                .error
                .is_none()
        );
        assert!(
            backend
                .register("app.path", |_| async { RpcResult::arg("direct") })
                .execute()
                .await
                .unwrap()
                .error
                .is_none()
        );
        let config = Config {
            ice_servers: vec![],
            retry: RetryPolicy {
                initial: Duration::from_millis(10),
                connect_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        };
        let managed = peer::connect(
            caller.clone(),
            config.clone(),
            || Ok(Box::new(CBORSerializerSpec)),
            || Ok(Box::new(AnonymousAuthenticator::default())),
        )
        .unwrap();
        assert_eq!(
            managed
                .session_for("app.path")
                .unwrap()
                .call("app.path")
                .execute()
                .await
                .unwrap()
                .args,
            Some(vec!["routed".into()])
        );
        let endpoint = Endpoint::expose(upstream.clone(), edge.clone(), config.clone())
            .await
            .unwrap();
        let own = peer::connect_once(
            &upstream,
            &config,
            Box::new(CBORSerializerSpec),
            Box::new(AnonymousAuthenticator::default()),
        )
        .await;
        assert_eq!(own.err().unwrap().to_string(), "realm endpoint belongs to this session");
        until(|| managed.direct().is_some()).await;
        assert_eq!(
            managed
                .session_for("app.path")
                .unwrap()
                .call("app.path")
                .execute()
                .await
                .unwrap()
                .args,
            Some(vec!["direct".into()])
        );
        assert_eq!(
            managed.session_for("wamp.session.get").unwrap().details().id(),
            caller.details().id()
        );
        assert_eq!(
            managed.session_for(peer::RESOLVE).unwrap().details().id(),
            caller.details().id()
        );
        {
            let closing = endpoint.close();
            tokio::pin!(closing);
            tokio::select! {
                _ = &mut closing => {},
                _ = tokio::task::yield_now() => {},
            }
        }
        until(|| managed.direct().is_none()).await;
        assert_eq!(
            caller.call(peer::RESOLVE).execute().await.unwrap().error.unwrap().uri,
            "wamp.error.no_such_procedure"
        );
        assert_eq!(
            managed
                .session_for("app.path")
                .unwrap()
                .call("app.path")
                .execute()
                .await
                .unwrap()
                .args,
            Some(vec!["routed".into()])
        );
        let replacement = Endpoint::expose(upstream.clone(), edge.clone(), config).await.unwrap();
        until(|| managed.direct().is_some()).await;
        assert!(managed.last_error().is_none());
        managed.close().await;
        assert!(caller.connected());
        replacement.close().await;
        caller.close().await.unwrap();
        upstream.close().await.unwrap();
        backend.close().await.unwrap();
        hub.close();
        edge.close();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn discovery_preserves_application_error_payloads() {
    let router = Router::new();
    router.add_realm("edge", RealmConfig::default()).unwrap();
    let endpoint = join(&router).await;
    let caller = join(&router).await;
    endpoint
        .register(peer::RESOLVE, |_| async {
            RpcResult {
                error: Some(xconn_core::WampError {
                    uri: "app.discovery_denied".into(),
                    args: Some(vec!["reason".into()]),
                    kwargs: Some(std::collections::HashMap::from([("retry".into(), false.into())])),
                }),
                ..Default::default()
            }
        })
        .execute()
        .await
        .unwrap();

    let result = peer::connect_once(
        &caller,
        &Config::default(),
        Box::new(CBORSerializerSpec),
        Box::new(AnonymousAuthenticator::default()),
    )
    .await;
    let Err(error) = result else {
        panic!("discovery should fail")
    };
    let application = error.application.expect("structured WAMP error");
    assert_eq!(application.uri, "app.discovery_denied");
    assert_eq!(application.args, Some(vec!["reason".into()]));
    assert_eq!(application.kwargs.unwrap().get("retry"), Some(&false.into()));

    caller.close().await.unwrap();
    endpoint.close().await.unwrap();
}
