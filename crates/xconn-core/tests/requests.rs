#![cfg(feature = "async")]

use wampproto::authenticators::{anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator};
use xconn_core::{JSONSerializerSpec, Result as RpcResult, Value};

fn auth() -> Box<dyn ClientAuthenticator + Send + Sync> {
    Box::new(AnonymousAuthenticator::default())
}

#[tokio::test]
async fn async_requests_execute_rpc_and_pubsub() {
    use xconn_core::async_::router::{RealmConfig, Router};
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let a = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let b = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let registration = a.register("echo", |inv| async move { RpcResult::new(inv.args, inv.kwargs) });
        // Building the registration has not installed it on the router.
        let missing = b.call("echo").execute().await.unwrap();
        assert!(missing.error.is_some());
        let registration = registration.option("invoke", "single").execute().await.unwrap();
        assert!(registration.error.is_none());
        let reply = b
            .call("echo")
            .arg("hello")
            .kwarg("enabled", true)
            .execute()
            .await
            .unwrap();
        assert_eq!(reply.args.unwrap(), vec![Value::from("hello")]);
        assert_eq!(reply.kwargs.unwrap()["enabled"], Value::Bool(true));
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let subscription = a
            .subscribe("events", move |event| {
                let sender = sender.clone();
                async move {
                    sender.send(event).unwrap();
                }
            })
            .option("match", "exact")
            .execute()
            .await
            .unwrap();
        assert!(subscription.error.is_none());
        let publication = b.publish("events").arg(7).option("acknowledge", true);
        assert!(receiver.try_recv().is_err());
        assert!(publication.execute().await.unwrap().unwrap().error.is_none());
        assert_eq!(receiver.recv().await.unwrap().args, vec![Value::Int(7)]);
        a.unsubscribe(subscription.subscription_id).await.unwrap();
        a.unregister(registration.registration_id).await.unwrap();
        a.leave().await.unwrap();
        b.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[cfg(feature = "sync")]
#[test]
fn blocking_requests_execute_rpc_and_pubsub() {
    use xconn_core::sync::router::{RealmConfig, Router};
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default()).unwrap();
    let a = router.connect("realm", Box::new(JSONSerializerSpec), auth()).unwrap();
    let b = router.connect("realm", Box::new(JSONSerializerSpec), auth()).unwrap();
    let registration = a.register("echo", |inv| RpcResult::new(inv.args, inv.kwargs));
    assert!(b.call("echo").execute().unwrap().error.is_some());
    let registration = registration.execute().unwrap();
    assert!(registration.error.is_none());
    let reply = b.call("echo").arg(42).execute().unwrap();
    assert_eq!(reply.args.unwrap(), vec![Value::Int(42)]);
    let subscription = a.subscribe("events", |_| {}).execute().unwrap();
    assert!(subscription.error.is_none());
    assert!(
        b.publish("events")
            .arg(7)
            .option("acknowledge", true)
            .execute()
            .unwrap()
            .unwrap()
            .error
            .is_none()
    );
    a.unsubscribe(subscription.subscription_id).unwrap();
    a.unregister(registration.registration_id).unwrap();
    a.leave().unwrap();
    b.leave().unwrap();
    router.close();
}
