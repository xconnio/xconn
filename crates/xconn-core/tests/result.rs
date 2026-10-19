#![cfg(feature = "async")]
use std::time::Duration;
use xconn_core::async_::router::{RealmConfig, Router};
use xconn_core::wampproto::authenticators::anonymous::AnonymousAuthenticator;
use xconn_core::{JSONSerializerSpec, Payload, Result as RpcResult};

#[tokio::test]
async fn received_result_can_be_returned_from_a_handler() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let auth = || Box::new(AnonymousAuthenticator::default());
        let callee = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        let caller = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        callee
            .register("inner", |inv| async move { RpcResult::new(inv.args, inv.kwargs) })
            .execute()
            .await
            .unwrap();
        let upstream = callee.clone();
        callee
            .register("outer", move |inv| {
                let upstream = upstream.clone();
                async move {
                    upstream
                        .call("inner")
                        .args(inv.args)
                        .kwargs(inv.kwargs)
                        .execute()
                        .await
                        .unwrap()
                }
            })
            .execute()
            .await
            .unwrap();
        let result: RpcResult = caller
            .call("outer")
            .arg("hello")
            .kwarg("enabled", true)
            .execute()
            .await
            .unwrap();
        assert_eq!(result.arg_str(0).unwrap(), "hello");
        assert!(result.kwarg_bool("enabled").unwrap());
        callee
            .register("failed", |_| async { RpcResult::error("example.failure") })
            .execute()
            .await
            .unwrap();
        let error = caller.call("failed").execute().await.unwrap().error.unwrap();
        assert_eq!(error.uri, "example.failure");
        callee.leave().await.unwrap();
        caller.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}
