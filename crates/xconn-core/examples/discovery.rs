//! A generic discovery host, independent of any DeskConn cloud service.
use std::sync::Arc;
use xconn_core::{
    JSONSerializerSpec,
    async_::{
        router::{RealmConfig, Router},
        server::Server,
        services::discovery::DiscoveryService,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = Router::new();
    router.add_realm(
        "example",
        RealmConfig {
            disclose_caller: true,
            meta_api: true,
            ..Default::default()
        },
    )?;
    let session = router
        .connect_internal("example", "directory", "trusted", Box::new(JSONSerializerSpec))
        .await?;
    // This public example permits discovery/advertising. Production applications
    // provide their own policy; ownership checks are still enforced by the service.
    let service = DiscoveryService::start(session.clone(), "example.peers", Arc::new(|_, _, _| true)).await?;
    let server = Server::websocket(router.clone(), "127.0.0.1:0".parse()?).await?;
    println!("ws://{}", server.local_addr);
    tokio::signal::ctrl_c().await?;
    service.close().await?;
    session.close().await?;
    server.close();
    router.close();
    Ok(())
}
