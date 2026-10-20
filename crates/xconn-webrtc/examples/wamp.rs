//! Run with: cargo run -p xconn-webrtc --example wamp
use std::time::Duration;
use xconn_core::{
    CBORSerializerSpec, JSONSerializerSpec, Result as RpcResult,
    async_::router::{RealmConfig, Router},
    wampproto::authenticators::{anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator},
};
use xconn_webrtc::wamp::{Provider, SignalingConfig, Timeouts, connect_with_timeouts};

fn auth() -> Box<dyn ClientAuthenticator + Send + Sync> {
    Box::new(AnonymousAuthenticator::default())
}

fn signaling_config(provider: bool) -> SignalingConfig {
    SignalingConfig {
        offer_procedure: "io.xconn.webrtc.offer".into(),
        remote_candidates_topic: if provider { "to-provider" } else { "to-client" }.into(),
        local_candidates_topic: if provider { "to-client" } else { "to-provider" }.into(),
        // Configure STUN/TURN here when connecting across networks.
        ice_servers: vec![],
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // This example uses an in-process signaling router. In separate processes,
    // establish these two signaling sessions over WebSocket instead.
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default())?;
    let provider_signal = router.connect("realm", Box::new(JSONSerializerSpec), auth()).await?;
    let client_signal = router.connect("realm", Box::new(JSONSerializerSpec), auth()).await?;
    provider_signal
        .register("echo", |inv| async move { RpcResult::new(inv.args, inv.kwargs) })
        .execute()
        .await?;

    let timeouts = Timeouts {
        connect: Duration::from_secs(15),
        open_session: Duration::from_secs(10),
        handshake: Duration::from_secs(5),
    };
    let provider = Provider::start_with_timeouts(
        provider_signal.clone(),
        router.clone(),
        signaling_config(true),
        timeouts,
    )
    .await?;
    let session = connect_with_timeouts(
        &client_signal,
        signaling_config(false),
        "realm",
        Box::new(CBORSerializerSpec),
        auth(),
        timeouts,
    )
    .await?;

    let result = session.call("echo").arg("WAMP over WebRTC").execute().await?;
    println!("RPC result: {:?}", result.args);

    let outgoing = session.open_channel("file-transfer", Default::default()).await?;
    // Acceptance does not require an initial client packet: the provider sends first.
    let incoming = provider.accept_channel().await.ok_or("provider closed")?;
    incoming.write_message(b"ready").await?;
    assert_eq!(outgoing.read_message().await?, b"ready");

    // Closing the WAMP session leaves the raw channel and connection alive.
    session.close().await?;
    outgoing.write_message(b"file contents").await?;
    assert_eq!(incoming.read_message().await?, b"file contents");
    outgoing.close().await?;
    session.connection.close().await?;
    provider.close().await;
    client_signal.leave().await?;
    provider_signal.leave().await?;
    router.close();
    Ok(())
}
