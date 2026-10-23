//! Local transport integration peer shared by the language-binding tests.
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use xconn_core::{
    CBORSerializerSpec, Result as RpcResult,
    async_::{
        quic,
        router::{RealmConfig, Router},
        server::Server,
        webtransport,
    },
};
use xconn_webrtc::wamp::{Provider, SignalingConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = Router::new();
    router.add_realm("realm", RealmConfig::default())?;
    let local = router
        .connect_internal("realm", "backend", "trusted", Box::new(CBORSerializerSpec))
        .await?;
    local
        .register("echo", |inv| async move { RpcResult::new(inv.args, inv.kwargs) })
        .execute()
        .await?;
    let server = Server::websocket(router.clone(), "127.0.0.1:0".parse()?).await?;
    let quic = Arc::new(quic::Listener::bind(router.clone(), "127.0.0.1:0".parse()?).await?);
    let mut params = rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])?;
    // Browsers accept serverCertificateHashes only for certificates valid at most 14 days.
    params.not_before = time::OffsetDateTime::now_utc() - time::Duration::days(1);
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(7);
    let key = rcgen::KeyPair::generate()?;
    let cert = params.self_signed(&key)?;
    let mut transport = webtransport::backend::quion::TransportConfig::default();
    transport
        .set_retry_enabled(false)
        .set_reset_stream_at(true)
        .set_max_datagram_frame_size(Some(webtransport::backend::quion::VarInt::from_u32(65535)));
    let backend = webtransport::backend::ServerBuilder::new()
        .with_transport_config(transport)
        .with_addr("127.0.0.1:0".parse()?)
        .with_certificate(
            vec![cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )?;
    let wt = Arc::new(
        webtransport::Listener::with_server_path(router.clone(), backend, cert.der().to_vec(), Some("/wamp".into()))
            .await?,
    );
    let provider = Arc::new(
        Provider::start(
            local.clone(),
            router.clone(),
            SignalingConfig {
                offer_procedure: "offer".into(),
                remote_candidates_topic: "to-provider".into(),
                local_candidates_topic: "to-client".into(),
                ice_servers: vec![],
            },
        )
        .await?,
    );
    println!(
        "{}",
        serde_json::json!({
            "ws": format!("ws://{}", server.local_addr),
            "quic": quic.local_addr.to_string(),
            "quic_certificate": hex::encode(&quic.certificate),
            "webtransport": format!("https://127.0.0.1:{}/wamp", wt.local_addr.port()),
            "webtransport_certificate": hex::encode(cert.der()),
        })
    );
    let q = quic.clone();
    tokio::spawn(async move {
        while let Some(raw) = q.accept_raw().await {
            tokio::spawn(echo_stream(raw.stream));
        }
    });
    let w = wt.clone();
    tokio::spawn(async move {
        while let Some(raw) = w.accept_raw().await {
            tokio::spawn(echo_stream(raw.stream));
        }
    });
    let p = provider.clone();
    tokio::spawn(async move {
        while let Some(channel) = p.accept_channel().await {
            tokio::spawn(async move {
                while let Ok(message) = channel.read_message().await {
                    if channel.write_message(&message).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let _ = tokio::io::stdin().read_u8().await;
    provider.close().await;
    quic.close();
    wt.close();
    server.close();
    router.close();
    Ok(())
}

async fn echo_stream(stream: impl AsyncRead + AsyncWrite) {
    let (mut read, mut write) = tokio::io::split(stream);
    let _ = tokio::io::copy(&mut read, &mut write).await;
    let _ = write.shutdown().await;
}
