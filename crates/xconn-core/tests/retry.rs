//! Manual upstream regression; not part of the passing release configuration.
#![cfg(feature = "webtransport")]
use std::time::Duration;
use xconn_core::Result as RpcResult;
use xconn_core::{
    async_::{
        router::{RealmConfig, Router},
        types::RegisterRequest,
        webtransport::{Listener, backend},
    },
    *,
};

#[tokio::test]
#[ignore = "quion 0.2.2 fragmented ClientHello Retry regression; see docs/webtransport-retry.md"]
async fn original_go_with_webtransport_retry() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let binary = std::env::var("XCONN_GO_INTEROP_BIN").unwrap();
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let local = router
            .connect_internal("realm", "service", "trusted", Box::new(JSONSerializerSpec))
            .await
            .unwrap();
        local
            .execute_register(RegisterRequest::new("rust.echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let certificate = generated.cert.der().to_vec();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der());
        let mut transport = backend::quion::TransportConfig::default();
        transport
            .set_reset_stream_at(true)
            .set_max_datagram_frame_size(Some(backend::quion::VarInt::from_u32(65535)))
            .set_retry_enabled(true)
            .set_max_connections(1024)
            .set_max_pending_handshakes(256)
            .set_max_endpoint_memory_bytes(64 * 1024 * 1024);
        let server = backend::ServerBuilder::new()
            .with_addr("127.0.0.1:0".parse().unwrap())
            .with_handshake_timeout(Duration::from_secs(10))
            .with_transport_config(transport)
            .with_certificate(vec![generated.cert.der().clone()], key.into())
            .unwrap();
        let listener = Listener::with_server(router.clone(), server, certificate.clone())
            .await
            .unwrap();
        let address = format!("https://127.0.0.1:{}", listener.local_addr.port());
        let encoded: String = certificate.iter().map(|byte| format!("{byte:02x}")).collect();
        let output = tokio::process::Command::new(binary)
            .args(["wt-client", &address, &encoded])
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        listener.close();
        local.leave().await.unwrap();
        router.close();
    })
    .await
    .expect("Retry regression deadline");
}
