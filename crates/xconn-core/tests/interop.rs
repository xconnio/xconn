#![cfg(feature = "async")]
use std::{collections::HashMap, time::Duration};
use tokio::io::{AsyncBufReadExt, BufReader};
use xconn_core::Result as RpcResult;
use xconn_core::{
    async_::{
        client::Client,
        router::{RealmConfig, Router},
        server::Server,
        types::RegisterRequest,
    },
    *,
};

fn auth() -> Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync> {
    Box::new(wampproto::authenticators::anonymous::AnonymousAuthenticator::default())
}

#[tokio::test]
#[ignore = "requires XCONN_GO_INTEROP_BIN from the Go interoperability test harness"]
async fn go_rust_bidirectional_rpc() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let binary = std::env::var("XCONN_GO_INTEROP_BIN").unwrap();
        let mut go = tokio::process::Command::new(&binary)
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut output = BufReader::new(go.stdout.take().unwrap()).lines();
        let line = output.next_line().await.unwrap().unwrap();
        let info: HashMap<String, String> = wampproto::serializers::codec::Codec::Json
            .decode(line.as_bytes())
            .unwrap();
        for uri in [&info["ws"], &info["rs"]] {
            for spec in [
                Box::new(JSONSerializerSpec) as Box<dyn SerializerSpec>,
                Box::new(CBORSerializerSpec),
                Box::new(MsgPackSerializerSpec),
            ] {
                let client = Client::new(spec, auth()).connect(uri, "realm").await.unwrap();
                let response = client
                    .execute_call(CallRequest::new("echo").arg("hello from Rust"))
                    .await
                    .unwrap();
                assert!(response.error.is_none());
                assert_eq!(response.args.unwrap(), vec![Value::Str("hello from Rust".into())]);
                client.leave().await.unwrap();
            }
        }
        go.kill().await.unwrap();
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let local = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        local
            .execute_register(RegisterRequest::new("rust.echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        let ws = Server::websocket(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let rs = Server::rawsocket(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        for uri in [format!("ws://{}", ws.local_addr), format!("rs://{}", rs.local_addr)] {
            let status = tokio::process::Command::new(&binary)
                .arg("client")
                .arg(uri)
                .kill_on_drop(true)
                .status()
                .await
                .unwrap();
            assert!(status.success());
        }
        local.leave().await.unwrap();
        router.close();
    })
    .await
    .unwrap();
}

#[cfg(all(feature = "quic", feature = "webtransport"))]
#[tokio::test]
#[ignore = "requires XCONN_GO_INTEROP_BIN"]
async fn go_rust_quic_and_webtransport() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    tokio::time::timeout(Duration::from_secs(45), async {
        let binary = std::env::var("XCONN_GO_INTEROP_BIN").unwrap();
        let mut go = tokio::process::Command::new(&binary)
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut output = BufReader::new(go.stdout.take().unwrap()).lines();
        let line = output.next_line().await.unwrap().unwrap();
        let info: HashMap<String, String> = wampproto::serializers::codec::Codec::Json
            .decode(line.as_bytes())
            .unwrap();
        let decode = |s: &str| -> Vec<u8> {
            s.as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                .collect()
        };
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(decode(&info["quic_cert"])))
            .unwrap();
        let mut tls =
            rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        tls.alpn_protocols = vec![xconn_core::async_::quic::ALPN.to_vec()];
        let config = quinn::ClientConfig::new(std::sync::Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
        ));
        let connection =
            xconn_core::async_::quic::Connection::connect(info["quic"].parse().unwrap(), "localhost", config)
                .await
                .unwrap();
        for spec in [
            Box::new(JSONSerializerSpec) as Box<dyn SerializerSpec>,
            Box::new(CBORSerializerSpec),
            Box::new(MsgPackSerializerSpec),
        ] {
            let session = connection.open_session("realm", spec, auth()).await.unwrap();
            assert!(
                session
                    .execute_call(CallRequest::new("echo").arg(42))
                    .await
                    .unwrap()
                    .error
                    .is_none()
            );
            session.leave().await.unwrap();
        }
        connection.close();
        let hash: [u8; 32] = decode(&info["wt_hash"]).try_into().unwrap();
        let config = xconn_core::async_::webtransport::backend::ClientBuilder::new()
            .with_handshake_timeout(Duration::from_secs(10))
            .with_server_certificate_hashes(vec![hash.to_vec()])
            .unwrap();
        let connection = xconn_core::async_::webtransport::Connection::connect(&info["wt"], config)
            .await
            .unwrap();
        for spec in [
            Box::new(JSONSerializerSpec) as Box<dyn SerializerSpec>,
            Box::new(CBORSerializerSpec),
            Box::new(MsgPackSerializerSpec),
        ] {
            let session = connection.open_session("realm", spec, auth()).await.unwrap();
            assert!(
                session
                    .execute_call(CallRequest::new("echo").arg(42))
                    .await
                    .unwrap()
                    .error
                    .is_none()
            );
            session.leave().await.unwrap();
        }
        connection.close();
        go.kill().await.unwrap();
        let router = Router::new();
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let local = router
            .connect("realm", Box::new(JSONSerializerSpec), auth())
            .await
            .unwrap();
        local
            .execute_register(RegisterRequest::new("rust.echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap();
        let quic = xconn_core::async_::quic::Listener::bind(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let wt = xconn_core::async_::webtransport::Listener::bind(router.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let encode = |s: &[u8]| -> String { s.iter().map(|byte| format!("{byte:02x}")).collect() };
        for (mode, uri, cert) in [
            ("quic-client", quic.local_addr.to_string(), quic.certificate.clone()),
            (
                "wt-client",
                format!("https://127.0.0.1:{}", wt.local_addr.port()),
                wt.certificate.clone(),
            ),
        ] {
            let status = tokio::process::Command::new(&binary)
                .arg(mode)
                .arg(uri)
                .arg(encode(&cert))
                .kill_on_drop(true)
                .status()
                .await
                .unwrap();
            assert!(status.success());
        }
        local.leave().await.unwrap();
        quic.close();
        wt.close();
        router.close();
    })
    .await
    .unwrap();
}
