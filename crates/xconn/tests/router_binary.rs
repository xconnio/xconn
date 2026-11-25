#![cfg(feature = "router")]
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use wampproto::authenticators::anonymous::AnonymousAuthenticator;
use xconn::Result as RpcResult;
use xconn::{
    async_::{client::Client, types::RegisterRequest},
    *,
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "xconn-binary-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }

    fn config(&self, kind: &str, serializers: &str) -> PathBuf {
        let text = include_str!("../src/bin/xconn/router/config.yaml").replace("realm1", "realm");
        let start = text.find("transports:").unwrap();
        let end = text.find("authenticators:").unwrap();
        let text = format!(
            "{}transports:\n  - type: {kind}\n    listener: tcp\n    address: 127.0.0.1:0\n    serializers: [{serializers}]\n{}",
            &text[..start],
            &text[end..]
        );
        let path = self.0.join("config.yaml");
        std::fs::write(&path, text).unwrap();
        path
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn start(file: &std::path::Path) -> (tokio::process::Child, String) {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_xconn"));
    command.args(["router", "start", "--config"]).arg(file);
    let mut child = command
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let first = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .expect("binary exited before binding");
    assert!(first.starts_with("Listening "), "{first}");
    loop {
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .expect("binary exited before becoming ready");
        if line == "Ready" {
            break;
        }
        assert!(line.starts_with("Listening "), "{line}");
    }
    (child, first.split_whitespace().last().unwrap().into())
}

fn client(spec: Box<dyn SerializerSpec>) -> Client {
    Client::new(spec, Box::new(AnonymousAuthenticator::default()))
}

#[tokio::test]
async fn executable_routes_and_enforces_serializer_policy() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for kind in ["websocket", "rawsocket", "universal"] {
            let files = Files::new();
            let path = files.config(kind, "json");
            let (mut child, address) = start(&path).await;
            for scheme in match kind {
                "websocket" => vec!["ws"],
                "rawsocket" => vec!["rs"],
                _ => vec!["ws", "rs"],
            } {
                let uri = format!("{scheme}://{address}");
                let callee = client(Box::new(JSONSerializerSpec))
                    .connect(&uri, "realm")
                    .await
                    .unwrap();
                assert!(
                    callee
                        .execute_register(RegisterRequest::new("echo", |inv| async move {
                            RpcResult::new(inv.args, inv.kwargs)
                        }))
                        .await
                        .unwrap()
                        .error
                        .is_none()
                );
                let caller = client(Box::new(JSONSerializerSpec))
                    .connect(&uri, "realm")
                    .await
                    .unwrap();
                assert_eq!(
                    caller
                        .execute_call(CallRequest::new("echo").arg("binary"))
                        .await
                        .unwrap()
                        .args,
                    Some(vec![Value::Str("binary".into())])
                );
                assert!(
                    client(Box::new(CBORSerializerSpec))
                        .connect(&uri, "realm")
                        .await
                        .is_err()
                );
                assert!(
                    client(Box::new(JSONSerializerSpec))
                        .connect(&uri, "missing")
                        .await
                        .is_err()
                );
                caller.leave().await.unwrap();
                callee.leave().await.unwrap();
            }
            child.kill().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[test]
fn init_never_overwrites_and_validate_rejects_unknown_settings() {
    let files = Files::new();
    let bin = env!("CARGO_BIN_EXE_xconn");
    let run = |command: &str| {
        std::process::Command::new(bin)
            .args(["router", command])
            .current_dir(&files.0)
            .output()
            .unwrap()
    };
    assert!(run("init").status.success());
    assert!(!run("init").status.success());
    assert!(run("validate").status.success());
    let path = files.0.join("xconn.yaml");
    let mut text = std::fs::read_to_string(&path).unwrap();
    text.push_str("typo: true\n");
    std::fs::write(path, text).unwrap();
    assert!(!run("validate").status.success());
}

#[tokio::test]
async fn xconn_router_initializes_validates_and_starts() {
    let files = Files::new();
    let bin = env!("CARGO_BIN_EXE_xconn");
    let run = |args: &[&str]| {
        std::process::Command::new(bin)
            .arg("router")
            .args(args)
            .current_dir(&files.0)
            .output()
            .unwrap()
    };
    assert!(run(&["init"]).status.success());
    assert!(!run(&["init"]).status.success());
    assert!(run(&["validate"]).status.success());

    let path = files.0.join("xconn.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("127.0.0.1:8080", "127.0.0.1:0")
        .replace("127.0.0.1:8082", "127.0.0.1:0");
    std::fs::write(&path, text).unwrap();
    let (mut child, address) = start(&path).await;
    let _session = client(Box::new(JSONSerializerSpec))
        .connect(&format!("ws://{address}"), "realm1")
        .await
        .unwrap();
    child.kill().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_sends_goodbye_and_releases_listener() {
    let files = Files::new();
    let path = files.config("websocket", "json");
    let (mut child, address) = start(&path).await;
    let session = client(Box::new(JSONSerializerSpec))
        .connect(&format!("ws://{address}"), "realm")
        .await
        .unwrap();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(5), session.wait_disconnect())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(tokio::net::TcpListener::bind(&address).await.is_ok());
}

#[tokio::test]
#[ignore = "requires XCONN_REFERENCE_GO_BIN built from original Go sources"]
async fn original_go_client_against_executable() {
    let binary = std::env::var("XCONN_REFERENCE_GO_BIN").unwrap();
    let provenance = tokio::process::Command::new(&binary)
        .arg("provenance")
        .output()
        .await
        .unwrap();
    assert!(
        String::from_utf8(provenance.stdout)
            .unwrap()
            .contains("independent-pure-go")
    );
    for kind in ["websocket", "rawsocket"] {
        let files = Files::new();
        let path = files.config(kind, "json, cbor, msgpack");
        let mut text = std::fs::read_to_string(&path).unwrap();
        // Public key for the fixture's deterministic test-only seed.
        let auth = wampproto::authenticators::cryptosign::CryptoSignAuthenticator::try_new(
            "interop",
            &"11".repeat(32),
            std::collections::HashMap::new(),
        )
        .unwrap();
        use wampproto::authenticators::authenticator::ClientAuthenticator;
        let extra = auth.auth_extra();
        let key = match &extra["pubkey"] {
            Value::Str(v) => v,
            _ => panic!(),
        };
        text.push_str(&format!("  ticket:\n    - {{authid: interop, realm: realm, role: anonymous, ticket: secret}}\n  wampcra:\n    - {{authid: interop, realm: realm, role: anonymous, secret: secret}}\n    - {{authid: salted, realm: realm, role: anonymous, secret: secret, derive_secret: true, salt: interop-salt, iterations: 1000, keylen: 32}}\n  cryptosign:\n    - authid: interop\n      realm: realm\n      role: anonymous\n      authorized_keys: ['{key}']\n"));
        std::fs::write(&path, text).unwrap();
        let (mut child, address) = start(&path).await;
        let scheme = if kind == "websocket" { "ws" } else { "rs" };
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new(&binary)
                .args(["router-client", &format!("{scheme}://{address}")])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "router-ok");
        child.kill().await.unwrap();
    }
}

#[tokio::test]
async fn role_permissions_are_enforced_by_executable() {
    let files = Files::new();
    let path = files.config("websocket", "json");
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("allow_register: true", "allow_register: false");
    std::fs::write(&path, text).unwrap();
    let (mut child, address) = start(&path).await;
    let session = client(Box::new(JSONSerializerSpec))
        .connect(&format!("ws://{address}"), "realm")
        .await
        .unwrap();
    let response = session
        .execute_register(RegisterRequest::new("forbidden", |_| async { RpcResult::args(vec![]) }))
        .await
        .unwrap();
    assert_eq!(response.error.unwrap().uri, "wamp.error.authorization_failed");
    assert!(
        session
            .execute_call(CallRequest::new("absent"))
            .await
            .unwrap()
            .error
            .is_some()
    );
    session.leave().await.unwrap();
    child.kill().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_listener_routes_and_removes_its_socket() {
    let files = Files::new();
    let path = files.config("rawsocket", "json");
    let socket = files.0.join("router.sock");
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("listener: tcp", "listener: unix")
        .replace("127.0.0.1:0", socket.to_str().unwrap());
    std::fs::write(&path, text).unwrap();
    let (mut child, _) = start(&path).await;
    let session = client(Box::new(JSONSerializerSpec))
        .connect(&format!("unix://{}", socket.display()), "realm")
        .await
        .unwrap();
    assert!(
        session
            .execute_register(RegisterRequest::new("echo", |inv| async move {
                RpcResult::new(inv.args, inv.kwargs)
            }))
            .await
            .unwrap()
            .error
            .is_none()
    );
    assert_eq!(
        session
            .execute_call(CallRequest::new("echo").arg("unix"))
            .await
            .unwrap()
            .args,
        Some(vec![Value::Str("unix".into())])
    );
    session.leave().await.unwrap();
    std::process::Command::new("kill")
        .args(["-TERM", &child.id().unwrap().to_string()])
        .status()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(!socket.exists());
}

#[tokio::test]
async fn tls_listeners_require_trust_and_route_with_valid_certificate() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for kind in ["websocket", "rawsocket"] {
            let files = Files::new();
            let path = files.config(kind, "json");
            let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            std::fs::write(files.0.join("cert.pem"), generated.cert.pem()).unwrap();
            std::fs::write(files.0.join("key.pem"), generated.key_pair.serialize_pem()).unwrap();
            let text = std::fs::read_to_string(&path).unwrap().replace(
                "    serializers: [json]",
                "    serializers: [json]\n    tls: {certificate: cert.pem, key: key.pem}",
            );
            std::fs::write(&path, text).unwrap();
            let (mut child, address) = start(&path).await;
            for trusted in [false, true] {
                let mut roots = rustls::RootCertStore::empty();
                if trusted {
                    roots.add(generated.cert.der().clone()).unwrap();
                }
                let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
                let result = tokio_rustls::TlsConnector::from(std::sync::Arc::new(tls))
                    .connect(
                        rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                        tokio::net::TcpStream::connect(&address).await.unwrap(),
                    )
                    .await;
                if !trusted {
                    assert!(result.is_err());
                    continue;
                }
                let stream = result.unwrap();
                let peer = if kind == "rawsocket" {
                    xconn::async_::rawsocket::client_handshake(
                        Box::new(stream),
                        wampproto::transports::rawsocket::SerializerID::JSON,
                    )
                    .await
                    .unwrap()
                } else {
                    use futures_util::StreamExt;
                    let request = tungstenite::ClientRequestBuilder::new(format!("wss://{address}").parse().unwrap())
                        .with_sub_protocol("wamp.2.json");
                    let (ws, _) = tokio_tungstenite::client_async(request, stream).await.unwrap();
                    let (writer, reader) = ws.split();
                    xconn::async_::websocket::WebSocketPeer::new(reader, writer, false)
                };
                let (peer, details) = xconn::async_::joiner::join(
                    peer,
                    "realm",
                    JSONSerializerSpec.serializer(),
                    Box::new(AnonymousAuthenticator::default()),
                )
                .await
                .unwrap();
                let session = xconn::async_::session::Session::new(details, peer, JSONSerializerSpec.serializer());
                assert!(
                    session
                        .execute_register(RegisterRequest::new("tls.echo", |inv| async move {
                            RpcResult::new(inv.args, inv.kwargs)
                        }))
                        .await
                        .unwrap()
                        .error
                        .is_none()
                );
                assert_eq!(
                    session
                        .execute_call(CallRequest::new("tls.echo").arg("trusted"))
                        .await
                        .unwrap()
                        .args,
                    Some(vec![Value::Str("trusted".into())])
                );
                session.leave().await.unwrap();
            }
            if kind == "websocket" {
                // Run with isolated native-root settings instead of mutating the
                // environment while other tests may be establishing connections.
                assert!(
                    client(Box::new(JSONSerializerSpec))
                        .connect(&format!("wss://{address}"), "realm")
                        .await
                        .is_err()
                );
                let output = tokio::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "websocket_client_uses_native_certificate_roots",
                        "--nocapture",
                    ])
                    .env("XCONN_TEST_WSS_ADDRESS", &address)
                    .env("SSL_CERT_FILE", files.0.join("cert.pem"))
                    .output()
                    .await
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            child.kill().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn websocket_client_uses_native_certificate_roots() {
    let Ok(address) = std::env::var("XCONN_TEST_WSS_ADDRESS") else {
        return;
    };

    // The trusted certificate is for localhost, not its numeric address.
    assert!(
        client(Box::new(JSONSerializerSpec))
            .connect(&format!("wss://{address}"), "realm")
            .await
            .is_err()
    );
    let port = address.rsplit(':').next().unwrap();
    let session = client(Box::new(JSONSerializerSpec))
        .connect(&format!("wss://localhost:{port}"), "realm")
        .await
        .unwrap();
    session
        .execute_register(RegisterRequest::new("tls.client.echo", |inv| async move {
            RpcResult::new(inv.args, inv.kwargs)
        }))
        .await
        .unwrap();
    let result = session
        .execute_call(CallRequest::new("tls.client.echo").arg("trusted"))
        .await
        .unwrap();
    assert_eq!(result.args, Some(vec![Value::Str("trusted".into())]));
    session.leave().await.unwrap();
}

async fn assert_rpc_stream(caller: &xconn::async_::session::Session, callee: &xconn::async_::session::Session) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let provider = callee.clone();
    let registered = callee
        .register_context("stream", Default::default(), move |_| {
            let provider = provider.clone();
            async move {
                let (reference, mut stream) = provider.offer_stream().await.unwrap();
                tokio::spawn(async move {
                    stream.write_all(b"router stream").await.unwrap();
                    stream.shutdown().await.unwrap();
                    tokio::io::copy(&mut stream, &mut tokio::io::sink()).await.unwrap();
                });
                RpcResult::args(vec![reference])
            }
        })
        .await
        .unwrap();
    assert!(registered.error.is_none());
    let result = caller.execute_call(CallRequest::new("stream")).await.unwrap();
    let mut stream = caller.open_stream_argument(&result.args.unwrap()[0]).await.unwrap();
    let mut received = Vec::new();
    stream.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, b"router stream");
}

async fn assert_unassociated_stream_rejected(mut stream: xconn::async_::rawsocket::BoxStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    stream.write_all(b"unassociated").await.unwrap();
    let mut byte = [0];
    assert!(!matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
            .await
            .expect("unassociated stream was not closed"),
        Ok(1)
    ));
}

#[cfg(unix)]
async fn assert_udp_shutdown(mut child: tokio::process::Child, stream: xconn::async_::rawsocket::BoxStream) {
    use wampproto::messages::goodbye::Goodbye;
    use xconn::async_::{joiner::join, rawsocket::client_handshake};

    let peer = client_handshake(stream, CBORSerializerSpec.serializer_id())
        .await
        .unwrap();
    let serializer = CBORSerializerSpec.serializer();
    let (peer, _) = join(
        peer,
        "realm",
        serializer.clone(),
        Box::new(AnonymousAuthenticator::default()),
    )
    .await
    .unwrap();
    // A completed RPC confirms the router attached the session before shutdown.
    peer.write(
        serializer
            .serialize(&wampproto::messages::call::Call {
                request_id: 1,
                options: Default::default(),
                procedure: "missing".into(),
                args: None,
                kwargs: None,
            })
            .unwrap(),
    )
    .await
    .unwrap();
    peer.read().await.unwrap();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let message = serializer.deserialize(peer.read().await.unwrap()).unwrap();
    let goodbye = message.as_any().downcast_ref::<Goodbye>().unwrap();
    assert_eq!(goodbye.reason, "wamp.close.system_shutdown");
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn quic_listener_routes_rpc_streams() {
    use xconn::async_::quic::{ALPN, Connection};

    tokio::time::timeout(Duration::from_secs(15), async {
        let files = Files::new();
        let path = files.config("quic", "json");
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(files.0.join("cert.pem"), generated.cert.pem()).unwrap();
        std::fs::write(files.0.join("key.pem"), generated.key_pair.serialize_pem()).unwrap();
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("listener: tcp", "listener: udp")
            .replace("serializers: [json]", "tls: {certificate: cert.pem, key: key.pem}");
        std::fs::write(&path, text).unwrap();
        let (child, address) = start(&path).await;
        let address = address.parse().unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(generated.cert.der().clone()).unwrap();
        let mut tls =
            rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let config = quinn::ClientConfig::new(std::sync::Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
        ));
        let a = Connection::connect(address, "localhost", config.clone()).await.unwrap();
        let b = Connection::connect(address, "localhost", config).await.unwrap();
        let caller = a
            .open_session(
                "realm",
                Box::new(CBORSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .unwrap();
        let callee = b
            .open_session(
                "realm",
                Box::new(CBORSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .unwrap();
        assert_rpc_stream(&caller, &callee).await;
        assert_unassociated_stream_rejected(Box::new(a.open_raw().await.unwrap())).await;
        #[cfg(unix)]
        assert_udp_shutdown(child, Box::new(a.open_raw().await.unwrap())).await;
        #[cfg(not(unix))]
        {
            let mut child = child;
            child.kill().await.unwrap();
        }
    })
    .await
    .unwrap();
}
