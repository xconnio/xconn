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
    let (child, listeners) = start_listeners(file).await;
    let (_, endpoint) = listeners.into_iter().next().unwrap();
    let (_, address) = endpoint.split_once("://").unwrap();
    (child, address.into())
}

type RouterLogs = tokio::io::Lines<BufReader<tokio::process::ChildStderr>>;

fn spawn_router(file: &std::path::Path) -> (tokio::process::Child, RouterLogs) {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_xconn"));
    command.args(["router", "start", "--config"]).arg(file);
    let mut child = command
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let lines = BufReader::new(child.stderr.take().unwrap()).lines();
    (child, lines)
}

fn listener_address(line: &str) -> Option<(&str, &str)> {
    if let Some((_, endpoint)) = line.split_once(" INFO Serving WebTransport certificate fingerprint at ") {
        return Some(("cert-hash", endpoint));
    }
    let (_, listener) = line.split_once(" INFO Listening on ")?;
    let (endpoint, transport) = listener.split_once(" (")?;
    let (transport, _) = transport.split_once(')')?;
    Some((transport, endpoint))
}

async fn next_router_log(logs: &mut RouterLogs) -> String {
    tokio::time::timeout(Duration::from_secs(10), logs.next_line())
        .await
        .expect("timed out waiting for router startup")
        .unwrap()
        .expect("router exited before becoming ready")
}

async fn wait_for_router_ready(logs: &mut RouterLogs, file: &std::path::Path) {
    loop {
        let line = next_router_log(logs).await;
        if line.contains(" INFO Router ready (config ") {
            assert!(
                line.ends_with(&format!(" INFO Router ready (config {})", file.display())),
                "{line}"
            );
            break;
        }
    }
}

async fn start_listeners(file: &std::path::Path) -> (tokio::process::Child, Vec<(String, String)>) {
    let (child, mut lines) = spawn_router(file);
    let mut listeners = Vec::new();
    loop {
        let line = next_router_log(&mut lines).await;
        if line.contains(" INFO Router ready (config ") {
            break;
        }
        if let Some((kind, address)) = listener_address(&line) {
            listeners.push((kind.into(), address.into()));
        }
    }
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    (child, listeners)
}

fn client(spec: Box<dyn SerializerSpec>) -> Client {
    Client::new(spec, Box::new(AnonymousAuthenticator::default()))
}

#[tokio::test]
async fn executable_routes_and_enforces_serializer_policy() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for kind in ["websocket", "rawsocket", "universal", "yamux"] {
            let files = Files::new();
            let path = files.config(kind, "json");
            let (mut child, address) = start(&path).await;
            for scheme in match kind {
                "websocket" => vec!["ws"],
                "rawsocket" => vec!["rs"],
                "yamux" => vec!["yamux"],
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
    let path = files.0.join("xconn/xconn.yaml");
    let instance = path.parent().unwrap();
    for name in ["router.key", "router.pub"] {
        let key = std::fs::read_to_string(instance.join(name)).unwrap();
        assert_eq!(key.trim_end().len(), 64);
        assert!(key.trim_end().bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(instance.join("router.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    let certificate = instance.join("tls-cert.pem");
    let tls_key = instance.join("tls-key.pem");
    let original_certificate = std::fs::read_to_string(&certificate).unwrap();
    assert!(original_certificate.starts_with("-----BEGIN CERTIFICATE-----"));
    assert!(
        std::fs::read_to_string(&tls_key)
            .unwrap()
            .starts_with("-----BEGIN PRIVATE KEY-----")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&tls_key).unwrap().permissions().mode() & 0o777, 0o600);
    }

    for name in ["xconn.yaml", "router.key", "router.pub", "tls-key.pem"] {
        std::fs::remove_file(instance.join(name)).unwrap();
    }
    assert!(!run("init").status.success());
    assert!(!path.exists());
    assert!(!instance.join("router.key").exists());
    assert!(!instance.join("router.pub").exists());
    assert!(!tls_key.exists());
    assert_eq!(std::fs::read_to_string(&certificate).unwrap(), original_certificate);
    std::fs::remove_file(certificate).unwrap();
    assert!(run("init").status.success());

    std::fs::write(instance.join("xconn"), "").unwrap();
    let inside = std::process::Command::new(bin)
        .args(["router", "validate"])
        .current_dir(instance)
        .output()
        .unwrap();
    assert!(inside.status.success());

    let local = files.0.join("xconn.yaml");
    std::fs::copy(&path, &local).unwrap();
    let ambiguous = run("validate");
    assert!(!ambiguous.status.success());
    assert!(String::from_utf8_lossy(&ambiguous.stderr).contains("Use --config to choose one."));
    let explicit = std::process::Command::new(bin)
        .args(["router", "validate", "--config", "xconn.yaml"])
        .current_dir(&files.0)
        .output()
        .unwrap();
    assert!(explicit.status.success());
    std::fs::remove_file(local).unwrap();

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

    let path = files.0.join("xconn/xconn.yaml");
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

fn quic_test_identity(files: &Files) -> quinn::ClientConfig {
    use xconn::async_::quic::ALPN;

    let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(files.0.join("cert.pem"), generated.cert.pem()).unwrap();
    std::fs::write(files.0.join("key.pem"), generated.key_pair.serialize_pem()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(generated.cert.der().clone()).unwrap();
    let mut tls =
        rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    quinn::ClientConfig::new(std::sync::Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ))
}

#[tokio::test]
async fn quic_listener_routes_rpc_streams() {
    use xconn::async_::quic::Connection;

    tokio::time::timeout(Duration::from_secs(15), async {
        let files = Files::new();
        let path = files.config("quic", "json");
        let config = quic_test_identity(&files);
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("listener: tcp", "listener: udp")
            .replace(
                "serializers: [json]",
                "serializers: [cbor]\n    tls: {certificate: cert.pem, key: key.pem}",
            );
        std::fs::write(&path, text).unwrap();
        let (child, address) = start(&path).await;
        let address = address.parse().unwrap();
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
        assert!(
            a.open_session(
                "realm",
                Box::new(JSONSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .is_err()
        );
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

#[tokio::test]
async fn webtransport_routes_rpc_streams_to_quic_using_the_published_certificate_hash() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use xconn::async_::webtransport::{Connection, backend};

    tokio::time::timeout(Duration::from_secs(15), async {
        let files = Files::new();
        let path = files.config("webtransport", "json");
        let quic_config = quic_test_identity(&files);
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("listener: tcp", "listener: udp")
            .replace(
                "serializers: [json]",
                "serializers: [cbor]\n    path: /terminal\n    cert_hash_address: 127.0.0.1:0",
            )
            .replace(
                "authenticators:",
                "  - type: quic\n    listener: udp\n    address: 127.0.0.1:0\n    tls: {certificate: cert.pem, key: key.pem}\nauthenticators:",
            );
        std::fs::write(&path, text).unwrap();
        let (child, listeners) = start_listeners(&path).await;
        assert_eq!(listeners.len(), 3);
        assert_eq!(listeners[0].0, "WebTransport");
        assert_eq!(listeners[1].0, "cert-hash");
        assert_eq!(listeners[2].0, "QUIC");
        let endpoint = url::Url::parse(&listeners[0].1).unwrap();
        assert_eq!(endpoint.scheme(), "https");
        assert_eq!(endpoint.path(), "/terminal");
        let hash_url = url::Url::parse(&listeners[1].1).unwrap();
        let request = |path: &'static str| {
            let addr = (hash_url.host_str().unwrap().to_owned(), hash_url.port().unwrap());
            async move {
                let mut stream = tokio::net::TcpStream::connect((addr.0.as_str(), addr.1)).await.unwrap();
                stream
                    .write_all(
                        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes(),
                    )
                    .await
                    .unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).await.unwrap();
                response
            }
        };
        let response = request("/wt-cert-hash").await;
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with("HTTP/1.1 200"));
        assert!(headers.contains("access-control-allow-origin: *"));
        assert!(headers.contains("cache-control: no-store"));
        assert!(request("/unknown").await.starts_with("HTTP/1.1 404"));
        let encoded = body
            .strip_prefix(r#"{"hash":""#)
            .unwrap()
            .strip_suffix(r#""}"#)
            .unwrap();
        let hash = STANDARD.decode(encoded).unwrap();
        assert_eq!(hash.len(), 32);
        let config = backend::ClientBuilder::new()
            .with_handshake_timeout(Duration::from_secs(3))
            .with_server_certificate_hashes(vec![hash])
            .unwrap();
        assert!(
            Connection::connect(endpoint.join("/wrong").unwrap().as_str(), config.clone())
                .await
                .is_err()
        );
        let a = Connection::connect(endpoint.as_str(), config).await.unwrap();
        let quic_address = listeners[2].1.strip_prefix("quic://").unwrap().parse().unwrap();
        let b = xconn::async_::quic::Connection::connect(quic_address, "localhost", quic_config)
            .await
            .unwrap();
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
                Box::new(JSONSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .unwrap();
        assert_rpc_stream(&caller, &callee).await;
        assert!(
            a.open_session(
                "realm",
                Box::new(JSONSerializerSpec),
                Box::new(AnonymousAuthenticator::default()),
            )
            .await
            .is_err()
        );
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

fn management_config(files: &Files) -> PathBuf {
    let path = files.config("websocket", "json");
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("management: false", "management: true")
        .replace(
            "transports:",
            r#"  - name: io.xconn.mgmt
    roles:
      - name: admin
        permissions:
          - uri: io.xconn.mgmt.
            match: prefix
            allow_call: true
      - name: viewer
        permissions:
          - uri: io.xconn.mgmt.realm.list
            match: exact
            allow_call: true
transports:"#,
        );
    std::fs::write(
        &path,
        format!(
            "{text}  ticket:\n    - {{authid: admin, realm: io.xconn.mgmt, role: admin, ticket: admin-secret}}\n    - {{authid: viewer, realm: io.xconn.mgmt, role: viewer, ticket: viewer-secret}}\n"
        ),
    )
    .unwrap();
    path
}

#[test]
fn management_configuration_rejects_anonymous_and_reserved_roles() {
    let files = Files::new();
    let path = management_config(&files);
    let text = std::fs::read_to_string(&path).unwrap().replace("\r\n", "\n");
    let validate = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_xconn"))
            .args(["router", "validate", "--config"])
            .arg(&path)
            .output()
            .unwrap()
    };
    assert!(validate().status.success());
    std::fs::write(
        &path,
        text.replace("role: anonymous", "role: admin")
            .replace("realm: realm\n", "realm: io.xconn.mgmt\n"),
    )
    .unwrap();
    let rejected = validate();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("requires authenticated credentials"));

    std::fs::write(
        &path,
        text.replace("name: admin", "name: trusted")
            .replace("role: admin", "role: trusted"),
    )
    .unwrap();
    let rejected = validate();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("reserved for the internal management session"));
}

#[tokio::test]
async fn executable_exposes_management_only_to_authorized_roles() {
    use wampproto::authenticators::ticket::TicketAuthenticator;
    use xconn::async_::management::{KILL_SESSION, LIST_REALMS, LIST_SESSIONS, REALM};

    tokio::time::timeout(Duration::from_secs(10), async {
        let files = Files::new();
        let path = management_config(&files);
        let (mut child, address) = start(&path).await;
        let url = format!("ws://{address}");
        let ticket_client = |authid, ticket| {
            Client::new(
                Box::new(JSONSerializerSpec),
                Box::new(TicketAuthenticator::new(authid, ticket, Default::default())),
            )
        };
        assert!(client(Box::new(JSONSerializerSpec)).connect(&url, REALM).await.is_err());
        assert!(ticket_client("admin", "wrong").connect(&url, REALM).await.is_err());

        // The client proposes anonymous first. Only ticket is configured for this realm.
        {
            use futures_util::{SinkExt, StreamExt};
            use wampproto::messages::{challenge::Challenge, welcome::Welcome};

            let request = tungstenite::ClientRequestBuilder::new(url.parse().unwrap())
                .with_sub_protocol("wamp.2.json");
            let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
            socket
                .send(tungstenite::Message::Text(
                    r#"[1,"io.xconn.mgmt",{"authid":"admin","authmethods":["anonymous","ticket"],"roles":{"caller":{}}}]"#
                        .into(),
                ))
                .await
                .unwrap();
            let serializer = JSONSerializerSpec.serializer();
            let reply = socket.next().await.unwrap().unwrap().into_data().to_vec();
            let challenge = serializer.deserialize(reply).unwrap();
            assert_eq!(challenge.as_any().downcast_ref::<Challenge>().unwrap().auth_method, "ticket");
            socket
                .send(tungstenite::Message::Text(r#"[5,"admin-secret",{}]"#.into()))
                .await
                .unwrap();
            let reply = socket.next().await.unwrap().unwrap().into_data().to_vec();
            assert!(serializer.deserialize(reply).unwrap().as_any().is::<Welcome>());
            socket.close(None).await.unwrap();
        }

        let viewer = ticket_client("viewer", "viewer-secret")
            .connect(&url, REALM)
            .await
            .unwrap();
        let listed = viewer.execute_call(CallRequest::new(LIST_REALMS)).await.unwrap();
        let realms = listed.args.unwrap();
        let Value::List(realms) = &realms[0] else {
            panic!("expected a realm list");
        };
        assert!(realms.contains(&Value::Str("realm".into())));
        assert!(realms.contains(&Value::Str(REALM.into())));

        let user = client(Box::new(JSONSerializerSpec))
            .connect(&url, "realm")
            .await
            .unwrap();
        let kill = || CallRequest::new(KILL_SESSION).arg("realm").arg(user.details().id());
        let denied = viewer.execute_call(kill()).await.unwrap();
        assert_eq!(denied.error.unwrap().uri, "wamp.error.authorization_failed");

        let admin = ticket_client("admin", "admin-secret")
            .connect(&url, REALM)
            .await
            .unwrap();
        let listed = admin
            .execute_call(CallRequest::new(LIST_SESSIONS).arg("realm"))
            .await
            .unwrap();
        assert_eq!(listed.kwargs.unwrap()["total"], Value::Int(1));
        let self_kill = CallRequest::new(KILL_SESSION).arg(REALM).arg(admin.details().id());
        assert_eq!(
            admin.execute_call(self_kill).await.unwrap().error.unwrap().uri,
            "wamp.error.invalid_argument"
        );
        assert!(admin.execute_call(kill()).await.unwrap().error.is_none());
        user.wait_disconnect().await;
        child.kill().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn management_procedures_are_absent_when_disabled() {
    use wampproto::authenticators::ticket::TicketAuthenticator;
    use xconn::async_::management::{LIST_REALMS, REALM};

    tokio::time::timeout(Duration::from_secs(10), async {
        let files = Files::new();
        let path = management_config(&files);
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("management: true", "management: false");
        std::fs::write(&path, text).unwrap();
        let (mut child, address) = start(&path).await;
        let admin = Client::new(
            Box::new(JSONSerializerSpec),
            Box::new(TicketAuthenticator::new("admin", "admin-secret", Default::default())),
        )
        .connect(&format!("ws://{address}"), REALM)
        .await
        .unwrap();
        let reply = admin.execute_call(CallRequest::new(LIST_REALMS)).await.unwrap();
        assert_eq!(reply.error.unwrap().uri, "wamp.error.no_such_procedure");
        child.kill().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn management_provisions_and_removes_realms() {
    use futures_util::{SinkExt, StreamExt};
    use wampproto::{
        authenticators::ticket::TicketAuthenticator,
        messages::{abort::Abort, challenge::Challenge},
    };
    use xconn::async_::management::{CREATE_REALM, REALM, REMOVE_REALM};

    tokio::time::timeout(Duration::from_secs(10), async {
        let files = Files::new();
        let path = management_config(&files);
        let (mut child, address) = start(&path).await;
        let url = format!("ws://{address}");
        let ticket_client = |authid, ticket| {
            Client::new(
                Box::new(JSONSerializerSpec),
                Box::new(TicketAuthenticator::new(authid, ticket, Default::default())),
            )
        };
        let admin = ticket_client("admin", "admin-secret")
            .connect(&url, REALM)
            .await
            .unwrap();
        let viewer = ticket_client("viewer", "viewer-secret")
            .connect(&url, REALM)
            .await
            .unwrap();
        let realm = "test.created";
        let spec: Value = serde_yaml_ng::from_str(
            r#"
roles:
  - name: member
    permissions:
      - uri: echo
        match: exact
        allow_call: true
        allow_register: true
authenticators:
  ticket:
    - authid: member
      realm: test.created
      role: member
      ticket: member-secret
"#,
        )
        .unwrap();
        let create = || CallRequest::new(CREATE_REALM).arg(realm).arg(spec.clone());
        let remove = |realm: &str| CallRequest::new(REMOVE_REALM).arg(realm);
        assert_eq!(
            viewer.execute_call(create()).await.unwrap().error.unwrap().uri,
            "wamp.error.authorization_failed"
        );
        assert_eq!(
            admin.execute_call(remove("realm")).await.unwrap().error.unwrap().uri,
            "wamp.error.no_such_realm"
        );
        assert_eq!(
            admin.execute_call(remove(REALM)).await.unwrap().error.unwrap().uri,
            "wamp.error.invalid_argument"
        );
        assert_eq!(
            admin
                .execute_call(CallRequest::new(CREATE_REALM).arg(realm).arg(Value::Null))
                .await
                .unwrap()
                .error
                .unwrap()
                .uri,
            "wamp.error.invalid_argument"
        );
        assert!(admin.execute_call(create()).await.unwrap().error.is_none());
        assert_eq!(
            admin.execute_call(create()).await.unwrap().error.unwrap().uri,
            "wamp.error.already_exists"
        );
        assert!(ticket_client("member", "wrong").connect(&url, realm).await.is_err());
        let member = ticket_client("member", "member-secret")
            .connect(&url, realm)
            .await
            .unwrap();
        assert!(
            member
                .execute_register(RegisterRequest::new("echo", |_| async { RpcResult::arg("ok") }))
                .await
                .unwrap()
                .error
                .is_none()
        );
        assert_eq!(
            member.execute_call(CallRequest::new("echo")).await.unwrap().args,
            Some(vec![Value::Str("ok".into())])
        );
        assert_eq!(
            viewer.execute_call(remove(realm)).await.unwrap().error.unwrap().uri,
            "wamp.error.authorization_failed"
        );
        let request = tungstenite::ClientRequestBuilder::new(url.parse().unwrap()).with_sub_protocol("wamp.2.json");
        let (mut pending, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        pending
            .send(tungstenite::Message::Text(
                r#"[1,"test.created",{"authid":"member","authmethods":["ticket"],"roles":{"caller":{}}}]"#.into(),
            ))
            .await
            .unwrap();
        let serializer = JSONSerializerSpec.serializer();
        let reply = pending.next().await.unwrap().unwrap().into_data().to_vec();
        assert!(serializer.deserialize(reply).unwrap().as_any().is::<Challenge>());

        assert!(admin.execute_call(remove(realm)).await.unwrap().error.is_none());
        member.wait_disconnect().await;
        assert!(
            ticket_client("member", "member-secret")
                .connect(&url, realm)
                .await
                .is_err()
        );
        assert_eq!(
            admin.execute_call(remove(realm)).await.unwrap().error.unwrap().uri,
            "wamp.error.no_such_realm"
        );
        assert!(admin.execute_call(create()).await.unwrap().error.is_none());
        pending
            .send(tungstenite::Message::Text(r#"[5,"member-secret",{}]"#.into()))
            .await
            .unwrap();
        let reply = pending.next().await.unwrap().unwrap().into_data().to_vec();
        let message = serializer.deserialize(reply).unwrap();
        let abort = message
            .as_any()
            .downcast_ref::<Abort>()
            .expect("stale handshake must abort");
        assert_eq!(abort.reason, "wamp.error.no_such_realm");

        assert!(
            ticket_client("member", "member-secret")
                .connect(&url, realm)
                .await
                .is_ok()
        );
        child.kill().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn realm_source_applies_updates_received_during_inventory_loading() {
    use tokio::sync::Notify;
    use wampproto::authenticators::ticket::TicketAuthenticator;
    use xconn::async_::management::{LIST_REALMS, REALM};

    tokio::time::timeout(Duration::from_secs(10), async {
        let files = Files::new();
        let path = management_config(&files);
        let config = std::fs::read_to_string(&path)
            .unwrap()
            .replace("  - name: realm", "  - name: realm\n    disclose_caller: true")
            .replace(
                "  - name: io.xconn.mgmt",
                r#"      - name: source
        permissions:
          - {uri: fixture.inventory., match: prefix, allow_call: true, allow_register: true}
      - name: provider
        permissions:
          - {uri: fixture.inventory.list, match: exact, allow_register: true}
          - {uri: fixture.inventory.add, match: exact, allow_call: true}
          - {uri: fixture.inventory.remove, match: exact, allow_call: true}
  - name: io.xconn.mgmt"#,
            )
            .replace(
                "authenticators:",
                r#"realm_source:
  control_realm: realm
  role: source
  list: fixture.inventory.list
  add: fixture.inventory.add
  remove: fixture.inventory.remove
authenticators:"#,
            )
            + "    - {authid: provider, realm: realm, role: provider, ticket: provider-secret}\n";
        for (field, message) in [
            ("allow_call", "must be allowed to call fixture.inventory.list"),
            ("allow_register", "must be allowed to register fixture.inventory.add"),
        ] {
            let no_permission = config.replace(
                "{uri: fixture.inventory., match: prefix, allow_call: true, allow_register: true}",
                &format!(
                    "{{uri: fixture.inventory., match: prefix, allow_call: {}, allow_register: {}}}",
                    field != "allow_call",
                    field != "allow_register"
                ),
            );
            std::fs::write(&path, no_permission).unwrap();
            let validation = std::process::Command::new(env!("CARGO_BIN_EXE_xconn"))
                .args(["router", "validate", "--config"])
                .arg(&path)
                .output()
                .unwrap();
            assert!(!validation.status.success());
            let error = String::from_utf8_lossy(&validation.stderr);
            assert!(error.contains(message), "{error}");
        }
        std::fs::write(&path, config).unwrap();

        let (mut child, mut lines) = spawn_router(&path);
        let line = next_router_log(&mut lines).await;
        let (kind, url) = listener_address(&line).unwrap();
        assert_eq!(kind, "WebSocket");
        assert!(url.starts_with("ws://"), "{url}");
        let url = url.to_owned();
        let ticket_client = |authid, ticket| {
            Client::new(
                Box::new(JSONSerializerSpec),
                Box::new(TicketAuthenticator::new(authid, ticket, Default::default())),
            )
        };
        let provider = ticket_client("provider", "provider-secret")
            .connect(&url, "realm")
            .await
            .unwrap();
        let requested = std::sync::Arc::new(Notify::new());
        let release = std::sync::Arc::new(Notify::new());
        let started = requested.clone();
        let proceed = release.clone();
        let registered = provider
            .execute_register(RegisterRequest::new("fixture.inventory.list", move |inv| {
                let started = started.clone();
                let proceed = proceed.clone();
                async move {
                    assert_eq!(inv.details.get("caller_authrole"), Some(&Value::Str("source".into())));
                    started.notify_one();
                    proceed.notified().await;
                    RpcResult::arg(Value::List(
                        [("machine.loaded", "a"), ("machine.removed", "b")]
                            .into_iter()
                            .map(|(realm, authid)| {
                                Value::Dict(std::collections::HashMap::from([
                                    ("realm".into(), Value::Str(realm.into())),
                                    ("authid".into(), Value::Str(authid.into())),
                                ]))
                            })
                            .collect(),
                    ))
                }
            }))
            .await
            .unwrap();
        assert!(registered.error.is_none());
        requested.notified().await;

        let update =
            |procedure, realm, authid| provider.execute_call(CallRequest::new(procedure).arg(realm).arg(authid));
        assert!(
            update("fixture.inventory.remove", "machine.removed", "b")
                .await
                .unwrap()
                .error
                .is_none()
        );
        assert!(
            update("fixture.inventory.add", "machine.added", "c")
                .await
                .unwrap()
                .error
                .is_none()
        );
        assert!(
            update("fixture.inventory.remove", "machine.loaded", "wrong-authid")
                .await
                .unwrap()
                .error
                .is_none()
        );
        release.notify_one();
        wait_for_router_ready(&mut lines, &path).await;

        let admin = ticket_client("admin", "admin-secret")
            .connect(&url, REALM)
            .await
            .unwrap();
        let list = || admin.execute_call(CallRequest::new(LIST_REALMS));
        let reply = list().await.unwrap();
        let Some([Value::List(realms)]) = reply.args.as_deref() else {
            panic!("expected realm list")
        };
        assert!(realms.contains(&Value::Str("machine.loaded".into())));
        assert!(realms.contains(&Value::Str("machine.added".into())));
        assert!(!realms.contains(&Value::Str("machine.removed".into())));
        assert!(
            update("fixture.inventory.remove", "machine.loaded", "a")
                .await
                .unwrap()
                .error
                .is_none()
        );
        assert!(
            update("fixture.inventory.remove", "realm", "a")
                .await
                .unwrap()
                .error
                .is_some()
        );
        child.kill().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rpc_authentication_preserves_static_bootstrap_and_applies_remote_roles() {
    use std::collections::HashMap;
    use wampproto::authenticators::{
        authenticator::ClientAuthenticator,
        cryptosign::CryptoSignAuthenticator,
        server::{CRAKey, Salt},
        wampcra::WAMPCRAAuthenticator,
    };

    tokio::time::timeout(Duration::from_secs(15), async {
        let files = Files::new();
        let path = files.0.join("config.yaml");
        let crypto = |id: &str, byte: &str| {
            Box::new(CryptoSignAuthenticator::try_new(id, &byte.repeat(32), HashMap::new()).unwrap())
        };
        let provider = crypto("provider", "11");
        let provider_key = match &provider.auth_extra()["pubkey"] {
            Value::Str(key) => key.clone(),
            _ => panic!("expected CryptoSign public key"),
        };
        let config = r#"
version: '1'
realms:
  - name: control
    disclose_caller: true
    roles:
      - name: provider
        permissions:
          - {uri: fixture., match: prefix, allow_register: true}
          - {uri: fixture.remove, match: exact, allow_call: true}
      - name: source
        permissions:
          - {uri: fixture., match: prefix, allow_call: true, allow_register: true}
      - name: user:cra
        permissions:
          - {uri: fixture.echo, match: exact, allow_call: true}
realm_source:
  control_realm: control
  role: source
  list: fixture.list
  add: fixture.add
  remove: fixture.remove
  authentication:
    cryptosign: fixture.verify_key
    wampcra: fixture.verify_password
    control_roles: [user:cra]
    realm_roles:
      - name: user:member
        permissions:
          - {uri: fixture.echo, match: exact, allow_call: true}
    realm_machine_role:
      name: 'machine:{authid}'
      permissions:
        - {uri: fixture.echo, match: exact, allow_register: true}
    control_machine_role:
      name: 'machine:{authid}'
      permissions:
        - {uri: 'fixture.machine.{authid}', match: exact, allow_call: true}
transports:
  - {type: websocket, listener: tcp, address: '127.0.0.1:0', serializers: [json]}
authenticators:
  cryptosign:
    - {realm: control, role: provider, authid: provider, authorized_keys: ['PROVIDER_KEY']}
"#;
        std::fs::write(&path, config.replace("PROVIDER_KEY", &provider_key)).unwrap();

        let (mut child, mut lines) = spawn_router(&path);
        let line = next_router_log(&mut lines).await;
        let (kind, url) = listener_address(&line).unwrap();
        assert_eq!(kind, "WebSocket");
        assert!(url.starts_with("ws://"), "{url}");
        let url = url.to_owned();
        let client =
            |authenticator: Box<dyn ClientAuthenticator>| Client::new(Box::new(JSONSerializerSpec), authenticator);
        // A static identity that offers multiple methods must be challenged with its configured method.
        {
            use futures_util::{SinkExt, StreamExt};
            use wampproto::{
                joiner::get_client_roles,
                messages::{challenge::Challenge, hello::Hello},
            };

            let request = tungstenite::ClientRequestBuilder::new(url.parse().unwrap()).with_sub_protocol("wamp.2.json");
            let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
            let hello = Hello::new(
                "control",
                "provider",
                provider.auth_extra(),
                get_client_roles(),
                vec!["wampcra".into(), "cryptosign".into()],
            );
            let serializer = JSONSerializerSpec.serializer();
            let message = String::from_utf8(serializer.serialize(&hello).unwrap()).unwrap();
            socket.send(tungstenite::Message::Text(message.into())).await.unwrap();
            let reply = socket.next().await.unwrap().unwrap().into_data().to_vec();
            let message = serializer.deserialize(reply).unwrap();
            let challenge = message
                .as_any()
                .downcast_ref::<Challenge>()
                .expect("static CryptoSign challenge");
            assert_eq!(challenge.auth_method, "cryptosign");
            socket.close(None).await.unwrap();
        }
        let provider = client(provider).connect(&url, "control").await.unwrap();
        let registration = provider
            .register("fixture.list", |_| async {
                RpcResult::arg(Value::List(vec![Value::Dict(HashMap::from([
                    ("realm".into(), Value::Str("test.machine".into())),
                    ("authid".into(), Value::Str("host".into())),
                ]))]))
            })
            .execute()
            .await
            .unwrap();
        assert!(registration.error.is_none());
        let registration = provider
            .register("fixture.verify_key", |inv| async move {
                assert_eq!(inv.details["caller_authrole"], Value::Str("source".into()));
                let [Value::Str(id), Value::Str(key), Value::Str(realm)] = inv.args.as_slice() else {
                    panic!("expected identity, public key and realm");
                };
                assert_eq!(key.len(), 64);
                let role = match (id.as_str(), realm.as_str()) {
                    ("member", "test.machine") => "user:member",
                    ("host", _) | ("other-host", "test.machine") => "machine:host",
                    ("provider", "control") => "user:cra",
                    ("intruder", "control") => "source",
                    _ => return RpcResult::error("fixture.denied"),
                };
                RpcResult::arg(Value::Dict(HashMap::from([
                    ("authid".into(), Value::Str(id.clone())),
                    ("authrole".into(), Value::Str(role.into())),
                ])))
            })
            .execute()
            .await
            .unwrap();
        assert!(registration.error.is_none());

        let salt = Salt {
            salt: "test-salt".into(),
            iterations: 1000,
            key_length: 32,
        };
        let key = CRAKey::from_secret("password", Some(salt.clone())).unwrap().key;
        let registration = provider
            .register("fixture.verify_password", move |inv| {
                let salt = salt.clone();
                let key = String::from_utf8(key.clone()).unwrap();
                async move {
                    assert_eq!(inv.args, vec![Value::Str("user".into()), Value::Str("control".into())]);
                    RpcResult::arg(Value::Dict(HashMap::from([
                        ("authrole".into(), Value::Str("user:cra".into())),
                        ("password".into(), Value::Str(key)),
                        ("salt".into(), Value::Str(salt.salt)),
                        ("iterations".into(), Value::Int(i64::from(salt.iterations))),
                        ("key_length".into(), Value::Int(salt.key_length as i64)),
                    ])))
                }
            })
            .execute()
            .await
            .unwrap();
        assert!(registration.error.is_none());
        wait_for_router_ready(&mut lines, &path).await;

        assert!(client(crypto("provider", "22")).connect(&url, "control").await.is_err());
        assert!(client(crypto("intruder", "33")).connect(&url, "control").await.is_err());
        assert!(
            client(crypto("denied", "44"))
                .connect(&url, "test.machine")
                .await
                .is_err()
        );
        assert!(
            client(crypto("other-host", "55"))
                .connect(&url, "test.machine")
                .await
                .is_err()
        );
        let host = client(crypto("host", "66"))
            .connect(&url, "test.machine")
            .await
            .unwrap();
        let member = client(crypto("member", "77"))
            .connect(&url, "test.machine")
            .await
            .unwrap();
        let registration = host
            .register("fixture.echo", |_| async { RpcResult::arg("ok") })
            .execute()
            .await
            .unwrap();
        assert!(registration.error.is_none());
        assert_eq!(
            member.call("fixture.echo").execute().await.unwrap().args,
            Some(vec![Value::Str("ok".into())])
        );
        let host_on_control = client(crypto("host", "66")).connect(&url, "control").await.unwrap();
        let registration = provider
            .register("fixture.machine.host", |_| async { RpcResult::default() })
            .execute()
            .await
            .unwrap();
        assert!(registration.error.is_none());
        assert!(
            host_on_control
                .call("fixture.machine.host")
                .execute()
                .await
                .unwrap()
                .error
                .is_none()
        );
        assert_eq!(
            host_on_control
                .call("fixture.machine.other")
                .execute()
                .await
                .unwrap()
                .error
                .unwrap()
                .uri,
            "wamp.error.authorization_failed"
        );

        let password = |value| client(Box::new(WAMPCRAAuthenticator::new("user", value, HashMap::new())));
        assert!(password("wrong").connect(&url, "control").await.is_err());
        assert_eq!(
            password("password")
                .connect(&url, "control")
                .await
                .unwrap()
                .details()
                .auth_role(),
            "user:cra"
        );
        assert!(
            provider
                .call("fixture.remove")
                .arg("test.machine")
                .arg("host")
                .execute()
                .await
                .unwrap()
                .error
                .is_none()
        );
        host.wait_disconnect().await;
        member.wait_disconnect().await;
        assert_eq!(
            host_on_control
                .call("fixture.machine.host")
                .execute()
                .await
                .unwrap()
                .error
                .unwrap()
                .uri,
            "wamp.error.authorization_failed"
        );
        assert!(client(crypto("host", "66")).connect(&url, "control").await.is_err());
        child.kill().await.unwrap();
    })
    .await
    .unwrap();
}
