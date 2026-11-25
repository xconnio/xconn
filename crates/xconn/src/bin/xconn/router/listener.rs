use super::{
    cert_hash::CertHashServer,
    config::{Result, Tls, Transport},
};
use futures_util::StreamExt;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::TlsAcceptor;
use wampproto::transports::rawsocket::{self as rs, SerializerID};
use xconn::async_::{
    quic,
    rawsocket::{BoxStream, RawSocketPeer},
    router::{Router, serializer},
    websocket::WebSocketPeer,
    webtransport,
};

pub enum Listener {
    Stream(StreamListener),
    Quic(quic::Listener),
    WebTransport(webtransport::Listener, Option<CertHashServer>),
}

impl Listener {
    pub async fn bind(router: Router, config: Transport) -> Result<Self> {
        if config.kind == "quic" {
            let tls = config.tls.as_ref().ok_or("QUIC requires a TLS certificate and key")?;
            let (chain, key) = tls_identity(tls)?;
            let mut listener = quic::Listener::with_certificate_and_serializers(
                router,
                config.address.parse()?,
                chain.into_iter().map(|cert| cert.to_vec()).collect(),
                key.secret_der().to_vec(),
                codecs(&config),
            )
            .await?;
            listener.disable_raw_streams();
            return Ok(Self::Quic(listener));
        }
        if config.kind == "webtransport" {
            let (chain, key) = match &config.tls {
                Some(tls) => tls_identity(tls)?,
                None => self_signed_webtransport_identity()?,
            };
            let serializers = codecs(&config);
            let mut listener = webtransport::Listener::with_certificate_and_serializers(
                router,
                config.address.parse()?,
                chain,
                key,
                Some(config.path.unwrap_or_else(|| "/wamp".into())),
                serializers,
            )
            .await?;
            listener.disable_raw_streams();
            let hash_server = match config.cert_hash_address {
                Some(address) => Some(CertHashServer::bind(address, &listener.certificate).await?),
                None => None,
            };
            return Ok(Self::WebTransport(listener, hash_server));
        }
        Ok(Self::Stream(StreamListener::bind(router, config).await?))
    }

    pub fn address(&self) -> String {
        match self {
            Self::Stream(listener) => listener.address.clone(),
            Self::Quic(listener) => listener.local_addr.to_string(),
            Self::WebTransport(listener, _) => listener.local_addr.to_string(),
        }
    }

    pub fn cert_hash_address(&self) -> Option<std::net::SocketAddr> {
        match self {
            Self::WebTransport(_, Some(server)) => Some(server.address),
            _ => None,
        }
    }

    pub fn stop(&self) {
        if let Self::Stream(listener) = self {
            listener.stop.send_replace(true);
        }
    }

    pub async fn finish(self) {
        match self {
            Self::Stream(mut listener) => {
                if let Some(task) = listener.task.take() {
                    let _ = task.await;
                }
            }
            listener => {
                // Keep the endpoint alive while WAMP GOODBYE is written and delivered.
                tokio::time::sleep(Duration::from_secs(1)).await;
                drop(listener);
            }
        }
    }
}

pub struct StreamListener {
    address: String,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
    unix_path: Option<PathBuf>,
}

impl Drop for StreamListener {
    fn drop(&mut self) {
        self.stop.send_replace(true);

        if let Some(task) = &self.task {
            task.abort();
        }

        if let Some(path) = &self.unix_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

enum Socket {
    Tcp(tokio::net::TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
}

impl Socket {
    async fn accept(&self) -> std::io::Result<BoxStream> {
        match self {
            Self::Tcp(socket) => {
                let (stream, _) = socket.accept().await?;
                let _ = stream.set_nodelay(true);
                Ok(Box::new(stream))
            }

            #[cfg(unix)]
            Self::Unix(socket) => {
                let (stream, _) = socket.accept().await?;
                Ok(Box::new(stream))
            }
        }
    }
}

type TlsIdentity = (
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
);

fn self_signed_webtransport_identity() -> Result<TlsIdentity> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let mut params = rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into(), "::1".into()])?;
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::minutes(1);
    params.not_after = now + time::Duration::days(13);
    params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let cert = params.self_signed(&key)?;
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der());
    Ok((vec![cert.der().clone()], key.into()))
}

fn tls_identity(tls: &Tls) -> Result<TlsIdentity> {
    let certificates = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(&tls.certificate)?))
        .collect::<std::io::Result<Vec<_>>>()?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(&tls.key)?))?
        .ok_or("TLS private key missing")?;
    Ok((certificates, key))
}

pub fn tls_config(tls: &Tls) -> Result<Arc<rustls::ServerConfig>> {
    let (certificates, key) = tls_identity(tls)?;
    Ok(Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(certificates, key)?,
    ))
}

impl StreamListener {
    async fn bind(router: Router, config: Transport) -> Result<Self> {
        // Parse credentials before reserving a socket, so startup errors unwind cleanly.
        let tls = config.tls.as_ref().map(tls_config).transpose()?.map(TlsAcceptor::from);
        let (socket, address, unix_path) = match config.listener.as_str() {
            "tcp" => {
                let socket = tokio::net::TcpListener::bind(&config.address).await?;
                let address = socket.local_addr()?.to_string();
                (Socket::Tcp(socket), address, None)
            }

            #[cfg(unix)]
            "unix" => {
                let socket = tokio::net::UnixListener::bind(&config.address)?;
                (
                    Socket::Unix(socket),
                    config.address.clone(),
                    Some(PathBuf::from(&config.address)),
                )
            }
            _ => return Err("unsupported listener".into()),
        };
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            let capacity = Arc::new(tokio::sync::Semaphore::new(1024));

            loop {
                tokio::select! {
                    biased;
                    _ = stopped.wait_for(|v| *v) => {
                        break;
                    }
                    Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
                    incoming = socket.accept() => {
                        let stream = match incoming {
                            Ok(stream) => stream,
                            Err(error) => {
                                tracing::error!(%error,"listener accept failed");
                                break;
                            }
                        };
                        let Ok(permit) = capacity.clone().try_acquire_owned() else {
                            continue;
                        };
                        let router = router.clone();
                        let config = config.clone();
                        let tls = tls.clone();
                        sessions.spawn(async move {
                            let _permit = permit;
                            let establish = async {
                                let stream: BoxStream = if let Some(tls) = tls {
                                    Box::new(tls.accept(stream).await?)
                                } else {
                                    stream
                                };
                                negotiate(stream, &config).await
                            };

                            match tokio::time::timeout(Duration::from_secs(10), establish).await {
                                Ok(Ok((peer, codec))) => {
                                    let _ = router.accept(peer, serializer(codec)).await;
                                }
                                Ok(Err(error)) => tracing::debug!(%error,"connection handshake rejected"),
                                Err(_) => tracing::debug!("connection handshake timed out"),
                            }
                        });
                    }
                }
            }
            // Router.close() queues GOODBYE first. Give established sessions time
            // to flush and receive the peer's acknowledgement before forcing close.
            let _ = tokio::time::timeout(Duration::from_secs(3), async {
                while sessions.join_next().await.is_some() {}
            })
            .await;
            sessions.abort_all();

            while sessions.join_next().await.is_some() {}
        });
        Ok(Self {
            address,
            stop,
            task: Some(task),
            unix_path,
        })
    }
}

fn codec(name: &str) -> SerializerID {
    match name {
        "json" => SerializerID::JSON,
        "cbor" => SerializerID::CBOR,
        _ => SerializerID::MSGPACK,
    }
}

fn codecs(config: &Transport) -> Vec<SerializerID> {
    config
        .serializers
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|name| codec(name))
        .collect()
}

async fn negotiate(
    stream: BoxStream,
    config: &Transport,
) -> Result<(Box<dyn xconn::async_::peer::Peer>, SerializerID)> {
    let mut stream = tokio::io::BufReader::new(stream);
    let websocket = if config.kind == "universal" {
        use tokio::io::AsyncBufReadExt;
        stream.fill_buf().await?.first() != Some(&0x7f)
    } else {
        config.kind == "websocket"
    };
    let allowed = codecs(config);

    if !websocket {
        use tokio::io::AsyncWriteExt;
        let mut header = [0; 4];
        stream.read_exact(&mut header).await?;
        let handshake = rs::receive_handshake(&header)?;
        let id = handshake.serializer_id();

        if !allowed.iter().any(|v| *v as u8 == id as u8) {
            stream.write_all(&[0x7f, 0x10, 0, 0]).await?;
            return Err("serializer disabled".into());
        }

        stream
            .write_all(&rs::send_handshake(&rs::Handshake::new(id, rs::DEFAULT_MAX_MSG_SIZE))?)
            .await?;
        return Ok((
            RawSocketPeer::from_stream(Box::new(stream), rs::DEFAULT_MAX_MSG_SIZE, handshake.max_message_size()),
            id,
        ));
    }

    let selected = Arc::new(Mutex::new(None));
    let output = selected.clone();

    #[allow(clippy::result_large_err)]
    let callback = move |request: &tungstenite::handshake::server::Request,
                         mut response: tungstenite::handshake::server::Response|
          -> std::result::Result<_, tungstenite::handshake::server::ErrorResponse> {
        let proposed = request
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let found = proposed.split(',').map(str::trim).find_map(|name| {
            let id = match name {
                "wamp.2.json" => SerializerID::JSON,
                "wamp.2.cbor" => SerializerID::CBOR,
                "wamp.2.msgpack" => SerializerID::MSGPACK,
                _ => return None,
            };
            allowed.iter().any(|v| *v as u8 == id as u8).then_some((name, id))
        });

        if let Some((name, id)) = found {
            response
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", name.parse().unwrap());
            *output.lock().unwrap() = Some(id);
            Ok(response)
        } else {
            Err(tungstenite::http::Response::builder()
                .status(400)
                .body(Some("enabled WAMP subprotocol required".into()))
                .unwrap())
        }
    };
    let ws = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
    let id = selected.lock().unwrap().take().ok_or("missing serializer")?;
    let (writer, reader) = ws.split();
    Ok((
        WebSocketPeer::new(reader, writer, !matches!(id, SerializerID::JSON)),
        id,
    ))
}
