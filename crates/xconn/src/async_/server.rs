//! Network listeners for the shared router runtime.
use super::{
    rawsocket::server_handshake,
    router::{Router, serializer},
    websocket::WebSocketPeer,
};
use crate::Error;
use futures_util::StreamExt;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use wampproto::transports::rawsocket::SerializerID;

pub type WebSocketProtocols = Arc<dyn Fn() -> Result<Vec<String>, Error> + Send + Sync>;

pub fn websocket_protocols() -> Vec<String> {
    ["wamp.2.json", "wamp.2.cbor", "wamp.2.msgpack"]
        .map(String::from)
        .to_vec()
}

pub fn select_websocket_protocol<'a>(offered: &'a str, supported: &[String]) -> Option<&'a str> {
    offered.split(',').map(str::trim).find(|protocol| {
        matches!(*protocol, "wamp.2.json" | "wamp.2.cbor" | "wamp.2.msgpack")
            && supported.iter().any(|value| value == protocol)
    })
}

#[derive(Clone)]
enum Transport {
    RawSocket,
    WebSocket,
    #[cfg(feature = "yamux")]
    Yamux(Option<Arc<rustls::ServerConfig>>),
}

pub struct Server {
    pub local_addr: SocketAddr,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.task.abort();
    }
}

impl Server {
    pub fn close(&self) {
        self.stop.send_replace(true);
    }

    pub async fn rawsocket(router: Router, addr: SocketAddr) -> Result<Self, Error> {
        Self::listen(router, addr, Transport::RawSocket, None).await
    }

    pub async fn websocket(router: Router, addr: SocketAddr) -> Result<Self, Error> {
        Self::listen(router, addr, Transport::WebSocket, None).await
    }

    pub async fn websocket_with_protocols(
        router: Router,
        addr: SocketAddr,
        protocols: WebSocketProtocols,
    ) -> Result<Self, Error> {
        Self::listen(router, addr, Transport::WebSocket, Some(protocols)).await
    }

    #[cfg(feature = "yamux")]
    pub async fn yamux(router: Router, addr: SocketAddr) -> Result<Self, Error> {
        Self::listen(router, addr, Transport::Yamux(None), None).await
    }

    #[cfg(feature = "yamux")]
    pub async fn yamux_tls(router: Router, addr: SocketAddr, tls: Arc<rustls::ServerConfig>) -> Result<Self, Error> {
        Self::listen(router, addr, Transport::Yamux(Some(tls)), None).await
    }

    #[allow(clippy::result_large_err)] // Required by tungstenite's callback signature.
    async fn listen(
        router: Router,
        addr: SocketAddr,
        transport: Transport,
        protocols: Option<WebSocketProtocols>,
    ) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await.map_err(|e| Error::new(e.to_string()))?;
        let local_addr = listener.local_addr().map_err(|e| Error::new(e.to_string()))?;
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();

            loop {
                tokio::select! {
                    _ = stopped.wait_for(|v| *v) => {
                        break;
                    }
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                    incoming = listener.accept() => {
                        let Ok((stream, _)) = incoming else { break };
                        let router = router.clone();
                        let protocols = protocols.clone();
                        let transport = transport.clone();
                        connections.spawn(async move {
                            serve_connection(router, stream, transport, protocols).await;
                        });
                    }
                }
            }

            connections.abort_all();

            while connections.join_next().await.is_some() {}
        });
        Ok(Self { local_addr, stop, task })
    }
}

#[allow(clippy::result_large_err)]
async fn serve_connection<S>(router: Router, stream: S, transport: Transport, protocols: Option<WebSocketProtocols>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    #[cfg(feature = "yamux")]
    if let Transport::Yamux(tls) = transport {
        let stream: super::rawsocket::BoxStream = match tls {
            Some(config) => {
                let stream = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    tokio_rustls::TlsAcceptor::from(config).accept(stream),
                )
                .await;
                let Ok(Ok(stream)) = stream else { return };
                Box::new(stream)
            }
            None => Box::new(stream),
        };
        let _ = super::yamux::serve(router, stream).await;
        return;
    }
    let websocket = matches!(transport, Transport::WebSocket);
    let session = async {
        if websocket {
            let selected = Arc::new(Mutex::new(None));
            let output = selected.clone();
            let protocols_provider = protocols;
            let callback = move |request: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
                let protocols = request
                    .headers()
                    .get("Sec-WebSocket-Protocol")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let supported = match &protocols_provider {
                    Some(provider) => provider().unwrap_or_default(),
                    None => websocket_protocols(),
                };
                let found = select_websocket_protocol(protocols, &supported).map(|protocol| {
                    let id = match protocol {
                        "wamp.2.json" => SerializerID::JSON,
                        "wamp.2.cbor" => SerializerID::CBOR,
                        _ => SerializerID::MSGPACK,
                    };
                    (protocol, id)
                });

                if let Some((protocol, id)) = found {
                    response
                        .headers_mut()
                        .insert("Sec-WebSocket-Protocol", protocol.parse().unwrap());
                    *output.lock().unwrap() = Some(id);
                    Ok(response)
                } else {
                    Err(tungstenite::http::Response::builder()
                        .status(400)
                        .body(Some("WAMP subprotocol required".into()))
                        .unwrap())
                }
            };
            let ws = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .map_err(|e| Error::new(e.to_string()))?;
            let id = selected
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| Error::new("missing serializer"))?;
            let binary = !matches!(id, SerializerID::JSON);
            let (writer, reader) = ws.split();
            Ok::<_, Error>((WebSocketPeer::new(reader, writer, binary), serializer(id)))
        } else {
            let (peer, id) = server_handshake(Box::new(stream)).await?;
            Ok((peer, serializer(id)))
        }
    };

    if let Ok(Ok((peer, serializer))) = tokio::time::timeout(std::time::Duration::from_secs(10), session).await {
        let _ = router.accept(peer, serializer).await;
    }
}

#[cfg(unix)]
pub struct UnixServer {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
    path: std::path::PathBuf,
}

#[cfg(unix)]
impl UnixServer {
    pub fn close(&self) {
        self.stop.send_replace(true);
    }

    pub async fn listen(router: Router, path: &str, websocket: bool) -> Result<Self, Error> {
        let transport = if websocket {
            Transport::WebSocket
        } else {
            Transport::RawSocket
        };
        Self::listen_transport(router, path, transport).await
    }

    #[cfg(feature = "yamux")]
    pub async fn yamux(router: Router, path: &str) -> Result<Self, Error> {
        Self::listen_transport(router, path, Transport::Yamux(None)).await
    }

    async fn listen_transport(router: Router, path: &str, transport: Transport) -> Result<Self, Error> {
        use std::os::unix::fs::FileTypeExt;

        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if !metadata.file_type().is_socket() {
                return Err(Error::new("listener path is not a Unix socket"));
            }

            if tokio::net::UnixStream::connect(path).await.is_ok() {
                return Err(Error::new("Unix socket is already listening"));
            }

            std::fs::remove_file(path).map_err(|e| Error::new(e.to_string()))?;
        }

        let listener = tokio::net::UnixListener::bind(path).map_err(|e| Error::new(e.to_string()))?;
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();

            loop {
                tokio::select! {
                    _ = stopped.wait_for(|value| *value) => break,
                    Some(_) = connections.join_next(), if !connections.is_empty() => {},
                    incoming = listener.accept() => {
                        let Ok((stream, _)) = incoming else {
                            break;
                        };
                        let router = router.clone();
                        let transport = transport.clone();
                        connections.spawn(async move {
                            serve_connection(router, stream, transport, None).await;
                        });
                    }
                }
            }

            connections.abort_all();

            while connections.join_next().await.is_some() {}
        });
        Ok(Self {
            stop,
            task,
            path: path.into(),
        })
    }
}

#[cfg(unix)]
impl Drop for UnixServer {
    fn drop(&mut self) {
        self.close();
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}
