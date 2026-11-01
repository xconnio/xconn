//! WAMP over yamux on Unix sockets, TCP, or TLS.
use super::{
    joiner::join,
    peer::Peer,
    rawsocket::{BoxStream, client_handshake, server_handshake},
    router::{Router, serializer},
    session::Session,
};
use crate::{Error, SerializerSpec, TransportType};
use futures_util::future::poll_fn;
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use wampproto::authenticators::authenticator::ClientAuthenticator;

// Yamux requires the connection driver to keep polling, independently of any
// application stream's reads and writes. No mutex guard is held across an await.
type Socket = ::yamux::Connection<tokio_util::compat::Compat<BoxStream>>;
type Shared = Arc<Mutex<Option<Socket>>>;

struct Driver(tokio::task::JoinHandle<()>);

impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Owner(Shared);

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.lock().unwrap().take();
    }
}

#[derive(Clone)]
struct Connection {
    socket: Shared,
    progress: Arc<Notify>,
    incoming: Arc<AsyncMutex<mpsc::Receiver<BoxStream>>>,
    driver: Arc<Driver>,
}

impl Connection {
    fn new(stream: BoxStream, mode: ::yamux::Mode) -> Self {
        let mut config = ::yamux::Config::default();
        config.set_max_num_streams(64);
        config.set_max_connection_receive_window(Some(16 * 1024 * 1024));
        let socket = Arc::new(Mutex::new(Some(::yamux::Connection::new(
            stream.compat(),
            config,
            mode,
        ))));
        let (sender, incoming) = mpsc::channel(64);
        let owner = Owner(socket.clone());
        let progress = Arc::new(Notify::new());
        let wake = progress.clone();
        let driver = tokio::spawn(async move {
            let _owner = owner;
            loop {
                let incoming = tokio::select! {
                    _ = wake.notified() => continue,
                    incoming = poll_fn(|cx| match _owner.0.lock().unwrap().as_mut() {
                        Some(connection) => connection.poll_next_inbound(cx),
                        None => std::task::Poll::Ready(None),
                    }) => incoming,
                };
                match incoming {
                    Some(Ok(stream)) => {
                        if sender.try_send(Box::new(stream.compat()) as BoxStream).is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        });
        Self {
            socket,
            progress,
            incoming: Arc::new(AsyncMutex::new(incoming)),
            driver: Arc::new(Driver(driver)),
        }
    }

    async fn open(&self) -> Result<BoxStream, Error> {
        let stream = poll_fn(|cx| match self.socket.lock().unwrap().as_mut() {
            Some(connection) => connection.poll_new_outbound(cx),
            None => std::task::Poll::Ready(Err(::yamux::ConnectionError::Closed)),
        })
        .await
        .map_err(|e| Error::new(e.to_string()))?;
        // Opening a stream mutates yamux outside the driver poll. Wake it so
        // the newly added command receiver is polled even on an idle socket.
        self.progress.notify_one();
        Ok(Box::new(stream.compat()))
    }

    async fn accept(&self) -> Option<BoxStream> {
        self.incoming.lock().await.recv().await
    }

    fn close(&self) {
        self.socket.lock().unwrap().take();
        self.driver.0.abort();
    }
}

struct SessionPeer {
    peer: Box<dyn Peer>,
    connection: Connection,
}

impl std::fmt::Debug for SessionPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("YamuxSessionPeer")
    }
}

#[async_trait::async_trait]
impl Peer for SessionPeer {
    fn kind(&self) -> TransportType {
        self.peer.kind()
    }

    async fn read(&self) -> Result<Vec<u8>, Error> {
        self.peer.read().await
    }

    async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
        self.peer.write(data).await
    }

    async fn ping(&self, timeout: Option<std::time::Duration>) -> Result<std::time::Duration, Error> {
        self.peer.ping(timeout).await
    }

    async fn close(&self) -> Result<(), Error> {
        let result = self.peer.close().await;
        self.connection.close();
        result
    }
}

/// Join WAMP over an ordered byte transport, including a TLS or Unix stream.
pub async fn connect(
    stream: BoxStream,
    realm: &str,
    spec: Box<dyn SerializerSpec>,
    auth: Box<dyn ClientAuthenticator + Send + Sync>,
) -> Result<Session, Error> {
    let connection = Connection::new(stream, ::yamux::Mode::Client);
    let peer = client_handshake(connection.open().await?, spec.serializer_id()).await?;
    let (peer, details) = join(peer, realm, spec.serializer(), auth).await?;
    let peer = Box::new(SessionPeer {
        peer,
        connection: connection.clone(),
    });
    Ok(Session::new(details, peer, spec.serializer()))
}

/// Serve a single yamux connection. The listener owns this future and all
/// logical-stream tasks, including their cancellation on listener shutdown.
pub async fn serve(router: Router, stream: BoxStream) -> Result<(), Error> {
    let connection = Connection::new(stream, ::yamux::Mode::Server);
    let mut streams = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            incoming = connection.accept() => {
                let Some(stream) = incoming else { break };
                let router = router.clone();
                streams.spawn(async move {
                    let handshake = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        server_handshake(stream),
                    ).await;
                    if let Ok(Ok((peer, id))) = handshake {
                        let _ = router.accept(peer, serializer(id)).await;
                    }
                });
            }
            Some(_) = streams.join_next(), if !streams.is_empty() => {}
        }
    }
    connection.close();
    streams.abort_all();
    while streams.join_next().await.is_some() {}
    Ok(())
}

pub(crate) async fn connect_uri(
    uri: &str,
    realm: &str,
    spec: Box<dyn SerializerSpec>,
    auth: Box<dyn ClientAuthenticator + Send + Sync>,
) -> Result<Session, Error> {
    let url = url::Url::parse(uri).map_err(|e| Error::new(e.to_string()))?;
    let stream: BoxStream = match url.scheme() {
        #[cfg(unix)]
        "unix+yamux" => Box::new(
            tokio::net::UnixStream::connect(url.path())
                .await
                .map_err(|e| Error::new(e.to_string()))?,
        ),
        "yamux" | "yamuxs" => {
            let host = url.host_str().ok_or_else(|| Error::new("missing host"))?;
            let port = url.port().ok_or_else(|| Error::new("missing port"))?;
            let stream = tokio::net::TcpStream::connect((host, port))
                .await
                .map_err(|e| Error::new(e.to_string()))?;
            stream.set_nodelay(true).map_err(|e| Error::new(e.to_string()))?;
            if url.scheme() == "yamuxs" {
                let mut roots = rustls::RootCertStore::empty();
                roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
                let tls =
                    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                        .with_safe_default_protocol_versions()
                        .map_err(|e| Error::new(e.to_string()))?
                        .with_root_certificates(roots)
                        .with_no_client_auth();
                let name =
                    rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| Error::new(e.to_string()))?;
                Box::new(
                    tokio_rustls::TlsConnector::from(Arc::new(tls))
                        .connect(name, stream)
                        .await
                        .map_err(|e| Error::new(e.to_string()))?,
                )
            } else {
                Box::new(stream)
            }
        }
        _ => return Err(Error::new("unsupported yamux URI")),
    };
    connect(stream, realm, spec, auth).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_blocked_stream_does_not_stop_another_stream_and_half_close_preserves_reply() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (a, b) = tokio::io::duplex(4096);
            let client = Connection::new(Box::new(a), ::yamux::Mode::Client);
            let server = Connection::new(Box::new(b), ::yamux::Mode::Server);
            let mut outgoing = client.open().await.unwrap();
            outgoing.write_all(b"x").await.unwrap();
            let mut incoming = server.accept().await.unwrap();
            let sending = tokio::spawn(async move {
                outgoing.write_all(&vec![42; 2 * 1024 * 1024]).await.unwrap();
                outgoing.shutdown().await.unwrap();
                let mut reply = Vec::new();
                outgoing.read_to_end(&mut reply).await.unwrap();
                reply
            });
            // Leave the first stream unread, exhausting its receive window.
            let mut control = client.open().await.unwrap();
            control.write_all(b"ping").await.unwrap();
            let mut remote_control = server.accept().await.unwrap();
            let mut ping = [0; 4];
            remote_control.read_exact(&mut ping).await.unwrap();
            assert_eq!(&ping, b"ping");
            remote_control.write_all(b"pong").await.unwrap();
            control.read_exact(&mut ping).await.unwrap();
            assert_eq!(&ping, b"pong");
            let mut payload = Vec::new();
            incoming.read_to_end(&mut payload).await.unwrap();
            assert_eq!(payload.len(), 1 + 2 * 1024 * 1024);
            assert_eq!(payload[0], b'x');
            assert!(payload[1..].iter().all(|byte| *byte == 42));
            incoming.write_all(b"complete").await.unwrap();
            incoming.shutdown().await.unwrap();
            assert_eq!(sending.await.unwrap(), b"complete");
            client.close();
            server.close();
        })
        .await
        .unwrap();
    }
}
