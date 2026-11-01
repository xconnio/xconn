use super::ping::PingState;
use crate::async_::peer::Peer;
use crate::{Error, SerializerSpec, TRANSPORT_RAW_SOCKET, TransportType};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex, watch};
use wampproto::transports::rawsocket::{self as rs, SerializerID};

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub type BoxStream = Box<dyn Stream>;

pub struct RawSocketPeer {
    reader: Mutex<ReadHalf<BoxStream>>,
    writer: Mutex<WriteHalf<BoxStream>>,
    receive_limit: usize,
    send_limit: usize,
    closed: watch::Sender<bool>,
    pings: Arc<PingState>,
}

impl RawSocketPeer {
    pub fn from_stream(stream: BoxStream, receive_limit: usize, send_limit: usize) -> Box<dyn Peer> {
        let (reader, writer) = tokio::io::split(stream);
        Box::new(Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            receive_limit,
            send_limit,
            closed: watch::channel(false).0,
            pings: Arc::new(PingState::default()),
        })
    }
}

#[async_trait]
impl Peer for RawSocketPeer {
    fn kind(&self) -> TransportType {
        TRANSPORT_RAW_SOCKET
    }

    async fn ping(&self, timeout: Option<Duration>) -> Result<Duration, Error> {
        if *self.closed.borrow() {
            return Err(Error::new("connection closed"));
        }

        let ping = self.pings.start();
        {
            let mut writer = self.writer.lock().await;
            writer
                .write_all(&rs::send_message_header(&rs::MessageHeader::new(
                    rs::Message::Ping,
                    ping.payload.len(),
                )))
                .await
                .map_err(|e| Error::new(e.to_string()))?;
            writer
                .write_all(&ping.payload)
                .await
                .map_err(|e| Error::new(e.to_string()))?;
        }

        ping.wait(timeout).await
    }

    async fn read(&self) -> Result<Vec<u8>, Error> {
        let mut reader = self.reader.lock().await;
        let mut closed = self.closed.subscribe();

        loop {
            let read = async {
                let mut raw = [0; 4];
                reader
                    .read_exact(&mut raw)
                    .await
                    .map_err(|e| Error::new(e.to_string()))?;
                let header = rs::receive_message_header(&raw).map_err(Error::from)?;

                if header.length() > self.receive_limit {
                    return Err(Error::new("rawsocket receive limit exceeded"));
                }

                let mut payload = vec![0; header.length()];
                reader
                    .read_exact(&mut payload)
                    .await
                    .map_err(|e| Error::new(e.to_string()))?;
                Ok::<_, Error>((header, payload))
            };
            let (header, payload) = tokio::select! {
                result = read => result?,
                _ = closed.wait_for(|v| *v) => {
                    return Err(Error::new("connection closed"));
                }
            };

            match header.kind() {
                rs::Message::Wamp => return Ok(payload),
                rs::Message::Ping => {
                    let mut writer = self.writer.lock().await;
                    let header = rs::send_message_header(&rs::MessageHeader::new(rs::Message::Pong, payload.len()));
                    writer.write_all(&header).await.map_err(|e| Error::new(e.to_string()))?;
                    writer
                        .write_all(&payload)
                        .await
                        .map_err(|e| Error::new(e.to_string()))?;
                }
                rs::Message::Pong => self.pings.pong(&payload),
            }
        }
    }

    async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
        if *self.closed.borrow() {
            return Err(Error::new("connection closed"));
        }

        if data.len() > self.send_limit.min(rs::MAX_MESSAGE_LENGTH) {
            return Err(Error::new("rawsocket send limit exceeded"));
        }

        let mut writer = self.writer.lock().await;
        writer
            .write_all(&rs::send_message_header(&rs::MessageHeader::new(
                rs::Message::Wamp,
                data.len(),
            )))
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        writer.write_all(&data).await.map_err(|e| Error::new(e.to_string()))
    }

    async fn close(&self) -> Result<(), Error> {
        self.closed.send_replace(true);
        self.pings.close();
        self.writer
            .lock()
            .await
            .shutdown()
            .await
            .map_err(|e| Error::new(e.to_string()))
    }
}

pub async fn client_handshake(mut stream: BoxStream, serializer: SerializerID) -> Result<Box<dyn Peer>, Error> {
    let raw = rs::send_handshake(&rs::Handshake::new(serializer, rs::DEFAULT_MAX_MSG_SIZE))?;
    stream.write_all(&raw).await.map_err(|e| Error::new(e.to_string()))?;
    let mut raw = [0; 4];
    stream
        .read_exact(&mut raw)
        .await
        .map_err(|e| Error::new(e.to_string()))?;
    let hs = rs::receive_handshake(&raw)?;

    if hs.serializer_id() as u8 != serializer as u8 {
        return Err(Error::new("server selected a different serializer"));
    }

    Ok(RawSocketPeer::from_stream(
        stream,
        rs::DEFAULT_MAX_MSG_SIZE,
        hs.max_message_size(),
    ))
}

pub async fn server_handshake(mut stream: BoxStream) -> Result<(Box<dyn Peer>, SerializerID), Error> {
    let mut raw = [0; 4];
    stream
        .read_exact(&mut raw)
        .await
        .map_err(|e| Error::new(e.to_string()))?;
    server_handshake_header(stream, raw).await
}

pub(crate) async fn server_handshake_header(
    mut stream: BoxStream,
    raw: [u8; 4],
) -> Result<(Box<dyn Peer>, SerializerID), Error> {
    let hs = rs::receive_handshake(&raw)?;
    let response = rs::send_handshake(&rs::Handshake::new(hs.serializer_id(), rs::DEFAULT_MAX_MSG_SIZE))?;
    stream
        .write_all(&response)
        .await
        .map_err(|e| Error::new(e.to_string()))?;
    Ok((
        RawSocketPeer::from_stream(stream, rs::DEFAULT_MAX_MSG_SIZE, hs.max_message_size()),
        hs.serializer_id(),
    ))
}

pub async fn connect_rawsocket(uri: &str, serializer: Box<dyn SerializerSpec>) -> Result<Box<dyn Peer>, Error> {
    let url = url::Url::parse(uri).map_err(|e| Error::new(e.to_string()))?;
    let stream: BoxStream = match url.scheme() {
        "rs" | "tcp" => Box::new(
            tokio::net::TcpStream::connect((
                url.host_str().ok_or_else(|| Error::new("missing host"))?,
                url.port().ok_or_else(|| Error::new("missing port"))?,
            ))
            .await
            .map_err(|e| Error::new(e.to_string()))?,
        ),
        #[cfg(unix)]
        "unix" | "unix+rs" => Box::new(
            tokio::net::UnixStream::connect(url.path())
                .await
                .map_err(|e| Error::new(e.to_string()))?,
        ),
        _ => return Err(Error::new("unsupported RawSocket scheme")),
    };
    client_handshake(stream, serializer.serializer_id()).await
}

impl std::fmt::Debug for RawSocketPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RawSocketPeer")
    }
}
