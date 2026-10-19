use super::ping::PingState;
use crate::async_::peer::Peer;
use crate::{Error, TRANSPORT_WEB_SOCKET, TransportType};
use async_trait::async_trait;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, watch},
};
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message;

pub struct WebSocketPeer<S> {
    reader: Mutex<SplitStream<WebSocketStream<S>>>,
    writer: Mutex<SplitSink<WebSocketStream<S>, Message>>,
    binary: bool,
    closed: watch::Sender<bool>,
    pings: Arc<PingState>,
}

impl<S> std::fmt::Debug for WebSocketPeer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WebSocketPeer")
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> WebSocketPeer<S> {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        reader: SplitStream<WebSocketStream<S>>,
        writer: SplitSink<WebSocketStream<S>, Message>,
        binary: bool,
    ) -> Box<dyn Peer> {
        Box::new(Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            binary,
            closed: watch::channel(false).0,
            pings: Arc::new(PingState::default()),
        })
    }
}

#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Peer for WebSocketPeer<S> {
    fn kind(&self) -> TransportType {
        TRANSPORT_WEB_SOCKET
    }

    async fn ping(&self, timeout: Option<Duration>) -> Result<Duration, Error> {
        if *self.closed.borrow() {
            return Err(Error::new("connection closed"));
        }

        let ping = self.pings.start();
        self.writer
            .lock()
            .await
            .send(Message::Ping(ping.payload.to_vec().into()))
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        ping.wait(timeout).await
    }

    async fn read(&self) -> Result<Vec<u8>, Error> {
        let mut reader = self.reader.lock().await;
        let mut closed = self.closed.subscribe();

        loop {
            let msg = tokio::select! {
                _ = closed.wait_for(|v| *v) => {
                    return Err(Error::new("connection closed"));
                }
                msg = reader.next() => msg
                    .ok_or_else(|| Error::new("websocket closed"))?
                    .map_err(|e| Error::new(e.to_string()))?,
            };

            match msg {
                Message::Text(v) if !self.binary => return Ok(v.as_bytes().to_vec()),
                Message::Binary(v) if self.binary => return Ok(v.to_vec()),
                Message::Ping(_) => {
                    self.writer
                        .lock()
                        .await
                        .flush()
                        .await
                        .map_err(|e| Error::new(e.to_string()))?;
                }
                Message::Pong(payload) => self.pings.pong(&payload),
                Message::Close(_) => return Err(Error::new("websocket closed")),
                _ => return Err(Error::new("unexpected websocket message type")),
            }
        }
    }

    async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
        if *self.closed.borrow() {
            return Err(Error::new("connection closed"));
        }

        let msg = if self.binary {
            Message::Binary(data.into())
        } else {
            Message::Text(String::from_utf8(data).map_err(|e| Error::new(e.to_string()))?.into())
        };
        self.writer
            .lock()
            .await
            .send(msg)
            .await
            .map_err(|e| Error::new(e.to_string()))
    }

    async fn close(&self) -> Result<(), Error> {
        self.closed.send_replace(true);
        self.pings.close();
        self.writer
            .lock()
            .await
            .close()
            .await
            .map_err(|e| Error::new(e.to_string()))
    }
}
