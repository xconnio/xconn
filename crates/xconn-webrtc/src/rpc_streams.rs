//! One reliable ordered SCTP channel per RPC byte stream. SCTP supplies muxing
//! and flow control; the one-byte marker only distinguishes data from write EOF.
use crate::{Connection, DataChannel, Error};
use bytes::Bytes;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
};
use xconn_core::async_::{peer::StreamOpener, rawsocket::BoxStream};

pub(crate) const PROTOCOL: &str = "xconn.rpc.stream.v1";
const DATA: u8 = 0;
const FIN: u8 = 1;
const CHUNK: usize = 16 * 1024;
const LIMIT: usize = 64;

pub(crate) fn capacity() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(LIMIT))
}

pub(crate) fn opener(connection: Connection, session: Arc<DataChannel>, timeout: Duration) -> StreamOpener {
    let capacity = capacity();
    Arc::new(move || {
        let (connection, session, capacity) = (connection.clone(), session.clone(), capacity.clone());
        Box::pin(async move {
            let permit = capacity
                .try_acquire_owned()
                .map_err(|_| xconn_core::Error::new("too many RPC streams"))?;
            let options = crate::webrtc::data_channel::data_channel_init::RTCDataChannelInit {
                ordered: Some(true),
                protocol: Some(PROTOCOL.into()),
                ..Default::default()
            };
            let channel = tokio::select! {
                biased;
                _ = session.closed() => return Err(xconn_core::Error::new("WAMP session closed")),
                result = connection.open_channel_with_options(PROTOCOL, options) => result.map_err(|e| xconn_core::Error::new(e.to_string()))?,
            };
            let mut guard = super::wamp::ChannelGuard(Some(channel.clone()));
            channel
                .wait_open(timeout)
                .await
                .map_err(|e| xconn_core::Error::new(e.to_string()))?;
            let stream = ChannelStream::new(channel, permit).map_err(|e| xconn_core::Error::new(e.to_string()))?;
            guard.0 = None;
            Ok(Box::new(stream) as BoxStream)
        })
    })
}

pub(crate) struct ChannelStream {
    io: DuplexStream,
    channel: Arc<DataChannel>,
    task: tokio::task::JoinHandle<()>,
    executor: tokio::runtime::Handle,
    failure: watch::Receiver<Option<String>>,
    eof: Arc<AtomicBool>,
    sent: watch::Receiver<bool>,
    shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    _permit: OwnedSemaphorePermit,
}

impl ChannelStream {
    pub(crate) fn new(channel: Arc<DataChannel>, permit: OwnedSemaphorePermit) -> Result<Self, Error> {
        if !channel.reliable_ordered() {
            return Err(Error("RPC streams require reliable ordered channels".into()));
        }
        let (io, transport) = tokio::io::duplex(64 * 1024);
        let (failure, failed) = watch::channel(None);
        let (sent, finished) = watch::channel(false);
        let eof = Arc::new(AtomicBool::new(false));
        let received = eof.clone();
        let native = channel.clone();
        let task = tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(transport);
            let incoming = async {
                loop {
                    let packet = native
                        .read_data_packet()
                        .await
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    if packet.is_string || packet.data.is_empty() || packet.data.len() > CHUNK + 1 {
                        return Err(io::Error::other("invalid RPC stream packet"));
                    }
                    match packet.data[0] {
                        FIN if packet.data.len() == 1 => {
                            received.store(true, Ordering::Release);
                            writer.shutdown().await?;
                            return Ok::<(), io::Error>(());
                        }
                        DATA if packet.data.len() > 1 => writer.write_all(&packet.data[1..]).await?,
                        _ => return Err(io::Error::other("invalid RPC stream packet")),
                    }
                }
            };
            let outgoing = async {
                let mut buffer = vec![0; CHUNK + 1];
                loop {
                    let length = reader.read(&mut buffer[1..]).await?;
                    if length == 0 {
                        native
                            .send_packet(Bytes::from_static(&[FIN]))
                            .await
                            .map_err(|e| io::Error::other(e.to_string()))?;
                        sent.send_replace(true);
                        return Ok::<(), io::Error>(());
                    }
                    native
                        .send_packet(Bytes::copy_from_slice(&buffer[..length + 1]))
                        .await
                        .map_err(|e| io::Error::other(e.to_string()))?;
                }
            };
            if let Err(error) = tokio::try_join!(incoming, outgoing) {
                failure.send_replace(Some(error.to_string()));
            }
        });
        let mut shutdown = finished.clone();
        Ok(Self {
            io,
            channel,
            task,
            executor: tokio::runtime::Handle::current(),
            failure: failed,
            eof,
            sent: finished,
            shutdown: Box::pin(async move {
                let _ = shutdown.wait_for(|sent| *sent).await;
            }),
            _permit: permit,
        })
    }

    fn error(&self) -> Option<io::Error> {
        self.failure
            .borrow()
            .as_ref()
            .map(|message| io::Error::other(message.clone()))
    }
}

impl AsyncRead for ChannelStream {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, buffer);
        if matches!(result, Poll::Ready(Ok(())))
            && buffer.remaining() > 0
            && buffer.filled().len() == before
            && !self.eof.load(Ordering::Acquire)
        {
            return Poll::Ready(Err(self
                .error()
                .unwrap_or_else(|| io::Error::other("RPC stream channel closed before EOF"))));
        }
        result
    }
}

impl AsyncWrite for ChannelStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        if let Some(error) = self.error() {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(Pin::new(&mut self.io).poll_shutdown(cx))?;
        std::task::ready!(self.shutdown.as_mut().poll(cx));
        Poll::Ready(self.error().map_or(Ok(()), Err))
    }
}

impl Drop for ChannelStream {
    fn drop(&mut self) {
        self.task.abort();
        let channel = self.channel.clone();
        let graceful = self.eof.load(Ordering::Acquire) && *self.sent.borrow();
        self.executor.spawn(async move {
            if graceful {
                let _ = channel.close_gracefully(Duration::from_secs(2)).await;
            } else {
                let _ = channel.close().await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webrtc::data_channel::data_channel_init::RTCDataChannelInit;

    async fn pair(ordered: bool) -> (Connection, Connection, Arc<DataChannel>, Arc<DataChannel>) {
        pair_with_options(RTCDataChannelInit {
            ordered: Some(ordered),
            ..Default::default()
        })
        .await
    }

    async fn pair_with_options(
        mut options: RTCDataChannelInit,
    ) -> (Connection, Connection, Arc<DataChannel>, Arc<DataChannel>) {
        options.protocol = Some(PROTOCOL.into());
        let a = Connection::new(vec![]).await.unwrap();
        let b = Connection::new(vec![]).await.unwrap();
        let local = a.open_channel_with_options(PROTOCOL, options).await.unwrap();
        let offer = a.offer().await.unwrap();
        let answer = b.answer(offer).await.unwrap();
        a.set_remote(answer).await.unwrap();
        let remote = b.accept_channel().await.unwrap();
        local.wait_open(Duration::from_secs(5)).await.unwrap();
        (a, b, local, remote)
    }

    #[tokio::test]
    async fn malformed_packets_and_abrupt_close_are_errors_not_eof() {
        tokio::time::timeout(Duration::from_secs(15), async {
            for packet in [vec![], vec![FIN, 0], vec![DATA], vec![2], vec![DATA; CHUNK + 2]] {
                let (a, b, local, remote) = pair(true).await;
                let mut stream = ChannelStream::new(remote, capacity().try_acquire_owned().unwrap()).unwrap();
                local.send_packet(Bytes::from(packet)).await.unwrap();
                assert!(stream.read(&mut [0]).await.is_err());
                drop(stream);
                a.close().await.unwrap();
                b.close().await.unwrap();
            }
            let (a, b, local, remote) = pair(true).await;
            let mut stream = ChannelStream::new(remote, capacity().try_acquire_owned().unwrap()).unwrap();
            local.close().await.unwrap();
            assert!(stream.read(&mut [0]).await.is_err());
            drop(stream);
            a.close().await.unwrap();
            b.close().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn unordered_channels_are_rejected() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (a, b, _, remote) = pair(false).await;
            assert!(ChannelStream::new(remote, capacity().try_acquire_owned().unwrap()).is_err());
            a.close().await.unwrap();
            b.close().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn partially_reliable_channels_are_rejected() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for options in [
                RTCDataChannelInit {
                    ordered: Some(true),
                    max_retransmits: Some(0),
                    ..Default::default()
                },
                RTCDataChannelInit {
                    ordered: Some(true),
                    max_packet_life_time: Some(20),
                    ..Default::default()
                },
            ] {
                let (a, b, _, remote) = pair_with_options(options).await;
                assert!(ChannelStream::new(remote, capacity().try_acquire_owned().unwrap()).is_err());
                a.close().await.unwrap();
                b.close().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn dropping_a_stream_off_runtime_still_closes_its_channel() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (a, b, local, remote) = pair(true).await;
            let stream = ChannelStream::new(remote, capacity().try_acquire_owned().unwrap()).unwrap();
            std::thread::spawn(move || drop(stream)).join().unwrap();
            local.closed().await;
            a.close().await.unwrap();
            b.close().await.unwrap();
        })
        .await
        .unwrap();
    }
}
