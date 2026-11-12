use crate::{Error, TransportType};
use async_trait::async_trait;
use std::fmt::Debug;
#[cfg(feature = "rpc-streams")]
use std::{pin::Pin, sync::Arc};
use tokio::sync::{Mutex, mpsc, watch};

/// Opens a byte stream on a session's authenticated transport.
#[cfg(feature = "rpc-streams")]
pub type StreamOpener =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<super::rawsocket::BoxStream, Error>> + Send>> + Send + Sync>;

#[async_trait]
pub trait Peer: Debug + Send + Sync {
    fn kind(&self) -> TransportType;

    /// Optional RPC stream carrier bound to this peer's authenticated session.
    #[cfg(feature = "rpc-streams")]
    fn stream_opener(&self) -> Option<StreamOpener> {
        None
    }

    async fn ping(&self, _timeout: Option<std::time::Duration>) -> Result<std::time::Duration, Error> {
        Err(Error::new("ping is not supported by this transport"))
    }

    async fn read(&self) -> Result<Vec<u8>, Error>;

    async fn write(&self, data: Vec<u8>) -> Result<(), Error>;

    fn try_write(&self, _data: Vec<u8>) -> Result<bool, Error> {
        Ok(false)
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemoryPeer {
    incoming: Mutex<mpsc::Receiver<Vec<u8>>>,
    outgoing: mpsc::Sender<Vec<u8>>,
    closed: watch::Sender<bool>,
    remote_closed: watch::Receiver<bool>,
}

impl MemoryPeer {
    pub fn pair(capacity: usize) -> (Box<dyn Peer>, Box<dyn Peer>) {
        let (at, ar) = mpsc::channel(capacity.max(1));
        let (bt, br) = mpsc::channel(capacity.max(1));
        let (ac, aw) = watch::channel(false);
        let (bc, bw) = watch::channel(false);
        (
            Box::new(Self {
                incoming: Mutex::new(ar),
                outgoing: bt,
                closed: ac,
                remote_closed: bw,
            }),
            Box::new(Self {
                incoming: Mutex::new(br),
                outgoing: at,
                closed: bc,
                remote_closed: aw,
            }),
        )
    }
}

#[async_trait]
impl Peer for MemoryPeer {
    fn kind(&self) -> TransportType {
        0
    }

    async fn read(&self) -> Result<Vec<u8>, Error> {
        let mut local = self.closed.subscribe();
        let mut remote = self.remote_closed.clone();
        let mut incoming = self.incoming.lock().await;
        tokio::select! {
            biased;
            _ = local.wait_for(|v| *v) => Err(Error::new("connection closed")),
            data = incoming.recv() => data.ok_or_else(|| Error::new("connection closed")),
            _ = remote.wait_for(|v| *v) => Err(Error::new("connection closed")),
        }
    }

    fn try_write(&self, data: Vec<u8>) -> Result<bool, Error> {
        if *self.closed.borrow() || *self.remote_closed.borrow() {
            return Err(Error::new("connection closed"));
        }

        match self.outgoing.try_send(data) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::new("connection closed")),
        }
    }

    async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
        if *self.closed.borrow() || *self.remote_closed.borrow() {
            return Err(Error::new("connection closed"));
        }

        let mut local = self.closed.subscribe();
        let mut remote = self.remote_closed.clone();
        tokio::select! {
            result = self.outgoing.send(data) => result.map_err(|_| Error::new("connection closed")),
            _ = local.wait_for(|v| *v) => Err(Error::new("connection closed")),
            _ = remote.wait_for(|v| *v) => Err(Error::new("connection closed")),
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.closed.send_replace(true);
        Ok(())
    }
}

// Preserve an authenticated in-process peer's byte-stream capability when a
// binding transfers that peer into a Session (including BaseSession adapters).
#[cfg(feature = "rpc-streams")]
pub(crate) fn with_streams(peer: Box<dyn Peer>, opener: StreamOpener) -> Box<dyn Peer> {
    Box::new(StreamPeer { peer, opener })
}

#[cfg(feature = "rpc-streams")]
struct StreamPeer {
    peer: Box<dyn Peer>,
    opener: StreamOpener,
}

#[cfg(feature = "rpc-streams")]
impl std::fmt::Debug for StreamPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.peer.fmt(f)
    }
}

#[cfg(feature = "rpc-streams")]
#[async_trait]
impl Peer for StreamPeer {
    fn kind(&self) -> TransportType {
        self.peer.kind()
    }

    fn stream_opener(&self) -> Option<StreamOpener> {
        Some(self.opener.clone())
    }

    async fn read(&self) -> Result<Vec<u8>, Error> {
        self.peer.read().await
    }

    async fn write(&self, data: Vec<u8>) -> Result<(), Error> {
        self.peer.write(data).await
    }

    fn try_write(&self, data: Vec<u8>) -> Result<bool, Error> {
        self.peer.try_write(data)
    }

    async fn ping(&self, timeout: Option<std::time::Duration>) -> Result<std::time::Duration, Error> {
        self.peer.ping(timeout).await
    }

    async fn close(&self) -> Result<(), Error> {
        self.peer.close().await
    }
}
