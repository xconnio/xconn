use super::{
    rawsocket::{BoxStream, server_handshake_header},
    router::{Router, serializer},
};
use crate::Error;
use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    sync::{mpsc, watch},
};
use wampproto::types::SessionDetails;

// Only QUIC and WebTransport expose unassociated raw streams.
#[cfg_attr(not(any(feature = "quic", feature = "webtransport")), allow(dead_code))]
pub struct AuthenticatedStream {
    pub stream: BoxStream,
    pub session: SessionDetails,
}

pub async fn serve_stream(
    router: Router,
    stream: BoxStream,
    authenticated: watch::Sender<Option<SessionDetails>>,
    raw: mpsc::Sender<AuthenticatedStream>,
) -> Result<(), Error> {
    let mut stream = BufReader::new(stream);
    let first = tokio::time::timeout(Duration::from_secs(10), stream.fill_buf())
        .await
        .map_err(|_| Error::new("stream classification timeout"))?
        .map_err(|e| Error::new(e.to_string()))?
        .first()
        .copied()
        .ok_or_else(|| Error::new("empty stream"))?;
    if first == wampproto::transports::rawsocket::MAGIC {
        let mut header = [0; 4];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut header))
            .await
            .map_err(|_| Error::new("stream handshake timeout"))?
            .map_err(|e| Error::new(e.to_string()))?;
        #[cfg(feature = "rpc-streams")]
        if header == super::rpc_streams::HEADER {
            let mut auth = authenticated.subscribe();
            let details = tokio::time::timeout(Duration::from_secs(5), auth.wait_for(|v| v.is_some()))
                .await
                .map_err(|_| Error::new("stream authentication timeout"))?
                .map_err(|_| Error::new("connection closed"))?
                .clone()
                .unwrap();
            return router.relay_rpc_stream(&details, Box::new(stream)).await;
        }
        let (peer, id) = tokio::time::timeout(
            Duration::from_secs(10),
            server_handshake_header(Box::new(stream), header),
        )
        .await
        .map_err(|_| Error::new("stream handshake timeout"))??;
        router.accept_notified(peer, serializer(id), authenticated).await
    } else {
        let mut auth = authenticated.subscribe();
        let details = tokio::time::timeout(Duration::from_secs(5), auth.wait_for(|v| v.is_some()))
            .await
            .map_err(|_| Error::new("raw stream authentication timeout"))?
            .map_err(|_| Error::new("connection closed"))?
            .clone()
            .unwrap();
        let incoming = AuthenticatedStream {
            stream: Box::new(stream),
            session: details,
        };
        raw.send(incoming)
            .await
            .map_err(|_| Error::new("raw stream receiver closed"))
    }
}
