//! WAMP integration uses the same connection/chunking implementation as raw data.
mod signaling;

pub use signaling::{OfferResponse, Provider, SignalingConfig, connect, connect_with_timeouts};

use crate::{Connection, DataChannel};
use async_trait::async_trait;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use xconn_core::{
    Error, SerializerSpec,
    async_::{
        joiner::join,
        peer::Peer,
        router::{Router, serializer},
        session::Session,
    },
    wampproto::{
        authenticators::authenticator::ClientAuthenticator,
        transports::rawsocket::{self as rs},
    },
};

// The manager owns the complete direct connection, not only its WAMP channel.
#[cfg(feature = "services")]
#[async_trait]
impl xconn_core::async_::services::peer::DirectConnection for WebRTCSession {
    fn session(&self) -> &Session {
        &self.session
    }

    async fn close(&self) {
        let _ = self.close().await;
        let _ = self.connection.close().await;
    }
}

/// Bounds for initial connection setup, WAMP session opening, and incoming channel classification.
#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    pub connect: Duration,
    pub open_session: Duration,
    pub handshake: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(45),
            open_session: Duration::from_secs(30),
            handshake: Duration::from_secs(20),
        }
    }
}

impl Timeouts {
    fn validate(self) -> std::result::Result<Self, Error> {
        if self.connect.is_zero() || self.open_session.is_zero() || self.handshake.is_zero() {
            return Err(Error::new("WebRTC timeouts must be greater than zero"));
        }

        Ok(self)
    }
}

// Async resources must also be closed if callers cancel a future.
struct ConnectionGuard(Option<Connection>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(connection) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = connection.close().await;
            });
        }
    }
}

pub(crate) struct ChannelGuard(pub(crate) Option<Arc<DataChannel>>);

impl Drop for ChannelGuard {
    fn drop(&mut self) {
        if let Some(channel) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = channel.close().await;
            });
        }
    }
}

#[derive(Debug)]
struct WampPeer {
    channel: Arc<DataChannel>,
    send_limit: usize,
}

#[async_trait]
impl Peer for WampPeer {
    fn kind(&self) -> usize {
        0
    }

    async fn read(&self) -> std::result::Result<Vec<u8>, Error> {
        self.channel.read_message().await.map_err(|e| Error::new(e.to_string()))
    }

    async fn write(&self, data: Vec<u8>) -> std::result::Result<(), Error> {
        if data.len() > self.send_limit {
            return Err(Error::new("negotiated WebRTC send limit exceeded"));
        }

        self.channel
            .write_message(&data)
            .await
            .map_err(|e| Error::new(e.to_string()))
    }

    async fn close(&self) -> std::result::Result<(), Error> {
        self.channel.close().await.map_err(|e| Error::new(e.to_string()))
    }
}

#[derive(Clone)]
pub struct WebRTCSession {
    pub session: Session,
    pub connection: Connection,
    channel: Arc<DataChannel>,
    timeouts: Timeouts,
}

impl std::ops::Deref for WebRTCSession {
    type Target = Session;

    fn deref(&self) -> &Session {
        &self.session
    }
}

impl WebRTCSession {
    pub async fn join(
        connection: Connection,
        channel: Arc<DataChannel>,
        realm: &str,
        spec: Box<dyn SerializerSpec>,
        auth: Box<dyn ClientAuthenticator + Send + Sync>,
    ) -> std::result::Result<Self, Error> {
        Self::join_with_timeouts(connection, channel, realm, spec, auth, Timeouts::default()).await
    }

    pub async fn join_with_timeouts(
        connection: Connection,
        channel: Arc<DataChannel>,
        realm: &str,
        spec: Box<dyn SerializerSpec>,
        auth: Box<dyn ClientAuthenticator + Send + Sync>,
        timeouts: Timeouts,
    ) -> std::result::Result<Self, Error> {
        let mut cleanup = ChannelGuard(Some(channel.clone()));
        let timeouts = timeouts.validate()?;
        #[cfg(feature = "rpc-streams")]
        let primary = connection.rpc_session.clone();
        let operation = async {
            #[cfg(feature = "rpc-streams")]
            let mut primary = primary.lock().await;
            channel
                .wait_open(timeouts.open_session)
                .await
                .map_err(|e| Error::new(e.to_string()))?;
            let request = rs::send_handshake(&rs::Handshake::new(spec.serializer_id(), rs::DEFAULT_MAX_MSG_SIZE))?;
            channel
                .send_packet(Bytes::from(request))
                .await
                .map_err(|e| Error::new(e.to_string()))?;
            let response = channel.read_packet().await.map_err(|e| Error::new(e.to_string()))?;
            let response = rs::receive_handshake(&response)?;

            if response.serializer_id() as u8 != spec.serializer_id() as u8 {
                return Err(Error::new("server selected different serializer"));
            }

            channel.set_receive_limit(rs::DEFAULT_MAX_MSG_SIZE).await;
            let (peer, details) = join(
                Box::new(WampPeer {
                    channel: channel.clone(),
                    send_limit: response.max_message_size(),
                }),
                realm,
                spec.serializer(),
                auth,
            )
            .await?;
            let session = Session::new(details, peer, spec.serializer());
            #[cfg(feature = "rpc-streams")]
            let session = if !*primary {
                *primary = true;
                session.with_stream_opener(crate::rpc_streams::opener(
                    connection.clone(),
                    channel.clone(),
                    timeouts.handshake,
                ))
            } else {
                session
            };
            Ok(Self {
                session,
                connection,
                channel: channel.clone(),
                timeouts,
            })
        };
        let result = tokio::time::timeout(timeouts.open_session, operation)
            .await
            .map_err(|_| Error::new("WebRTC join timeout"))?;
        if result.is_ok() {
            cleanup.0 = None;
        }

        result
    }

    pub async fn open_session(
        &self,
        realm: &str,
        spec: Box<dyn SerializerSpec>,
        auth: Box<dyn ClientAuthenticator + Send + Sync>,
    ) -> std::result::Result<Self, Error> {
        self.open_session_with_timeouts(realm, spec, auth, self.timeouts).await
    }

    pub async fn open_session_with_timeouts(
        &self,
        realm: &str,
        spec: Box<dyn SerializerSpec>,
        auth: Box<dyn ClientAuthenticator + Send + Sync>,
        timeouts: Timeouts,
    ) -> std::result::Result<Self, Error> {
        let timeouts = timeouts.validate()?;
        let channel = self
            .connection
            .open_channel("data")
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        Self::join_with_timeouts(self.connection.clone(), channel, realm, spec, auth, timeouts).await
    }

    /// Open an explicitly raw channel, ready for packet or chunked-message IO.
    pub async fn open_channel(
        &self,
        label: &str,
        options: crate::webrtc::data_channel::data_channel_init::RTCDataChannelInit,
    ) -> std::result::Result<Arc<DataChannel>, Error> {
        let channel = self
            .connection
            .open_raw_channel(label, options)
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        let mut cleanup = ChannelGuard(Some(channel.clone()));
        channel
            .wait_open(self.timeouts.open_session)
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        cleanup.0 = None;
        Ok(channel)
    }

    /// Receive a channel opened by the remote peer. Only one consumer should accept channels.
    pub async fn accept_channel(&self) -> std::result::Result<Arc<DataChannel>, Error> {
        self.connection
            .accept_channel()
            .await
            .map_err(|e| Error::new(e.to_string()))
    }

    pub async fn close(&self) -> std::result::Result<(), Error> {
        let result = self.session.leave().await;
        let _ = self.channel.close().await;
        result
    }
}

pub async fn accept_channel(router: Router, channel: Arc<DataChannel>) -> std::result::Result<(), Error> {
    accept_channel_with_timeout(router, channel, Timeouts::default().handshake).await
}

pub async fn accept_channel_with_timeout(
    router: Router,
    channel: Arc<DataChannel>,
    timeout: Duration,
) -> std::result::Result<(), Error> {
    let _cleanup = ChannelGuard(Some(channel.clone()));
    let request = tokio::time::timeout(timeout, channel.read_packet())
        .await
        .map_err(|_| Error::new("WebRTC handshake timeout"))?
        .map_err(|e| Error::new(e.to_string()))?;
    accept_handshaken(router, channel, request, None).await
}

async fn accept_handshaken(
    router: Router,
    channel: Arc<DataChannel>,
    request: Bytes,
    authenticated: Option<tokio::sync::watch::Sender<Option<xconn_core::wampproto::types::SessionDetails>>>,
) -> std::result::Result<(), Error> {
    let hs = rs::receive_handshake(&request)?;
    channel
        .send_packet(Bytes::from(rs::send_handshake(&rs::Handshake::new(
            hs.serializer_id(),
            rs::DEFAULT_MAX_MSG_SIZE,
        ))?))
        .await
        .map_err(|e| Error::new(e.to_string()))?;
    channel.set_receive_limit(rs::DEFAULT_MAX_MSG_SIZE).await;
    let peer = Box::new(WampPeer {
        channel,
        send_limit: hs.max_message_size(),
    });
    let serializer = serializer(hs.serializer_id());
    if let Some(authenticated) = authenticated {
        router.accept_notified(peer, serializer, authenticated).await
    } else {
        router.accept(peer, serializer).await
    }
}

/// Raw channel plus any packet consumed while identifying a legacy channel.
/// Explicit raw channels need no first packet, but require an authenticated WAMP session.
pub struct RawChannel {
    /// Signaling request ID, populated for channels accepted through Provider.
    pub connection_id: String,
    /// Shared connection, usable for opening additional channels back to this peer.
    pub connection: Connection,
    pub channel: Arc<DataChannel>,
    pub first_packet: Option<Bytes>,
    pub first_is_string: bool,
}

impl RawChannel {
    /// Restore any classification packet, so callers can use ordinary channel reads.
    pub async fn into_channel(self) -> Arc<DataChannel> {
        if let Some(packet) = self.first_packet {
            self.channel
                .unread_data_packet(crate::Packet {
                    data: packet,
                    is_string: self.first_is_string,
                })
                .await;
        }

        self.channel
    }
}

pub async fn serve_connection(router: Router, connection: Connection) -> std::result::Result<(), Error> {
    serve_connection_with_raw(router, connection, None).await
}

pub async fn serve_connection_with_raw(
    router: Router,
    connection: Connection,
    raw: Option<tokio::sync::mpsc::Sender<RawChannel>>,
) -> std::result::Result<(), Error> {
    serve_connection_configured(router, connection, raw, Timeouts::default()).await
}

async fn serve_connection_configured(
    router: Router,
    connection: Connection,
    raw: Option<tokio::sync::mpsc::Sender<RawChannel>>,
    timeouts: Timeouts,
) -> std::result::Result<(), Error> {
    let _cleanup = ConnectionGuard(Some(connection.clone()));
    let mut sessions = tokio::task::JoinSet::new();
    let mut first = true;
    #[cfg(feature = "rpc-streams")]
    let stream_capacity = crate::rpc_streams::capacity();
    let (authenticated, _) = tokio::sync::watch::channel(None);
    let (authentication_failed, _) = tokio::sync::watch::channel(false);

    loop {
        tokio::select! {
            channel = connection.accept_channel() => {
                let channel = channel.map_err(|e| Error::new(e.to_string()))?;
                let router = router.clone();
                let peer_connection = connection.clone();
                let raw = raw.clone();
                let authenticated = authenticated.clone();
                let authentication_failed = authentication_failed.clone();
                let is_first = first;
                #[cfg(feature = "rpc-streams")]
                let stream_capacity = stream_capacity.clone();
                first = false;
                sessions.spawn(async move {
                    let result = async {
                        #[cfg(feature = "rpc-streams")]
                        if channel.protocol() == crate::rpc_streams::PROTOCOL {
                            wait_raw_authentication(&authenticated, &authentication_failed, timeouts.handshake).await?;
                            let identity = authenticated.borrow().clone()
                                .ok_or_else(|| Error::new("RPC stream requires authentication"))?;
                            let opener = router.rpc_stream_opener(&identity)?;
                            let permit = stream_capacity.try_acquire_owned()
                                .map_err(|_| Error::new("too many RPC streams"))?;
                            let mut incoming = crate::rpc_streams::ChannelStream::new(channel.clone(), permit)
                                .map_err(|error| Error::new(error.to_string()))?;
                            let mut outgoing = opener().await?;
                            return tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await
                                .map(|_| ()).map_err(|error| Error::new(error.to_string()));
                        }

                        if channel.protocol() == crate::RAW_CHANNEL_PROTOCOL {
                            wait_raw_authentication(&authenticated, &authentication_failed, timeouts.handshake).await?;
                            if let Some(raw) = raw {
                                raw.send(RawChannel { connection_id: String::new(), connection: peer_connection.clone(), channel: channel.clone(), first_packet: None, first_is_string: false }).await
                                    .map_err(|_| Error::new("raw channel receiver closed"))?;
                            } else {
                                channel.close().await.map_err(|e| Error::new(e.to_string()))?;
                            }

                            return Ok(());
                        }

                        let first = tokio::time::timeout(timeouts.handshake, channel.read_data_packet())
                            .await
                            .map_err(|_| Error::new("channel classification timeout"))?
                            .map_err(|e| Error::new(e.to_string()))?;
                        let packet = first.data;
                        if packet.first() == Some(&rs::MAGIC) && packet.len() == 4 {
                            return accept_handshaken(router, channel.clone(), packet, Some(authenticated.clone())).await;
                        }

                        if is_first {
                            let id = match channel.protocol() {
                                "wamp.2.json" => Some(rs::SerializerID::JSON),
                                "wamp.2.cbor" => Some(rs::SerializerID::CBOR),
                                "wamp.2.msgpack" => Some(rs::SerializerID::MSGPACK),
                                _ => None,
                            };

                            if let Some(id) = id {
                                channel.unread_packet(packet).await;
                                return router
                                    .accept_notified(
                                        Box::new(WampPeer {
                                            channel: channel.clone(),
                                            send_limit: crate::DEFAULT_MAX_MESSAGE,
                                        }),
                                        serializer(id),
                                        authenticated.clone(),
                                )
                                    .await;
                            }
                        }

                        wait_raw_authentication(&authenticated, &authentication_failed, timeouts.handshake).await?;
                        if let Some(raw) = raw {
                            raw.send(RawChannel {
                                connection_id: String::new(),
                                connection: peer_connection,
                                channel: channel.clone(),
                                first_packet: Some(packet),
                                first_is_string: first.is_string,
                            })
                                .await
                                .map_err(|_| Error::new("raw channel receiver closed"))?;
                        } else {
                            let _ = channel.close().await;
                        }

                        Ok::<(), Error>(())
                    }
                        .await;
                    if result.is_err() {
                        if authenticated.borrow().is_none() { authentication_failed.send_replace(true); }
                        let _ = channel.close().await;
                    }
                });
            }
            Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }
}

async fn wait_raw_authentication(
    authenticated: &tokio::sync::watch::Sender<Option<xconn_core::wampproto::types::SessionDetails>>,
    failed: &tokio::sync::watch::Sender<bool>,
    timeout: Duration,
) -> std::result::Result<(), Error> {
    let mut authenticated = authenticated.subscribe();
    let mut failed = failed.subscribe();
    tokio::time::timeout(timeout, async {
        tokio::select! {
            biased;
            result = authenticated.wait_for(|details| details.is_some()) => {
                result.map(|_| ()).map_err(|_| Error::new("connection closed before authentication"))
            }
            _ = failed.wait_for(|failed| *failed) => Err(Error::new("raw channel requires WAMP authentication")),
        }
    })
    .await
    .map_err(|_| Error::new("raw channel authentication timeout"))?
}
