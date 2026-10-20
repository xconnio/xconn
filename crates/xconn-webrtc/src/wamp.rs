//! WAMP integration uses the same connection/chunking implementation as raw data.
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

struct ChannelGuard(Option<Arc<DataChannel>>);

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

struct TaskGuard(Option<tokio::task::JoinHandle<()>>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

struct SubscriptionGuard {
    signaling: Session,
    id: Option<i64>,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        let signaling = self.signaling.clone();
        let Some(id) = self.id else {
            return;
        };

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = signaling.unsubscribe(id).await;
            });
        }
    }
}

async fn publish_candidates(connection: Connection, signaling: Session, topic: String, id: String) {
    while let Some(candidate) = connection.next_candidate().await {
        let complete = candidate.candidate.is_empty();
        let Ok(json) = serde_json::to_string(&candidate) else {
            continue;
        };

        if signaling
            .execute_publish(xconn_core::PublishRequest::new(topic.clone()).arg(id.clone()).arg(json))
            .await
            .is_err()
        {
            break;
        }

        if complete {
            break;
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
        let operation = async {
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
            Ok(Self {
                session: Session::new(details, peer, spec.serializer()),
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
                first = false;
                sessions.spawn(async move {
                    let result = async {
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

#[derive(Clone)]
pub struct SignalingConfig {
    pub offer_procedure: String,
    pub remote_candidates_topic: String,
    pub local_candidates_topic: String,
    pub ice_servers: Vec<crate::webrtc::ice_transport::ice_server::RTCIceServer>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct OfferResponse {
    #[serde(rename = "requestID")]
    pub request_id: String,
    pub answer: crate::Description,
}

/// Owns provider registrations, peer connections and their worker tasks.
pub struct Provider {
    raw: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<RawChannel>>,
    signaling: Session,
    registration: i64,
    subscription: i64,
    connections: Arc<std::sync::Mutex<std::collections::HashMap<String, Connection>>>,
    tasks: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    closed: tokio::sync::watch::Sender<bool>,
    ice_servers: Arc<std::sync::RwLock<Vec<crate::webrtc::ice_transport::ice_server::RTCIceServer>>>,
}

impl Provider {
    pub async fn start(
        signaling: Session,
        router: Router,
        config: SignalingConfig,
    ) -> std::result::Result<Self, Error> {
        Self::start_with_timeouts(signaling, router, config, Timeouts::default()).await
    }

    pub async fn start_with_timeouts(
        signaling: Session,
        router: Router,
        config: SignalingConfig,
        timeouts: Timeouts,
    ) -> std::result::Result<Self, Error> {
        let timeouts = timeouts.validate()?;
        let connections = Arc::new(std::sync::Mutex::new(
            std::collections::HashMap::<String, Connection>::new(),
        ));
        let candidates = connections.clone();
        let sub = signaling
            .execute_subscribe(xconn_core::async_::types::SubscribeRequest::new(
                config.remote_candidates_topic.clone(),
                move |event| {
                    let connections = candidates.clone();
                    async move {
                        if let [xconn_core::Value::Str(id), xconn_core::Value::Str(json), ..] = event.args.as_slice() {
                            let connection = connections.lock().unwrap().get(id).cloned();

                            if let (Some(connection), Ok(candidate)) = (connection, serde_json::from_str(json)) {
                                let _ = connection.add_candidate(candidate).await;
                            }
                        }
                    }
                },
            ))
            .await?;
        if let Some(error) = sub.error {
            return Err(Error::new(error.uri));
        }

        let mut subscription = SubscriptionGuard {
            signaling: signaling.clone(),
            id: Some(sub.subscription_id),
        };
        let (raw_tx, raw) = tokio::sync::mpsc::channel(64);
        let stored = connections.clone();
        let tasks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let workers = tasks.clone();
        let publisher = signaling.clone();
        let settings = config.clone();
        let ice_servers = Arc::new(std::sync::RwLock::new(config.ice_servers.clone()));
        let servers = ice_servers.clone();
        let registration = signaling
            .register_context(
                config.offer_procedure.clone(),
                std::collections::HashMap::new(),
                move |context| {
                    let connections = stored.clone();
                    let raw = raw_tx.clone();
                    let router = router.clone();
                    let config = settings.clone();
                    let publisher = publisher.clone();
                    let workers = workers.clone();
                    let ice_servers = servers.read().unwrap().clone();
                    async move {
                        let offer = match context.invocation.args.first() {
                            Some(xconn_core::Value::Str(json)) => serde_json::from_str::<crate::Description>(json),
                            _ => return xconn_core::Result::error("wamp.error.invalid_argument"),
                        };
                        let Ok(offer) = offer else {
                            return xconn_core::Result::error("wamp.error.invalid_argument");
                        };
                        let operation = async {
                            let connection = Connection::new(ice_servers)
                                .await
                                .map_err(|e| Error::new(e.to_string()))?;
                            let mut cleanup = ConnectionGuard(Some(connection.clone()));
                            let answer =
                                match tokio::time::timeout(timeouts.connect, connection.answer_trickle(offer)).await {
                                    Ok(result) => result,
                                    Err(_) => return Err(Error::new("WebRTC answer timeout")),
                                };
                            let answer = match answer {
                                Ok(answer) => answer,
                                Err(err) => {
                                    let _ = connection.close().await;
                                    return Err(Error::new(err.to_string()));
                                }
                            };
                            let request_id = uuid::Uuid::new_v4().to_string();
                            connections
                                .lock()
                                .unwrap()
                                .insert(request_id.clone(), connection.clone());
                            let serving = connection.clone();
                            let id = request_id.clone();
                            let peers = connections.clone();
                            workers.lock().unwrap().push(tokio::spawn(async move {
                                let _cleanup = ConnectionGuard(Some(serving.clone()));

                                if serving.wait_connected(timeouts.connect).await.is_ok() {
                                    let _ = serve_connection_configured(router, serving, Some(raw), timeouts).await;
                                }

                                peers.lock().unwrap().remove(&id);
                            }));
                            let trickle = connection.clone();
                            let id = request_id.clone();
                            workers.lock().unwrap().retain(|task| !task.is_finished());
                            workers.lock().unwrap().push(tokio::spawn(publish_candidates(
                                trickle,
                                publisher,
                                config.local_candidates_topic,
                                id,
                            )));
                            cleanup.0 = None;
                            serde_json::to_string(&OfferResponse { request_id, answer })
                                .map_err(|e| Error::new(e.to_string()))
                        };

                        match operation.await {
                            Ok(response) => xconn_core::Result::arg(response),
                            Err(_) => xconn_core::Result::error("wamp.error.runtime_error"),
                        }
                    }
                },
            )
            .await?;
        if let Some(error) = registration.error {
            return Err(Error::new(error.uri));
        }
        // Ownership of this subscription moves into Provider.
        subscription.id = None;
        Ok(Self {
            raw: tokio::sync::Mutex::new(raw),
            signaling,
            registration: registration.registration_id,
            subscription: sub.subscription_id,
            connections,
            tasks,
            closed: tokio::sync::watch::channel(false).0,
            ice_servers,
        })
    }

    /// Configure ICE servers for future connections without disturbing existing peers.
    pub fn update_ice_servers(&self, servers: Vec<crate::webrtc::ice_transport::ice_server::RTCIceServer>) {
        *self.ice_servers.write().unwrap() = servers;
    }

    pub async fn accept_raw(&self) -> Option<RawChannel> {
        let mut closed = self.closed.subscribe();
        let mut incoming = tokio::select! {
            biased;
            _ = closed.wait_for(|value| *value) => None,
            channel = async { self.raw.lock().await.recv().await } => channel,
        }?;
        incoming.connection_id = self
            .connections
            .lock()
            .unwrap()
            .iter()
            .find(|(_, connection)| Arc::ptr_eq(&connection.peer, &incoming.connection.peer))
            .map(|(id, _)| id.clone())
            .unwrap_or_default();
        Some(incoming)
    }

    /// Accept an explicit or legacy raw channel, preserving every packet.
    pub async fn accept_channel(&self) -> Option<Arc<DataChannel>> {
        Some(self.accept_raw().await?.into_channel().await)
    }

    pub async fn close(&self) {
        self.closed.send_replace(true);
        let _ = self.signaling.unregister(self.registration).await;
        let _ = self.signaling.unsubscribe(self.subscription).await;
        let peers: Vec<_> = self
            .connections
            .lock()
            .unwrap()
            .drain()
            .map(|(_, connection)| connection)
            .collect();
        for peer in peers {
            let _ = peer.close().await;
        }

        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.closed.send_replace(true);

        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }

        let peers: Vec<_> = self
            .connections
            .lock()
            .unwrap()
            .drain()
            .map(|(_, connection)| connection)
            .collect();
        let signaling = self.signaling.clone();
        let registration = self.registration;
        let subscription = self.subscription;

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = signaling.unregister(registration).await;
                let _ = signaling.unsubscribe(subscription).await;

                for peer in peers {
                    let _ = peer.close().await;
                }
            });
        }
    }
}

/// Connect using the existing Go-compatible WAMP offer/answer and candidate topics.
pub async fn connect(
    signaling: &Session,
    config: SignalingConfig,
    realm: &str,
    spec: Box<dyn SerializerSpec>,
    auth: Box<dyn ClientAuthenticator + Send + Sync>,
) -> std::result::Result<WebRTCSession, Error> {
    connect_with_timeouts(signaling, config, realm, spec, auth, Timeouts::default()).await
}

pub async fn connect_with_timeouts(
    signaling: &Session,
    config: SignalingConfig,
    realm: &str,
    spec: Box<dyn SerializerSpec>,
    auth: Box<dyn ClientAuthenticator + Send + Sync>,
    timeouts: Timeouts,
) -> std::result::Result<WebRTCSession, Error> {
    let timeouts = timeouts.validate()?;
    tokio::time::timeout(
        timeouts.connect,
        connect_inner(signaling, config, realm, spec, auth, timeouts),
    )
    .await
    .map_err(|_| Error::new("WebRTC connect timeout"))?
}

type IceCandidate = crate::webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;

enum RemoteCandidate {
    Candidate(String, IceCandidate),
    Activate(String, tokio::sync::oneshot::Sender<std::result::Result<(), Error>>),
}

// Both buffered and live candidates pass through this one ordered consumer.
// The activation acknowledgement includes draining the pre-answer backlog.
async fn apply_remote_candidates<F, Fut>(
    mut receiver: tokio::sync::mpsc::Receiver<RemoteCandidate>,
    mut apply: F,
) -> std::result::Result<(), Error>
where
    F: FnMut(IceCandidate) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), Error>>,
{
    let mut active: Option<String> = None;
    let mut pending = Vec::new();
    while let Some(command) = receiver.recv().await {
        match command {
            RemoteCandidate::Candidate(id, candidate) => {
                if let Some(expected) = &active {
                    if expected == &id {
                        apply(candidate).await?;
                    }
                } else if pending.len() < 256 {
                    pending.push((id, candidate));
                } else {
                    return Err(Error::new("too many pending signaling candidates"));
                }
            }
            RemoteCandidate::Activate(id, ready) => {
                let result = async {
                    if active.is_some() {
                        return Err(Error::new("signaling candidates already activated"));
                    }
                    active = Some(id.clone());
                    for (request, candidate) in pending.drain(..) {
                        if request == id {
                            apply(candidate).await?;
                        }
                    }
                    Ok(())
                }
                .await;
                let failed = result.is_err();
                let _ = ready.send(result);
                if failed {
                    return Err(Error::new("failed to activate signaling candidates"));
                }
            }
        }
    }
    Ok(())
}

async fn connect_inner(
    signaling: &Session,
    config: SignalingConfig,
    realm: &str,
    spec: Box<dyn SerializerSpec>,
    auth: Box<dyn ClientAuthenticator + Send + Sync>,
    timeouts: Timeouts,
) -> std::result::Result<WebRTCSession, Error> {
    let connection = Connection::new(config.ice_servers.clone())
        .await
        .map_err(|e| Error::new(e.to_string()))?;
    let mut cleanup = ConnectionGuard(Some(connection.clone()));
    let local = connection
        .open_channel("data")
        .await
        .map_err(|e| Error::new(e.to_string()))?;
    let (candidates, incoming_candidates) = tokio::sync::mpsc::channel(256);
    let candidate_connection = connection.clone();
    let candidate_task = TaskGuard(Some(tokio::spawn(async move {
        let sink = candidate_connection.clone();
        let result = apply_remote_candidates(incoming_candidates, move |candidate| {
            let sink = sink.clone();
            async move {
                sink.add_candidate(candidate)
                    .await
                    .map_err(|error| Error::new(error.to_string()))
            }
        })
        .await;
        if result.is_err() {
            let _ = candidate_connection.close().await;
        }
    })));
    let queued_candidates = candidates.clone();
    let remote = connection.clone();
    let sub = signaling
        .execute_subscribe(xconn_core::async_::types::SubscribeRequest::new(
            config.remote_candidates_topic.clone(),
            move |event| {
                let candidates = queued_candidates.clone();
                let remote = remote.clone();
                async move {
                    if let [xconn_core::Value::Str(id), xconn_core::Value::Str(json), ..] = event.args.as_slice() {
                        let Ok(candidate) = serde_json::from_str::<
                            crate::webrtc::ice_transport::ice_candidate::RTCIceCandidateInit,
                        >(json) else {
                            return;
                        };
                        if candidates
                            .try_send(RemoteCandidate::Candidate(id.clone(), candidate))
                            .is_err()
                        {
                            let _ = remote.close().await;
                        }
                    }
                }
            },
        ))
        .await?;
    if let Some(error) = sub.error {
        let _ = connection.close().await;
        return Err(Error::new(error.uri));
    }

    let subscription = SubscriptionGuard {
        signaling: signaling.clone(),
        id: Some(sub.subscription_id),
    };
    let mut publisher_task = TaskGuard(None);
    let operation = async {
        let offer = connection
            .offer_trickle()
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        let json = serde_json::to_string(&offer).map_err(|e| Error::new(e.to_string()))?;
        let response = signaling
            .execute_call(xconn_core::CallRequest::new(config.offer_procedure.clone()).arg(json))
            .await?;
        if let Some(error) = response.error {
            return Err(Error::new(error.uri));
        }

        let args = response.args.unwrap_or_default();
        let Some(xconn_core::Value::Str(json)) = args.first() else {
            return Err(Error::new("invalid offer response"));
        };
        let response: OfferResponse = serde_json::from_str(json).map_err(|e| Error::new(e.to_string()))?;

        if response.request_id.is_empty() {
            return Err(Error::new("empty signaling request ID"));
        }

        connection
            .set_remote(response.answer)
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        let (ready, activated) = tokio::sync::oneshot::channel();
        candidates
            .send(RemoteCandidate::Activate(response.request_id.clone(), ready))
            .await
            .map_err(|_| Error::new("signaling candidate worker closed"))?;
        activated
            .await
            .map_err(|_| Error::new("signaling candidate activation stopped"))??;

        let publisher_connection = connection.clone();
        let publisher_signaling = signaling.clone();
        publisher_task.0 = Some(tokio::spawn(publish_candidates(
            publisher_connection,
            publisher_signaling,
            config.local_candidates_topic,
            response.request_id,
        )));
        WebRTCSession::join_with_timeouts(connection.clone(), local, realm, spec, auth, timeouts).await
    };
    let result = operation.await;

    if result.is_ok() {
        cleanup.0 = None;
        let connection = connection.clone();
        // Retain signaling until the connection closes, including late candidates.
        tokio::spawn(async move {
            connection.closed().await;
            drop(publisher_task);
            drop(candidate_task);
            drop(subscription);
        });
    }

    result
}

#[cfg(test)]
mod candidate_order_tests {
    use super::*;
    use tokio::sync::{Mutex, Notify, mpsc, oneshot};

    fn candidate(name: &str) -> IceCandidate {
        IceCandidate {
            candidate: name.into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn live_candidates_and_completion_cannot_overtake_the_answer_backlog() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let (sender, receiver) = mpsc::channel(256);
            let observed = Arc::new(Mutex::new(Vec::new()));
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let (sink, started, proceed) = (observed.clone(), entered.clone(), release.clone());
            let worker = tokio::spawn(apply_remote_candidates(receiver, move |candidate| {
                let (sink, started, proceed) = (sink.clone(), started.clone(), proceed.clone());
                async move {
                    if candidate.candidate == "first" {
                        started.notify_one();
                        proceed.notified().await;
                    }
                    sink.lock().await.push(candidate.candidate);
                    Ok(())
                }
            }));
            for (id, name) in [("other", "irrelevant"), ("request", "first"), ("request", "second")] {
                assert!(
                    sender
                        .send(RemoteCandidate::Candidate(id.into(), candidate(name)))
                        .await
                        .is_ok()
                );
            }
            let (ready, activated) = oneshot::channel();
            assert!(
                sender
                    .send(RemoteCandidate::Activate("request".into(), ready))
                    .await
                    .is_ok()
            );
            entered.notified().await;
            assert!(
                sender
                    .send(RemoteCandidate::Candidate("request".into(), candidate("third")))
                    .await
                    .is_ok()
            );
            assert!(
                sender
                    .send(RemoteCandidate::Candidate("request".into(), candidate("")))
                    .await
                    .is_ok()
            );
            assert!(observed.lock().await.is_empty());
            release.notify_one();
            activated.await.unwrap().unwrap();
            drop(sender);
            worker.await.unwrap().unwrap();
            assert_eq!(*observed.lock().await, ["first", "second", "third", ""]);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn activation_errors_reach_the_waiter() {
        let (sender, receiver) = mpsc::channel(2);
        let worker = tokio::spawn(apply_remote_candidates(receiver, |_| async {
            Err(Error::new("bad candidate"))
        }));
        assert!(
            sender
                .send(RemoteCandidate::Candidate("request".into(), candidate("bad")))
                .await
                .is_ok()
        );
        let (ready, activated) = oneshot::channel();
        assert!(
            sender
                .send(RemoteCandidate::Activate("request".into(), ready))
                .await
                .is_ok()
        );
        assert_eq!(activated.await.unwrap().unwrap_err().message, "bad candidate");
        assert!(worker.await.unwrap().is_err());
    }
}
