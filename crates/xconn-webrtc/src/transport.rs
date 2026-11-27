use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc, watch};
pub use webrtc;
use webrtc::{
    data_channel::{DataChannel as NativeChannel, DataChannelEvent, RTCDataChannelInit, RTCDataChannelState},
    peer_connection::{
        PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
        RTCIceCandidateInit, RTCIceGatheringState, RTCIceServer, RTCPeerConnectionIceEvent, RTCPeerConnectionState,
        RTCSessionDescription,
    },
};

/// Explicit raw channels can be accepted before either peer sends a packet.
pub use xconn_framing::{DEFAULT_MAX_MESSAGE, MAX_PACKET_SIZE, MTU, RAW_CHANNEL_PROTOCOL};

#[derive(Debug)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Error {}
impl From<webrtc::error::Error> for Error {
    fn from(e: webrtc::error::Error) -> Self {
        Self(e.to_string())
    }
}

/// Go-compatible framing shared with browser WASM through xconn-framing.
pub struct Assembler(xconn_framing::Assembler);

impl Assembler {
    pub fn new(max_message: usize) -> Self {
        Self(xconn_framing::Assembler::new(max_message))
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.0.feed(chunk).map_err(|error| Error(error.to_string()))
    }

    pub fn chunks(message: &[u8]) -> impl Iterator<Item = Bytes> + '_ {
        xconn_framing::Assembler::chunks(message)
    }
}

#[derive(Clone, Debug)]
pub struct Packet {
    pub data: Bytes,
    pub is_string: bool,
}

/// Owns all data-channel callbacks once, so negotiation cannot lose messages
/// while switching callbacks between handshake and WAMP processing.
pub struct DataChannel {
    channel: Arc<dyn NativeChannel>,
    label: String,
    protocol: String,
    reliable_ordered: bool,
    incoming: Mutex<mpsc::Receiver<Packet>>,
    send_lock: Mutex<()>,
    open: watch::Sender<bool>,
    closed: watch::Sender<bool>,
    low_events: watch::Sender<u64>,
    low_threshold: Arc<std::sync::atomic::AtomicUsize>,
    pending_low: watch::Sender<u64>,
    pending_peak: Arc<std::sync::atomic::AtomicUsize>,
    failure: watch::Sender<Option<String>>,
    assembler: Mutex<Assembler>,
    prefetched: Mutex<std::collections::VecDeque<Packet>>,
}

impl fmt::Debug for DataChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataChannel").field("label", &self.label).finish()
    }
}

impl DataChannel {
    async fn attach(channel: Arc<dyn NativeChannel>) -> Result<Arc<Self>, Error> {
        let label = channel.label().await?;
        let protocol = channel.protocol().await?;
        let partial_reliability =
            channel.max_retransmits().await?.is_some() || channel.max_packet_life_time().await?.is_some();
        let reliable_ordered = channel.ordered().await? && !partial_reliability;
        let (tx, incoming) = mpsc::channel(64);
        let open = watch::channel(channel.ready_state().await? == RTCDataChannelState::Open).0;
        let closed = watch::channel(false).0;
        let (low_events, _) = watch::channel(0_u64);
        let (failure, _) = watch::channel(None);
        let low_threshold = Arc::new(std::sync::atomic::AtomicUsize::new(256 * 1024));
        let (pending_low, mut pending) = watch::channel(0_u64);
        let pending_peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = pending_peak.clone();
        let threshold = low_threshold.clone();
        let weak_channel = Arc::downgrade(&channel);
        let low_notifications = low_events.clone();
        let mut finished = closed.subscribe();
        // Observe buffer drains independently of callback scheduling. Monitor
        // only after an application or transport threshold is crossed; idle
        // channels do not poll or retain the underlying channel.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = pending.changed() => if changed.is_err() { return; },
                    _ = finished.wait_for(|value| *value) => return,
                }
                loop {
                    let Some(channel) = weak_channel.upgrade() else {
                        return;
                    };
                    let Ok(amount) = channel.outstanding_bytes().await else {
                        return;
                    };
                    drop(channel);
                    let requested = threshold.load(std::sync::atomic::Ordering::Acquire);
                    if amount <= requested.min(256 * 1024) {
                        if peak.swap(0, std::sync::atomic::Ordering::AcqRel) > requested {
                            low_notifications.send_modify(|value| *value = value.wrapping_add(1));
                        }
                        break;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                        _ = finished.wait_for(|value| *value) => return,
                    }
                }
            }
        });
        #[cfg(feature = "rpc-streams")]
        let backpressure = protocol == crate::rpc_streams::PROTOCOL;
        #[cfg(not(feature = "rpc-streams"))]
        let backpressure = false;
        let native = channel.clone();
        let opened = open.clone();
        let ended = closed.clone();
        let errors = failure.clone();
        let mut closing = ended.subscribe();
        tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    _ = tx.closed() => break,
                    _ = closing.wait_for(|closed| *closed) => break,
                    event = native.poll() => event,
                };
                match event {
                    Some(DataChannelEvent::OnOpen) => {
                        opened.send_replace(true);
                    }
                    Some(DataChannelEvent::OnMessage(message)) => {
                        let packet = Packet {
                            data: message.data.freeze(),
                            is_string: message.is_string,
                        };
                        if backpressure || partial_reliability {
                            tokio::select! {
                                _ = closing.wait_for(|closed| *closed) => break,
                                result = tx.send(packet) => if result.is_err() { break; },
                            }
                        } else if tx.try_send(packet).is_err() {
                            errors.send_replace(Some("WebRTC receive queue overflow".into()));
                            ended.send_replace(true);
                            let _ = native.close().await;
                            break;
                        }
                    }
                    Some(DataChannelEvent::OnError) => {
                        errors.send_replace(Some("WebRTC data channel error".into()));
                        break;
                    }
                    Some(DataChannelEvent::OnClose) | None => break,
                    _ => {}
                }
            }
            ended.send_replace(true);
        });
        Ok(Arc::new(Self {
            channel,
            label,
            protocol,
            reliable_ordered,
            incoming: Mutex::new(incoming),
            send_lock: Mutex::new(()),
            open,
            closed,
            low_events,
            low_threshold,
            pending_low,
            pending_peak,
            failure,
            assembler: Mutex::new(Assembler::new(DEFAULT_MAX_MESSAGE)),
            prefetched: Mutex::new(std::collections::VecDeque::new()),
        }))
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    #[cfg(feature = "rpc-streams")]
    pub(crate) fn reliable_ordered(&self) -> bool {
        self.reliable_ordered
    }

    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    pub async fn wait_open(&self, timeout: Duration) -> std::result::Result<(), Error> {
        let mut open = self.open.subscribe();
        let mut closed = self.closed.subscribe();
        tokio::time::timeout(timeout, async {
            tokio::select! {
                result = open.wait_for(|v| *v) => result.map(|_| ()).map_err(|_| Error("channel closed".into())),
                _ = closed.wait_for(|v| *v) => Err(Error("channel closed".into())),
            }
        })
        .await
        .map_err(|_| Error("channel open timeout".into()))?
    }

    pub async fn unread_packet(&self, packet: Bytes) {
        self.unread_data_packet(Packet {
            data: packet,
            is_string: false,
        })
        .await;
    }

    pub async fn unread_data_packet(&self, packet: Packet) {
        self.prefetched.lock().await.push_front(packet);
    }

    pub async fn set_receive_limit(&self, limit: usize) {
        self.assembler.lock().await.0.set_limit(limit);
    }

    pub async fn read_packet(&self) -> std::result::Result<Bytes, Error> {
        Ok(self.read_data_packet().await?.data)
    }

    pub async fn read_data_packet(&self) -> std::result::Result<Packet, Error> {
        if let Some(packet) = self.prefetched.lock().await.pop_front() {
            return Ok(packet);
        }

        let mut incoming = self.incoming.lock().await;
        let mut closed = self.closed.subscribe();
        tokio::select! {
            biased;
            packet = incoming.recv() => packet.ok_or_else(|| self.read_error()),
            _ = closed.wait_for(|v| *v) => Err(self.read_error()),
        }
    }

    fn read_error(&self) -> Error {
        Error(self.failure.borrow().clone().unwrap_or_else(|| "channel closed".into()))
    }

    async fn send_packet_inner(&self, data: Bytes) -> std::result::Result<(), Error> {
        if *self.closed.borrow() {
            return Err(Error("channel closed".into()));
        }
        self.channel
            .send(
                data.try_into_mut()
                    .unwrap_or_else(|data| bytes::BytesMut::from(data.as_ref())),
            )
            .await?;
        self.note_send(self.buffered_amount().await);
        Ok(())
    }

    pub async fn send_packet(&self, data: Bytes) -> std::result::Result<(), Error> {
        if data.len() > MAX_PACKET_SIZE {
            return Err(Error("raw packet exceeds 65535 bytes; use chunked messages".into()));
        }

        let _lock = self.send_lock.lock().await;
        self.send_packet_inner(data).await
    }

    pub async fn send_text(&self, text: &str) -> std::result::Result<(), Error> {
        if text.len() > MAX_PACKET_SIZE {
            return Err(Error("text packet exceeds 65535 bytes".into()));
        }
        let _lock = self.send_lock.lock().await;
        if *self.closed.borrow() {
            return Err(Error("channel closed".into()));
        }
        self.channel.send_text(text).await?;
        self.note_send(self.buffered_amount().await);
        Ok(())
    }

    pub async fn buffered_amount(&self) -> usize {
        self.channel.outstanding_bytes().await.unwrap_or_default()
    }

    fn note_send(&self, amount: usize) {
        if amount
            > self
                .low_threshold
                .load(std::sync::atomic::Ordering::Acquire)
                .min(256 * 1024)
        {
            self.pending_peak.fetch_max(amount, std::sync::atomic::Ordering::AcqRel);
            self.pending_low.send_modify(|value| *value = value.wrapping_add(1));
        }
    }

    pub async fn set_buffered_amount_low_threshold(&self, threshold: usize) {
        self.low_threshold
            .store(threshold, std::sync::atomic::Ordering::Release);
        self.note_send(self.channel.outstanding_bytes().await.unwrap_or_default());
    }

    pub async fn next_buffered_low(&self, previous: u64) -> std::result::Result<u64, Error> {
        let mut events = self.low_events.subscribe();
        let mut closed = self.closed.subscribe();
        tokio::select! {
            value = events.wait_for(|value| *value != previous) => value.map(|value| *value).map_err(|_| Error("channel closed".into())),
            _ = closed.wait_for(|value| *value) => Err(Error("channel closed".into())),
        }
    }

    pub async fn failure(&self) -> Option<String> {
        self.closed().await;
        self.failure.borrow().clone()
    }

    pub async fn read_message(&self) -> std::result::Result<Vec<u8>, Error> {
        let mut assembler = self.assembler.lock().await;

        loop {
            if let Some(message) = assembler.feed(&self.read_packet().await?)? {
                return Ok(message);
            }
        }
    }

    pub async fn write_message(&self, data: &[u8]) -> std::result::Result<(), Error> {
        if data.len() > DEFAULT_MAX_MESSAGE {
            return Err(Error("message exceeds send limit".into()));
        }

        let _lock = self.send_lock.lock().await;

        for chunk in Assembler::chunks(data) {
            self.send_packet_inner(chunk).await?;
        }

        Ok(())
    }

    /// Wait until the channel closes or fails.
    pub async fn closed(&self) {
        let _ = self.closed.subscribe().wait_for(|value| *value).await;
    }

    /// Finish queued outgoing messages before resetting the SCTP stream.
    /// Cancellation and resource cleanup can still use `close` to abort immediately.
    pub async fn close_gracefully(&self, timeout: Duration) -> std::result::Result<(), Error> {
        self.closed.send_replace(true);
        let drain = async {
            let _writer = self.send_lock.lock().await;
            while self.channel.outstanding_bytes().await.unwrap_or_default() > 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let drained = tokio::time::timeout(timeout, drain).await.is_ok();
        self.close().await?;
        if !drained {
            return Err(Error("channel flush timeout".into()));
        }
        Ok(())
    }

    pub async fn close(&self) -> std::result::Result<(), Error> {
        self.closed.send_replace(true);
        match self.channel.close().await {
            Ok(()) | Err(webrtc::error::Error::ErrDataChannelClosed) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Description {
    #[serde(default)]
    pub candidates: Vec<RTCIceCandidateInit>,
    pub description: RTCSessionDescription,
}

#[derive(Clone)]
pub struct Connection {
    #[cfg(feature = "rpc-streams")]
    pub(crate) rpc_session: Arc<Mutex<bool>>,
    pub peer: Arc<dyn PeerConnection>,
    incoming: Arc<Mutex<mpsc::Receiver<Arc<DataChannel>>>>,
    candidates: Arc<Mutex<mpsc::UnboundedReceiver<RTCIceCandidateInit>>>,
    closed: watch::Sender<bool>,
    remote: Arc<Mutex<RemoteCandidates>>,
    connected: watch::Sender<bool>,
    gathered: watch::Sender<bool>,
}

#[derive(Default)]
struct RemoteCandidates {
    ready: bool,
    pending: Vec<RTCIceCandidateInit>,
}

struct ConnectionEvents {
    peer: std::sync::OnceLock<std::sync::Weak<dyn PeerConnection>>,
    incoming: mpsc::Sender<Arc<DataChannel>>,
    candidates: mpsc::UnboundedSender<RTCIceCandidateInit>,
    closed: watch::Sender<bool>,
    connected: watch::Sender<bool>,
    gathered: watch::Sender<bool>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for ConnectionEvents {
    async fn on_data_channel(&self, native: Arc<dyn NativeChannel>) {
        if let Ok(channel) = DataChannel::attach(native.clone()).await
            && self.incoming.try_send(channel).is_ok()
        {
            return;
        }
        // The event driver must be free to process the close request.
        tokio::spawn(async move {
            let _ = native.close().await;
        });
    }

    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        if let Ok(candidate) = event.candidate.to_json() {
            let _ = self.candidates.send(candidate);
        }
    }

    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            self.gathered.send_replace(true);
            let _ = self.candidates.send(RTCIceCandidateInit::default());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        match state {
            RTCPeerConnectionState::Connected => {
                self.connected.send_replace(true);
            }
            // ICE lost its path. It may recover, or fail after its failed timeout.
            RTCPeerConnectionState::Disconnected => {
                self.connected.send_replace(false);
            }
            RTCPeerConnectionState::Failed => {
                self.closed.send_replace(true);
                if let Some(peer) = self.peer.get().and_then(std::sync::Weak::upgrade) {
                    tokio::spawn(async move {
                        let _ = peer.close().await;
                    });
                }
            }
            RTCPeerConnectionState::Closed => {
                self.closed.send_replace(true);
            }
            _ => {}
        }
    }
}

impl Connection {
    pub async fn new(ice_servers: Vec<RTCIceServer>) -> std::result::Result<Self, Error> {
        let (incoming, rx) = mpsc::channel(64);
        let (candidate_tx, candidates) = mpsc::unbounded_channel();
        let closed = watch::channel(false).0;
        let connected = watch::channel(false).0;
        let gathered = watch::channel(false).0;
        let events = Arc::new(ConnectionEvents {
            peer: std::sync::OnceLock::new(),
            incoming,
            candidates: candidate_tx,
            closed: closed.clone(),
            connected: connected.clone(),
            gathered: gathered.clone(),
        });
        let peer: Arc<dyn PeerConnection> = Arc::new(
            PeerConnectionBuilder::new()
                .with_configuration(RTCConfigurationBuilder::default().with_ice_servers(ice_servers).build())
                .with_udp_addrs(vec!["0.0.0.0:0", "[::]:0"])
                .with_runtime(Arc::new(crate::runtime::WebRtcRuntime))
                .with_data_channel_send_buffer_limit(512 * 1024)
                .with_handler(events.clone())
                .build()
                .await?,
        );
        let _ = events.peer.set(Arc::downgrade(&peer));
        Ok(Self {
            #[cfg(feature = "rpc-streams")]
            rpc_session: Arc::new(Mutex::new(false)),
            closed,
            peer,
            incoming: Arc::new(Mutex::new(rx)),
            candidates: Arc::new(Mutex::new(candidates)),
            remote: Arc::new(Mutex::new(RemoteCandidates::default())),
            connected,
            gathered,
        })
    }

    pub async fn open_channel(&self, label: &str) -> std::result::Result<Arc<DataChannel>, Error> {
        self.open_channel_with_options(
            label,
            RTCDataChannelInit {
                ordered: true,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn open_channel_with_options(
        &self,
        label: &str,
        options: RTCDataChannelInit,
    ) -> std::result::Result<Arc<DataChannel>, Error> {
        let channel = self.peer.create_data_channel(label, Some(options)).await?;
        DataChannel::attach(channel).await
    }

    /// Open an explicitly raw channel. It is independent of every WAMP session.
    pub async fn open_raw_channel(
        &self,
        label: &str,
        mut options: RTCDataChannelInit,
    ) -> std::result::Result<Arc<DataChannel>, Error> {
        options.protocol = RAW_CHANNEL_PROTOCOL.into();
        self.open_channel_with_options(label, options).await
    }

    pub async fn wait_connected(&self, timeout: Duration) -> std::result::Result<(), Error> {
        let mut connected = self.connected.subscribe();
        tokio::time::timeout(timeout, async {
            tokio::select! {
                _ = self.closed() => Err(Error("connection closed".into())),
                result = connected.wait_for(|value| *value) => result.map(|_| ()).map_err(|_| Error("connection closed".into())),
            }
        }).await.map_err(|_| Error("WebRTC connection timeout".into()))?
    }

    /// Whether ICE has a working path now: false before connecting and while disconnected.
    pub fn is_connected(&self) -> bool {
        *self.connected.borrow() && !*self.closed.borrow()
    }

    /// Wait until this connection fails or is closed.
    pub async fn closed(&self) {
        let _ = self.closed.subscribe().wait_for(|value| *value).await;
    }

    pub async fn accept_channel(&self) -> std::result::Result<Arc<DataChannel>, Error> {
        let mut incoming = self.incoming.lock().await;
        let mut closed = self.closed.subscribe();
        tokio::select! {
            channel = incoming.recv() => channel.ok_or_else(|| Error("connection closed".into())),
            _ = closed.wait_for(|v| *v) => Err(Error("connection closed".into())),
        }
    }

    /// Address used by the nominated ICE pair, for applications that pin routes.
    /// This is the peer or TURN relay address seen by the ICE transport.
    pub async fn selected_remote_address(&self) -> Option<String> {
        let ice = self.peer.sctp().await?.transport().ice_transport();
        let pair = ice.get_selected_candidate_pair().await.ok()??;
        Some(pair.remote().address.clone())
    }

    pub async fn next_candidate(&self) -> Option<RTCIceCandidateInit> {
        let mut incoming = self.candidates.lock().await;
        let mut closed = self.closed.subscribe();
        tokio::select! {
            candidate = incoming.recv() => candidate,
            _ = closed.wait_for(|v| *v) => None,
        }
    }

    pub async fn add_candidate(&self, candidate: RTCIceCandidateInit) -> std::result::Result<(), Error> {
        let mut remote = self.remote.lock().await;

        if remote.ready {
            self.peer.add_ice_candidate(candidate).await?;
        } else if remote.pending.len() < 256 {
            remote.pending.push(candidate);
        } else {
            return Err(Error("too many pending ICE candidates".into()));
        }

        Ok(())
    }

    /// Return initial SDP immediately; drain `next_candidate` and signal each candidate.
    pub async fn offer_trickle(&self) -> std::result::Result<Description, Error> {
        let offer = self.peer.create_offer(None).await?;
        self.local_trickle(offer).await
    }

    pub async fn answer_trickle(&self, offer: Description) -> std::result::Result<Description, Error> {
        self.set_remote(offer).await?;
        let answer = self.peer.create_answer(None).await?;
        self.local_trickle(answer).await
    }

    async fn local_trickle(&self, description: RTCSessionDescription) -> std::result::Result<Description, Error> {
        self.peer.set_local_description(description.clone()).await?;
        Ok(Description {
            candidates: Vec::new(),
            description,
        })
    }

    async fn local(&self, description: RTCSessionDescription) -> std::result::Result<Description, Error> {
        let mut gathered = self.gathered.subscribe();
        self.peer.set_local_description(description).await?;
        tokio::time::timeout(Duration::from_secs(20), gathered.wait_for(|done| *done))
            .await
            .map_err(|_| Error("ICE gathering timeout".into()))?
            .map_err(|_| Error("connection closed".into()))?;
        Ok(Description {
            candidates: Vec::new(),
            description: self
                .peer
                .local_description()
                .await
                .ok_or_else(|| Error("missing local SDP".into()))?,
        })
    }

    pub async fn offer(&self) -> std::result::Result<Description, Error> {
        let description = self.peer.create_offer(None).await?;
        self.local(description).await
    }

    pub async fn answer(&self, offer: Description) -> std::result::Result<Description, Error> {
        self.set_remote(offer).await?;
        let answer = self.peer.create_answer(None).await?;
        self.local(answer).await
    }

    pub async fn set_remote(&self, description: Description) -> std::result::Result<(), Error> {
        let mut remote = self.remote.lock().await;
        self.peer.set_remote_description(description.description).await?;

        for candidate in description.candidates.into_iter().chain(remote.pending.drain(..)) {
            self.peer.add_ice_candidate(candidate).await?;
        }

        remote.ready = true;
        Ok(())
    }

    pub async fn close(&self) -> std::result::Result<(), Error> {
        self.closed.send_replace(true);
        self.peer.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_boundaries_and_limits() {
        for size in [0, 1, MTU - 1, MTU, MTU * 4] {
            let data = vec![42; size];
            let mut assembler = Assembler::new(size);
            let mut result = None;

            for chunk in Assembler::chunks(&data) {
                assert!(chunk.len() <= MTU);
                result = assembler.feed(&chunk).unwrap();
            }

            assert_eq!(result.unwrap(), data);
        }

        let mut assembler = Assembler::new(2);
        assert!(assembler.feed(&[0, 1, 2, 3]).is_err());
        assert!(assembler.feed(&[2, 1]).is_err());
        assert_eq!(assembler.feed(&[1, 7]).unwrap(), Some(vec![7]));
    }

    // While ICE is disconnected the connection isn't usable, and it is again once ICE recovers.
    #[tokio::test]
    async fn disconnected_ice_is_not_connected_until_it_recovers() {
        let connection = Connection::new(Vec::new()).await.unwrap();
        let events = ConnectionEvents {
            peer: std::sync::OnceLock::new(),
            incoming: mpsc::channel(1).0,
            candidates: mpsc::unbounded_channel().0,
            closed: connection.closed.clone(),
            connected: connection.connected.clone(),
            gathered: connection.gathered.clone(),
        };
        assert!(!connection.is_connected());
        events
            .on_connection_state_change(RTCPeerConnectionState::Connected)
            .await;
        assert!(connection.is_connected());
        events
            .on_connection_state_change(RTCPeerConnectionState::Disconnected)
            .await;
        assert!(!connection.is_connected());
        events
            .on_connection_state_change(RTCPeerConnectionState::Connected)
            .await;
        assert!(connection.is_connected());
        events.on_connection_state_change(RTCPeerConnectionState::Closed).await;
        assert!(!connection.is_connected());
        connection.close().await.unwrap();
    }
}
