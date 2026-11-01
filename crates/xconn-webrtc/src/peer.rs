//! Default realm-scoped discovery and direct connection establishment.
use crate::{
    wamp::{Provider, SignalingConfig, Timeouts, WebRTCSession, connect_with_timeouts},
    webrtc::ice_transport::ice_server::RTCIceServer,
};
use std::sync::Arc;
use tokio::sync::watch;
use xconn_core::wampproto::messages::schema::WireValue;
use xconn_core::{
    Error, Result as RpcResult, SerializerSpec, Value,
    async_::{
        router::Router,
        services::peer::{Connector, Peer, RetryPolicy},
        session::Session,
    },
    wampproto::authenticators::authenticator::ClientAuthenticator,
};

pub const RESOLVE: &str = "io.xconn.peer.resolve";
const OFFER: &str = "io.xconn.peer.offer";
const OFFERER_CANDIDATES: &str = "io.xconn.peer.offerer_candidates";
const ANSWERER_CANDIDATES: &str = "io.xconn.peer.answerer_candidates";

#[derive(Clone)]
pub struct Config {
    pub ice_servers: Vec<RTCIceServer>,
    pub retry: RetryPolicy,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".into()],
                ..Default::default()
            }],
            retry: RetryPolicy::default(),
        }
    }
}

impl Config {
    fn signaling(&self, offerer: bool) -> SignalingConfig {
        let (remote, local) = if offerer {
            (ANSWERER_CANDIDATES, OFFERER_CANDIDATES)
        } else {
            (OFFERER_CANDIDATES, ANSWERER_CANDIDATES)
        };
        SignalingConfig {
            offer_procedure: OFFER.into(),
            remote_candidates_topic: remote.into(),
            local_candidates_topic: local.into(),
            ice_servers: self.ice_servers.clone(),
        }
    }

    fn timeouts(&self) -> Timeouts {
        Timeouts {
            connect: self.retry.connect_timeout,
            ..Default::default()
        }
    }
}

/// Discover one edge in the signaling realm and authenticate a separate session.
/// The caller bounds the complete attempt, including discovery.
pub async fn connect_once(
    hub: &Session,
    config: &Config,
    spec: Box<dyn SerializerSpec>,
    auth: Box<dyn ClientAuthenticator + Send + Sync>,
) -> Result<WebRTCSession, Error> {
    let capability = hub.call(RESOLVE).execute().await?;
    if let Some(error) = capability.error {
        return Err(Error::new(error.uri));
    }
    let args = capability.args.unwrap_or_default();
    if args.get(1).and_then(Value::integer) == Some(hub.details().id()) {
        return Err(Error::new("realm endpoint belongs to this session"));
    }
    if args.first() != Some(&Value::Str("webrtc".into())) {
        return Err(Error::new("realm does not advertise a supported direct transport"));
    }
    connect_with_timeouts(
        hub,
        config.signaling(true),
        &hub.details().realm(),
        spec,
        auth,
        config.timeouts(),
    )
    .await
}

/// Construct a managed caller. Factories supply fresh authentication/serialization
/// state for each retry; discovery and all networking remain in Rust.
pub fn connect<S, A>(
    hub: Session,
    config: Config,
    serializer: S,
    authenticator: A,
) -> Result<Peer<WebRTCSession>, Error>
where
    S: Fn() -> Result<Box<dyn SerializerSpec>, Error> + Send + Sync + 'static,
    A: Fn() -> Result<Box<dyn ClientAuthenticator + Send + Sync>, Error> + Send + Sync + 'static,
{
    let signaling = hub.clone();
    let policy = config.retry;
    let serializer = Arc::new(serializer);
    let authenticator = Arc::new(authenticator);
    let connector: Connector<WebRTCSession> = Arc::new(move || {
        let (hub, config, serializer, authenticator) = (
            signaling.clone(),
            config.clone(),
            serializer.clone(),
            authenticator.clone(),
        );
        Box::pin(async move { connect_once(&hub, &config, serializer()?, authenticator()?).await })
    });
    Peer::start(hub, connector, policy)
}

/// Owns the standard capability registration and WebRTC provider. Application
/// procedures, bridges, credentials and realm permissions remain application policy.
pub struct Endpoint {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
}

impl Endpoint {
    pub async fn expose(signaling: Session, router: Router, config: Config) -> Result<Self, Error> {
        let provider =
            Provider::start_with_timeouts(signaling.clone(), router, config.signaling(false), config.timeouts())
                .await?;
        let id = signaling.details().id();
        let registered = signaling
            .register(RESOLVE, move |_| async move {
                RpcResult::args(vec![Value::Str("webrtc".into()), Value::Int(id)])
            })
            .execute()
            .await?;
        if let Some(error) = registered.error {
            provider.close().await;
            return Err(Error::new(error.uri));
        }
        let registration = registered.registration_id;
        let (stop, mut stopping) = watch::channel(false);
        let (finished, done) = watch::channel(false);
        // This worker owns cleanup, so canceling close() cannot interrupt it.
        tokio::spawn(async move {
            loop {
                let raw = tokio::select! {
                    biased;
                    _ = stopping.wait_for(|stop| *stop) => break,
                    _ = signaling.wait_disconnect() => break,
                    raw = provider.accept_raw() => raw,
                };
                let Some(raw) = raw else {
                    break;
                };
                let _ = raw.channel.close().await;
            }
            let _ = signaling.unregister(registration).await;
            provider.close().await;
            finished.send_replace(true);
        });
        Ok(Self { stop, done })
    }

    pub async fn close(&self) {
        self.stop.send_replace(true);
        let mut done = self.done.clone();
        let _ = done.wait_for(|done| *done).await;
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
