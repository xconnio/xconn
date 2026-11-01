//! RPC-associated transport streams. References are bound by the dealer's
//! authorized routing decision, never by a client-provided destination.
use super::{peer::StreamOpener, rawsocket::BoxStream};
use crate::{Error, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Notify, oneshot, watch},
};

pub(crate) const HEADER: [u8; 4] = [0x7f, 0, 0, 0];
const KEY: &str = "io.xconn.stream.v1";
const WAIT: Duration = Duration::from_secs(30);
const CAPACITY: usize = 8192;
type Token = [u8; 32];

fn reference(token: Token) -> Value {
    let encoded: String = token.iter().map(|byte| format!("{byte:02x}")).collect();
    Value::Dict(HashMap::from([(KEY.into(), Value::Str(encoded))]))
}

pub(crate) fn token(value: &Value) -> Result<Option<Token>, Error> {
    let Value::Dict(fields) = value else { return Ok(None) };
    let Some(encoded) = fields.get(KEY) else {
        return Ok(None);
    };
    let Value::Str(encoded) = encoded else {
        return Err(Error::new("invalid stream reference"));
    };
    if fields.len() != 1 || encoded.len() != 64 || !encoded.is_ascii() {
        return Err(Error::new("invalid stream reference"));
    }
    let mut token = [0; 32];
    for (index, byte) in token.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
            .map_err(|_| Error::new("invalid stream reference"))?;
    }
    Ok(Some(token))
}

pub(crate) async fn offer(open: &StreamOpener) -> Result<(Value, BoxStream), Error> {
    let mut token = [0; 32];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut token)
        .map_err(|_| Error::new("cannot generate stream reference"))?;
    Ok((reference(token), connect(open, token).await?))
}

pub(crate) async fn accept(open: &StreamOpener, value: &Value) -> Result<BoxStream, Error> {
    let token = token(value)?.ok_or_else(|| Error::new("argument is not a stream"))?;
    connect(open, token).await
}

async fn connect(open: &StreamOpener, token: Token) -> Result<BoxStream, Error> {
    let mut stream = open().await?;
    let mut header = Vec::with_capacity(36);
    header.extend_from_slice(&HEADER);
    header.extend_from_slice(&token);
    stream.write_all(&header).await.map_err(|e| Error::new(e.to_string()))?;
    Ok(stream)
}

struct Grant {
    generation: Arc<()>,
    parties: [i64; 2],
    invocation: Option<(i64, i64)>,
    created: Instant,
    waiting: Option<(i64, oneshot::Sender<BoxStream>)>,
    joined: bool,
    cancelled: watch::Sender<bool>,
}

pub(crate) struct Registry {
    grants: Mutex<HashMap<Token, Grant>>,
    sessions: Mutex<HashMap<i64, watch::Sender<()>>>,
    changed: Notify,
    pending: tokio::sync::Semaphore,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            grants: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            pending: tokio::sync::Semaphore::new(CAPACITY),
        }
    }
}

impl Registry {
    pub(crate) fn attach(&self, session: i64) {
        self.sessions
            .lock()
            .unwrap()
            .entry(session)
            .or_insert_with(|| watch::channel(()).0);
    }

    pub(crate) fn memory_opener(self: &Arc<Self>, session: i64) -> Result<StreamOpener, Error> {
        let closed = self
            .sessions
            .lock()
            .unwrap()
            .get(&session)
            .map(watch::Sender::subscribe)
            .ok_or_else(|| Error::new("stream session is no longer attached"))?;
        let registry = self.clone();
        Ok(Arc::new(move || {
            let registry = registry.clone();
            let mut closed = closed.clone();
            Box::pin(async move {
                if closed.has_changed().is_err() {
                    return Err(Error::new("stream session is no longer attached"));
                }
                let (client, mut server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    tokio::select! {
                        biased;
                        _ = closed.changed() => {},
                        _ = async {
                            let mut header = [0; 4];
                            let read = tokio::time::timeout(WAIT, server.read_exact(&mut header)).await;
                            if read.is_ok_and(|result| result.is_ok()) && header == HEADER {
                                let _ = registry.relay(session, Box::new(server)).await;
                            }
                        } => {},
                    }
                });
                Ok(Box::new(client) as BoxStream)
            })
        }))
    }

    /// Called only after authorization and dealer selection, before delivery.
    pub(crate) fn grant(
        &self,
        source: i64,
        target: i64,
        args: &[Value],
        invocation: Option<(i64, i64)>,
    ) -> Result<(), Error> {
        let tokens: Vec<_> = args
            .iter()
            .filter_map(|arg| token(arg).transpose())
            .collect::<Result<_, _>>()?;
        if tokens.is_empty() {
            return Ok(());
        }
        let mut grants = self.grants.lock().unwrap();
        grants.retain(|_, grant| grant.joined || grant.created.elapsed() < WAIT);
        let mut unique = std::collections::HashSet::new();
        if grants.len() + tokens.len() > CAPACITY
            || tokens
                .iter()
                .any(|key| !unique.insert(*key) || grants.contains_key(key))
        {
            return Err(Error::new("stream reference is reused or capacity exhausted"));
        }
        for key in tokens {
            grants.insert(
                key,
                Grant {
                    generation: Arc::new(()),
                    parties: [source, target],
                    invocation,
                    created: Instant::now(),
                    waiting: None,
                    joined: false,
                    cancelled: watch::channel(false).0,
                },
            );
        }
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn route(
        &self,
        source: i64,
        target: i64,
        message: &dyn wampproto::messages::message::Message,
    ) -> Result<(), Error> {
        use wampproto::messages::{invocation::Invocation, result::Result_};
        if let Some(invocation) = message.as_any().downcast_ref::<Invocation>() {
            self.grant(
                source,
                target,
                invocation.args.as_deref().unwrap_or_default(),
                Some((target, invocation.request_id)),
            )?;
        } else if let Some(result) = message.as_any().downcast_ref::<Result_>() {
            self.grant(source, target, result.args.as_deref().unwrap_or_default(), None)?;
        }
        Ok(())
    }

    pub(crate) fn finish_invocation(&self, session: i64, request: i64) {
        self.grants
            .lock()
            .unwrap()
            .retain(|_, grant| grant.invocation != Some((session, request)));
        self.changed.notify_waiters();
    }

    pub(crate) fn detach(&self, session: i64) {
        self.sessions.lock().unwrap().remove(&session);
        self.grants
            .lock()
            .unwrap()
            .retain(|_, grant| !grant.parties.contains(&session));
        self.changed.notify_waiters();
    }

    pub(crate) async fn relay(self: &Arc<Self>, session: i64, mut stream: BoxStream) -> Result<(), Error> {
        let _capacity = self
            .pending
            .try_acquire()
            .map_err(|_| Error::new("too many pending streams"))?;
        let mut key = [0; 32];
        tokio::time::timeout(WAIT, stream.read_exact(&mut key))
            .await
            .map_err(|_| Error::new("stream header timeout"))?
            .map_err(|e| Error::new(e.to_string()))?;
        struct Cleanup(Arc<Registry>, Token, Arc<()>);

        impl Drop for Cleanup {
            fn drop(&mut self) {
                let mut grants = self.0.grants.lock().unwrap();
                if grants
                    .get(&self.1)
                    .is_some_and(|grant| Arc::ptr_eq(&grant.generation, &self.2))
                {
                    grants.remove(&self.1);
                }
            }
        }

        let mut cleanup = None;
        let mut incoming = Some(stream);
        let paired = tokio::time::timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if !self.sessions.lock().unwrap().contains_key(&session) {
                    return Err(Error::new("stream session is no longer attached"));
                }
                let pending = {
                    let mut grants = self.grants.lock().unwrap();
                    if let Some(grant) = grants.get_mut(&key) {
                        if !grant.parties.contains(&session) || grant.joined {
                            return Err(Error::new("stream is not authorized for this session"));
                        }
                        if let Some((first, sender)) = grant.waiting.take() {
                            if first == session && grant.parties[0] != grant.parties[1] {
                                grant.waiting = Some((first, sender));
                                return Err(Error::new("stream endpoint already claimed"));
                            }
                            grant.joined = true;
                            sender
                                .send(incoming.take().unwrap())
                                .map_err(|_| Error::new("stream endpoint closed"))?;
                            return Ok(None);
                        }
                        let (sender, receiver) = oneshot::channel();
                        grant.waiting = Some((session, sender));
                        cleanup = Some(Cleanup(self.clone(), key, grant.generation.clone()));
                        Some((receiver, grant.cancelled.subscribe()))
                    } else {
                        None
                    }
                };
                if let Some((receiver, cancelled)) = pending {
                    let peer = receiver.await.map_err(|_| Error::new("stream association closed"))?;
                    return Ok(Some((peer, cancelled)));
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| Error::new("stream association timeout"))??;
        if let Some((peer, mut cancelled)) = paired {
            tokio::select! {
                result = forward(incoming.take().unwrap(), peer) => { result?; }
                _ = cancelled.changed() => {}
            }
        }
        drop(cleanup);
        Ok(())
    }
}

// EOF half-closes the opposite writer while the remaining reply drains.
pub(crate) async fn forward(mut source: BoxStream, mut destination: BoxStream) -> Result<(u64, u64), Error> {
    tokio::io::copy_bidirectional(&mut source, &mut destination)
        .await
        .map_err(|error| Error::new(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn references_are_strict_and_cannot_replace_an_existing_grant() {
        let registry = Registry::default();
        registry.attach(1);
        registry.attach(2);
        let value = reference([3; 32]);
        registry
            .grant(1, 2, &[Value::Str("name".into()), value.clone()], Some((2, 9)))
            .unwrap();
        assert!(registry.grant(1, 3, std::slice::from_ref(&value), None).is_err());
        assert_eq!(registry.grants.lock().unwrap()[&[3; 32]].parties, [1, 2]);
        assert!(token(&Value::Dict(HashMap::from([(KEY.into(), Value::Bool(true))]))).is_err());
        assert!(token(&Value::Str("ordinary".into())).unwrap().is_none());
        registry.finish_invocation(2, 9);
        assert!(registry.grants.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn only_the_authorized_rpc_endpoints_can_pair_and_half_close() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let registry = Arc::new(Registry::default());
            registry.attach(1);
            registry.attach(2);
            registry.attach(3);
            registry.grant(1, 2, &[reference([7; 32])], None).unwrap();
            let (mut attacker, incoming) = duplex(64);
            attacker.write_all(&[7; 32]).await.unwrap();
            assert!(registry.relay(3, Box::new(incoming)).await.is_err());
            assert_eq!(registry.grants.lock().unwrap()[&[7; 32]].parties, [1, 2]);

            let (mut caller, incoming) = duplex(64);
            let (mut backend, outgoing) = duplex(64);
            caller.write_all(&[7; 32]).await.unwrap();
            backend.write_all(&[7; 32]).await.unwrap();
            let first = tokio::spawn({
                let registry = registry.clone();
                async move { registry.relay(1, Box::new(incoming)).await }
            });
            let second = tokio::spawn({
                let registry = registry.clone();
                async move { registry.relay(2, Box::new(outgoing)).await }
            });
            let send = async {
                caller.write_all(b"request").await.unwrap();
                caller.shutdown().await.unwrap();
                let mut response = Vec::new();
                caller.read_to_end(&mut response).await.unwrap();
                assert_eq!(response, vec![42; 128 * 1024]);
            };
            let receive = async {
                let mut request = Vec::new();
                backend.read_to_end(&mut request).await.unwrap();
                assert_eq!(request, b"request");
                backend.write_all(&vec![42; 128 * 1024]).await.unwrap();
                backend.shutdown().await.unwrap();
            };
            tokio::join!(send, receive);
            first.await.unwrap().unwrap();
            second.await.unwrap().unwrap();
            assert!(registry.grants.lock().unwrap().is_empty());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn detaching_a_session_closes_a_waiting_stream() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let registry = Arc::new(Registry::default());
            registry.attach(1);
            registry.attach(2);
            registry.grant(1, 2, &[reference([5; 32])], None).unwrap();
            let (mut caller, incoming) = duplex(64);
            caller.write_all(&[5; 32]).await.unwrap();
            let waiting = tokio::spawn({
                let registry = registry.clone();
                async move { registry.relay(1, Box::new(incoming)).await }
            });
            while registry.grants.lock().unwrap()[&[5; 32]].waiting.is_none() {
                tokio::task::yield_now().await;
            }
            registry.detach(2);
            assert!(waiting.await.unwrap().is_err());
            assert_eq!(caller.read(&mut [0]).await.unwrap(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn memory_pipes_close_on_session_leave_before_an_rpc_grant() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let registry = Arc::new(Registry::default());
            registry.attach(1);
            let open = registry.memory_opener(1).unwrap();
            let (_, mut stream) = offer(&open).await.unwrap();
            while registry.pending.available_permits() == CAPACITY {
                tokio::task::yield_now().await;
            }
            registry.detach(1);
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            assert!(registry.grants.lock().unwrap().is_empty());
            assert!(offer(&open).await.is_err());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn an_old_memory_opener_cannot_claim_a_reused_session_id() {
        let registry = Arc::new(Registry::default());
        registry.attach(1);
        let old = registry.memory_opener(1).unwrap();
        registry.detach(1);
        registry.attach(1);
        assert!(offer(&old).await.is_err());
        let replacement = registry.memory_opener(1).unwrap();
        let (_, stream) = offer(&replacement).await.unwrap();
        drop(stream);
        registry.detach(1);
    }
}
