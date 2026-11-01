//! Select a session for new calls without replaying or migrating existing work.
use crate::{Error, async_::session::Session};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
};

/// An independently authenticated direct session and its owned transport.
#[async_trait::async_trait]
pub trait DirectConnection: Send + Sync + 'static {
    fn session(&self) -> &Session;

    async fn close(&self);
}

pub type Connector<C> = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<C, Error>> + Send>> + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub initial: Duration,
    pub maximum: Duration,
    pub connect_timeout: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            maximum: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(20),
        }
    }
}

/// Borrows the hub session and owns only its direct connections and retry worker.
/// Subscriptions and registrations belong on the original hub session.
pub struct Peer<C: DirectConnection> {
    hub: Session,
    direct: Arc<Mutex<Option<Arc<C>>>>,
    last_error: Arc<Mutex<Option<String>>>,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
}

impl<C: DirectConnection> Peer<C> {
    pub fn start(hub: Session, connect: Connector<C>, policy: RetryPolicy) -> Result<Self, Error> {
        if policy.initial.is_zero() || policy.maximum < policy.initial || policy.connect_timeout.is_zero() {
            return Err(Error::new("invalid peer retry policy"));
        }
        if !hub.connected() {
            return Err(Error::new("hub session is disconnected"));
        }
        let direct = Arc::new(Mutex::new(None));
        let active = direct.clone();
        let last_error = Arc::new(Mutex::new(None));
        let failure = last_error.clone();
        let upstream = hub.clone();
        let (stop, mut stopping) = watch::channel(false);
        let (finished, done) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut delay = policy.initial;
            loop {
                let attempt = tokio::select! {
                    biased;
                    _ = stopping.wait_for(|stop| *stop) => break,
                    _ = upstream.wait_disconnect() => break,
                    result = tokio::time::timeout(policy.connect_timeout, connect()) => result,
                };
                let connected = match attempt {
                    Ok(Ok(connection)) => Some(connection),
                    Ok(Err(error)) => {
                        *failure.lock().unwrap() = Some(error.to_string());
                        None
                    }
                    Err(_) => {
                        *failure.lock().unwrap() = Some("direct connection timed out".into());
                        None
                    }
                };
                if let Some(connection) = connected {
                    let connection = Arc::new(connection);
                    // A direct endpoint must serve the same destination realm.
                    if connection.session().details().realm() != upstream.details().realm() {
                        *failure.lock().unwrap() = Some("direct endpoint realm mismatch".into());
                        connection.close().await;
                    } else {
                        failure.lock().unwrap().take();
                        *active.lock().unwrap() = Some(connection.clone());
                        tokio::select! {
                            biased;
                            _ = stopping.wait_for(|stop| *stop) => {},
                            _ = upstream.wait_disconnect() => {},
                            _ = connection.session().wait_disconnect() => {},
                        }
                        active.lock().unwrap().take();
                        connection.close().await;
                        delay = policy.initial;
                    }
                }
                tokio::select! {
                    biased;
                    _ = stopping.wait_for(|stop| *stop) => break,
                    _ = upstream.wait_disconnect() => break,
                    _ = tokio::time::sleep(delay) => {},
                }
                delay = delay.saturating_mul(2).min(policy.maximum);
            }
            finished.send_replace(true);
        });
        Ok(Self {
            hub,
            direct,
            last_error,
            stop,
            done,
            task: AsyncMutex::new(Some(task)),
        })
    }

    /// Snapshot a path once, before preparing arguments or dispatching the call.
    /// A subsequent transport error must be returned, never retried on another path.
    pub fn session(&self) -> Result<Session, Error> {
        self.session_for("")
    }

    pub fn session_for(&self, procedure: &str) -> Result<Session, Error> {
        self.ensure_open()?;
        Ok(self
            .direct_for(procedure)
            .map_or_else(|| self.hub.clone(), |connection| connection.session().clone()))
    }

    pub fn direct(&self) -> Option<Arc<C>> {
        self.direct
            .lock()
            .unwrap()
            .as_ref()
            .filter(|connection| connection.session().connected())
            .cloned()
    }

    /// Meta operations and direct negotiation use the hub's identity/state.
    pub fn direct_for(&self, procedure: &str) -> Option<Arc<C>> {
        if procedure.starts_with("wamp.") || procedure.starts_with("io.xconn.peer.") {
            return None;
        }
        self.direct()
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    pub fn ensure_open(&self) -> Result<(), Error> {
        if *self.stop.borrow() || *self.done.borrow() || !self.hub.connected() {
            return Err(Error::new("peer is closed"));
        }
        Ok(())
    }

    pub async fn wait_closed(&self) {
        let mut done = self.done.clone();
        let _ = done.wait_for(|done| *done).await;
    }

    pub async fn close(&self) {
        self.stop.send_replace(true);
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

impl<C: DirectConnection> Drop for Peer<C> {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CBORSerializerSpec, RpcResult,
        async_::router::{RealmConfig, Router},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    struct Direct {
        session: Session,
        closed: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl DirectConnection for Direct {
        fn session(&self) -> &Session {
            &self.session
        }

        async fn close(&self) {
            self.closed.fetch_add(1, Ordering::SeqCst);
            let _ = self.session.close().await;
        }
    }

    async fn joined(router: &Router) -> Session {
        router
            .connect_internal("realm", "client", "trusted", Box::new(CBORSerializerSpec))
            .await
            .unwrap()
    }

    async fn routed(router: &Router, label: &'static str) -> Session {
        router.add_realm("realm", RealmConfig::default()).unwrap();
        let backend = joined(router).await;
        backend
            .register("app.path", move |_| async move { RpcResult::arg(label) })
            .execute()
            .await
            .unwrap();
        backend
    }

    async fn until(mut check: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !check() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn direct_loss_falls_back_and_reconnects_without_closing_the_hub() {
        let hub_router = Router::new();
        let hub_backend = routed(&hub_router, "hub").await;
        let direct_router = Router::new();
        let direct_backend = routed(&direct_router, "direct").await;
        let hub = joined(&hub_router).await;
        let (tx, rx) = mpsc::channel::<Session>(2);
        let rx = Arc::new(AsyncMutex::new(rx));
        let closed = Arc::new(AtomicUsize::new(0));
        let counter = closed.clone();
        let peer = Peer::start(
            hub.clone(),
            Arc::new(move || {
                let rx = rx.clone();
                let closed = counter.clone();
                Box::pin(async move {
                    Ok(Direct {
                        session: rx.lock().await.recv().await.unwrap(),
                        closed,
                    })
                })
            }),
            RetryPolicy {
                initial: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            peer.session().unwrap().call("app.path").execute().await.unwrap().args,
            Some(vec!["hub".into()])
        );
        let first = joined(&direct_router).await;
        tx.send(first.clone()).await.unwrap();
        until(|| peer.direct().is_some()).await;
        assert_eq!(
            peer.session().unwrap().call("app.path").execute().await.unwrap().args,
            Some(vec!["direct".into()])
        );
        first.close().await.unwrap();
        until(|| peer.direct().is_none()).await;
        assert_eq!(
            peer.session().unwrap().call("app.path").execute().await.unwrap().args,
            Some(vec!["hub".into()])
        );
        tx.send(joined(&direct_router).await).await.unwrap();
        until(|| peer.direct().is_some()).await;
        peer.close().await;
        assert!(peer.session().is_err());
        assert!(hub.connected());
        assert_eq!(closed.load(Ordering::SeqCst), 2);
        hub.close().await.unwrap();
        hub_backend.close().await.unwrap();
        direct_backend.close().await.unwrap();
        hub_router.close();
        direct_router.close();
    }

    #[tokio::test]
    async fn timeout_retries_and_close_cancels_an_unfinished_connection() {
        let router = Router::new();
        let backend = routed(&router, "hub").await;
        let hub = joined(&router).await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = attempts.clone();
        let peer = Peer::<Direct>::start(
            hub.clone(),
            Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::pending())
            }),
            RetryPolicy {
                initial: Duration::from_millis(1),
                maximum: Duration::from_millis(2),
                connect_timeout: Duration::from_millis(5),
            },
        )
        .unwrap();
        until(|| attempts.load(Ordering::SeqCst) >= 2).await;
        tokio::time::timeout(Duration::from_secs(1), peer.close())
            .await
            .unwrap();
        assert!(hub.connected());
        hub.close().await.unwrap();
        backend.close().await.unwrap();
        router.close();
    }
}
