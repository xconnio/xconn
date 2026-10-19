//! Correlate transport ping replies without blocking the session receive loop.
use crate::Error;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

#[derive(Debug, Default)]
pub(crate) struct PingState {
    next: AtomicU64,
    pending: Mutex<HashMap<[u8; 8], oneshot::Sender<()>>>,
}

impl PingState {
    pub fn start(self: &Arc<Self>) -> PendingPing {
        let payload = self.next.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().unwrap().insert(payload, sender);
        PendingPing {
            state: self.clone(),
            payload,
            receiver,
            started: Instant::now(),
        }
    }

    pub fn pong(&self, payload: &[u8]) {
        let Ok(payload) = <[u8; 8]>::try_from(payload) else {
            return;
        };

        if let Some(sender) = self.pending.lock().unwrap().remove(&payload) {
            let _ = sender.send(());
        }
    }

    pub fn close(&self) {
        self.pending.lock().unwrap().clear();
    }
}

pub(crate) struct PendingPing {
    state: Arc<PingState>,
    pub payload: [u8; 8],
    receiver: oneshot::Receiver<()>,
    started: Instant,
}

impl PendingPing {
    pub async fn wait(mut self, timeout: Option<Duration>) -> Result<Duration, Error> {
        let response = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, &mut self.receiver)
                .await
                .map_err(|_| Error::new("ping timed out"))?,
            None => (&mut self.receiver).await,
        };
        response.map_err(|_| Error::new("connection closed"))?;
        Ok(self.started.elapsed())
    }
}

impl Drop for PendingPing {
    fn drop(&mut self) {
        self.state.pending.lock().unwrap().remove(&self.payload);
    }
}
