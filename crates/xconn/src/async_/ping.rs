//! Correlate transport ping replies without blocking the session receive loop.
use crate::Error;
use std::{
    collections::HashMap,
    future::Future,
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
    pub async fn wait(
        mut self,
        timeout: Option<Duration>,
        send: impl Future<Output = Result<(), Error>>,
    ) -> Result<Duration, Error> {
        let response = async {
            send.await?;
            (&mut self.receiver)
                .await
                .map_err(|_| Error::new("connection closed"))?;
            Ok(self.started.elapsed())
        };

        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, response)
                .await
                .map_err(|_| Error::new("ping timed out"))?,
            None => response.await,
        }
    }
}

impl Drop for PendingPing {
    fn drop(&mut self) {
        self.state.pending.lock().unwrap().remove(&self.payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn sending_and_waiting_share_one_timeout() {
        let state = Arc::new(PingState::default());
        let ping = state.start();
        let started = tokio::time::Instant::now();
        let error = ping
            .wait(Some(Duration::from_secs(1)), async {
                tokio::time::sleep(Duration::from_millis(750)).await;
                Ok(())
            })
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "ping timed out");
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert!(state.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pong_completes_with_or_without_timeout() {
        for timeout in [None, Some(Duration::from_secs(1))] {
            let state = Arc::new(PingState::default());
            let ping = state.start();
            let payload = ping.payload;
            ping.wait(timeout, async {
                state.pong(&payload);
                Ok(())
            })
            .await
            .unwrap();

            assert!(state.pending.lock().unwrap().is_empty());
        }
    }
}
