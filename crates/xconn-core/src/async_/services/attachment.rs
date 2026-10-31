//! Attachment supervision independent of a cloud protocol or credential store.
use super::bridge::Bridge;
use crate::{Error, async_::session::Session};
use futures_util::future::select_all;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

/// Outcome of one attachment attempt. A cycle owns its sessions and resources
/// until it completes; dropping its future must release those resources.
pub enum AttachmentOutcome {
    /// The service revoked the attachment. Return control to enrollment/storage policy.
    Detached,
    /// Connection failed before attachment was established.
    Retry(Error),
    /// An established attachment disconnected. Reset reconnect backoff.
    Disconnected(Error),
}

/// Owns the upstream sessions and bridges of one established attachment.
/// Losing any session shuts down the whole attachment. Local backend sessions
/// shared with bridges remain owned by the application.
///
/// Dropping this handle requests cleanup on the current Tokio runtime. Use
/// `close().await` to wait for it; the runtime must remain alive until it finishes.
pub struct Attachment {
    stop: watch::Sender<bool>,
    completed: watch::Receiver<Option<Result<(), String>>>,
}

impl Attachment {
    pub fn new(sessions: Vec<Session>, bridges: Vec<Arc<Bridge>>) -> Result<Self, Error> {
        Self::with_shutdown(sessions, bridges, || async { Ok(()) })
    }

    /// The shutdown callback owns additional resources, such as a shared QUIC
    /// connection or stream workers. It runs once, after bridges and sessions
    /// close, including when the handle is dropped or a `close` wait is cancelled.
    pub fn with_shutdown<F, Fut>(sessions: Vec<Session>, bridges: Vec<Arc<Bridge>>, shutdown: F) -> Result<Self, Error>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        if sessions.is_empty() {
            return Err(Error::new("an attachment requires at least one upstream session"));
        }
        let (stop, mut stopped) = watch::channel(false);
        let (completed, completion) = watch::channel(None);
        tokio::spawn(async move {
            {
                let disconnected = sessions.iter().map(|session| Box::pin(session.wait_disconnect()));
                tokio::select! {
                    _ = stopped.wait_for(|stop| *stop) => {},
                    _ = select_all(disconnected) => {},
                }
            }
            let mut failure = None;
            for bridge in bridges {
                if let Err(error) = bridge.close().await {
                    failure.get_or_insert_with(|| error.to_string());
                }
            }
            for session in sessions {
                if session.connected()
                    && let Err(error) = session.close().await
                {
                    failure.get_or_insert_with(|| error.to_string());
                }
            }
            if let Err(error) = shutdown().await {
                failure.get_or_insert_with(|| error.to_string());
            }
            completed.send_replace(Some(failure.map_or(Ok(()), Err)));
        });
        Ok(Self {
            stop,
            completed: completion,
        })
    }

    /// Wait for disconnection or requested shutdown and completion of cleanup.
    /// Cancelling this wait leaves the attachment running until explicitly
    /// closed, dropped, or disconnected.
    pub async fn wait(&self) -> Result<(), Error> {
        let mut completed = self.completed.clone();
        completed
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Error::new("attachment task stopped before cleanup completed"))?
            .as_ref()
            .unwrap()
            .clone()
            .map_err(Error::new)
    }

    /// Request shutdown without waiting for cleanup to finish.
    pub fn request_close(&self) {
        self.stop.send_replace(true);
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.request_close();
        self.wait().await
    }
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.request_close();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub initial: Duration,
    pub maximum: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            maximum: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    pub fn validate(&self) -> Result<(), Error> {
        if self.initial.is_zero() || self.maximum < self.initial {
            return Err(Error::new(
                "attachment retry durations must be positive and maximum >= initial",
            ));
        }
        Ok(())
    }
}

/// Run attachment cycles until detached or the caller drops this future.
/// Enrollment, storage, discovery protocol and authorization remain explicit
/// application adapters. Error reporting is supplied by the embedding runtime.
pub async fn supervise<F, Fut, Report>(policy: RetryPolicy, mut cycle: F, mut report: Report) -> Result<(), Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = AttachmentOutcome>,
    Report: FnMut(&Error),
{
    policy.validate()?;
    let mut delay = policy.initial;
    loop {
        match cycle().await {
            AttachmentOutcome::Detached => return Ok(()),
            AttachmentOutcome::Retry(error) => report(&error),
            AttachmentOutcome::Disconnected(error) => {
                delay = policy.initial;
                report(&error);
            }
        }
        tokio::time::sleep(delay).await;
        delay = delay.saturating_mul(2).min(policy.maximum);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test(start_paused = true)]
    async fn retry_backoff_caps_and_resets_after_an_established_connection() {
        let started = tokio::time::Instant::now();
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let observed = attempts.clone();
        let mut attempt = 0;
        supervise(
            RetryPolicy::default(),
            || {
                observed.lock().unwrap().push(started.elapsed().as_secs());
                attempt += 1;
                let attempt = attempt;
                async move {
                    match attempt {
                        1..=6 => AttachmentOutcome::Retry(Error::new("offline")),
                        7 => AttachmentOutcome::Disconnected(Error::new("closed")),
                        _ => AttachmentOutcome::Detached,
                    }
                }
            },
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(*attempts.lock().unwrap(), vec![0, 1, 3, 7, 15, 31, 61, 62]);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_drops_a_blocked_cycle_without_starting_another() {
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let mut receiver = Some(receiver);
            supervise(
                RetryPolicy::default(),
                || {
                    let receiver = receiver.take().expect("must not reconnect");
                    async move {
                        let _ = receiver.await;
                        AttachmentOutcome::Detached
                    }
                },
                |_| {},
            )
            .await
        });
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(sender.is_closed());
    }
}
