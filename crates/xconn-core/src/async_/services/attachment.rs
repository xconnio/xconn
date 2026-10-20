//! Attachment supervision independent of a cloud protocol or credential store.
use crate::Error;
use std::time::Duration;

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
