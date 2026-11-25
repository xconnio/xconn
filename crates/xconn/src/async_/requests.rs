//! Session-bound fluent requests with an explicit execution terminal.
crate::common::requests::session_request_builders!((async) (.await));

impl CallBuilder<'_> {
    /// Execute a call, retrying `wamp.error.no_such_procedure` with capped backoff.
    /// The timeout covers all attempts and the wait for a reply.
    /// A callee can return the same error, so use this only for calls safe to repeat.
    pub async fn execute_when_available(
        self,
        timeout: std::time::Duration,
    ) -> std::result::Result<crate::RpcResult, crate::Error> {
        use std::time::Duration;

        tokio::time::timeout(timeout, async {
            let mut delay = Duration::from_millis(50);
            loop {
                let reply = self.session.execute_call_ref(&self.request).await?;
                if !reply
                    .error
                    .as_ref()
                    .is_some_and(|error| error.uri == "wamp.error.no_such_procedure")
                {
                    return Ok(reply);
                }
                tokio::select! {
                    _ = self.session.wait_disconnect() => return Err(crate::Error::new("session disconnected")),
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(Duration::from_secs(1));
            }
        })
        .await
        .map_err(|_| crate::Error::new("procedure call timed out"))?
    }

    pub async fn stream(self) -> std::result::Result<super::call::Call, crate::Error> {
        self.stream_with(crate::StreamOptions::default()).await
    }

    pub async fn stream_with(
        self,
        options: crate::StreamOptions,
    ) -> std::result::Result<super::call::Call, crate::Error> {
        super::call::Call::open(self.session, self.request, options).await
    }
}
