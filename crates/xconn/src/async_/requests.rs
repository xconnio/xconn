//! Session-bound fluent requests with an explicit execution terminal.
crate::common::requests::session_request_builders!((async) (.await));

impl CallBuilder<'_> {
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
