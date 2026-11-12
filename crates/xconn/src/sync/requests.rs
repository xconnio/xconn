//! Session-bound fluent requests with an explicit execution terminal.
crate::common::requests::session_request_builders!(()());

impl CallBuilder<'_> {
    pub fn stream(self) -> std::result::Result<super::call::Call, crate::Error> {
        self.stream_with(crate::StreamOptions::default())
    }

    pub fn stream_with(self, options: crate::StreamOptions) -> std::result::Result<super::call::Call, crate::Error> {
        super::session::block_on(super::super::async_::call::Call::open(
            self.session.as_async(),
            self.request,
            options,
        ))
        .map(super::call::Call::new)
    }
}
