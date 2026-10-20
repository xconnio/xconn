//! Blocking stream adapter for optional multiplexed transports.
use super::session::block_on;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct Stream(pub(crate) crate::async_::rawsocket::BoxStream);

impl std::io::Read for Stream {
    fn read(&mut self, data: &mut [u8]) -> std::io::Result<usize> {
        block_on(self.0.read(data))
    }
}

impl std::io::Write for Stream {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        block_on(self.0.write(data))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        block_on(self.0.flush())
    }
}

impl Stream {
    pub fn close(&mut self) -> std::io::Result<()> {
        block_on(self.0.shutdown())
    }
}

pub struct AuthenticatedStream {
    pub stream: Stream,
    pub session: wampproto::types::SessionDetails,
}
