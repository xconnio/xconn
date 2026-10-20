use super::{
    session::{Session, block_on},
    stream::{AuthenticatedStream, Stream},
};
use crate::{Error, SerializerSpec};

pub struct Connection {
    inner: crate::async_::webtransport::Connection,
}

impl Connection {
    pub fn connect(url: &str, config: crate::async_::webtransport::ClientConfig) -> Result<Self, crate::Error> {
        block_on(crate::async_::webtransport::Connection::connect(url, config)).map(|inner| Self { inner })
    }

    pub fn open_session(
        &self,
        realm: &str,
        spec: Box<dyn SerializerSpec>,
        auth: Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync>,
    ) -> Result<Session, Error> {
        block_on(self.inner.open_session(realm, spec, auth)).map(Session::from_async)
    }

    pub fn open_raw(&self) -> Result<Stream, Error> {
        block_on(self.inner.open_raw()).map(|stream| Stream(Box::new(stream)))
    }

    pub fn accept_raw(&self) -> Result<Stream, Error> {
        block_on(self.inner.accept_raw()).map(|stream| Stream(Box::new(stream)))
    }

    pub fn close(&self) {
        self.inner.close();
    }
}

pub struct Listener {
    inner: crate::async_::webtransport::Listener,
}

impl Listener {
    pub fn bind(router: &super::router::Router, addr: std::net::SocketAddr) -> Result<Self, Error> {
        block_on(crate::async_::webtransport::Listener::bind(
            router.as_async().clone(),
            addr,
        ))
        .map(|inner| Self { inner })
    }

    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.inner.local_addr
    }

    pub fn client_config(&self) -> Result<crate::async_::webtransport::ClientConfig, Error> {
        self.inner.client_config()
    }

    pub fn accept_raw(&self) -> Option<AuthenticatedStream> {
        block_on(self.inner.accept_raw()).map(|raw| AuthenticatedStream {
            stream: Stream(raw.stream),
            session: raw.session,
        })
    }

    pub fn close(&self) {
        self.inner.close();
    }
}
