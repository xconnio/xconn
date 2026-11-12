use super::session::{Session, block_on};
pub use crate::async_::router::{AuthenticationFactory, Authorizer, Permission, RealmConfig};
use crate::{Error, SerializerSpec};

#[derive(Clone, Default)]
pub struct Router {
    inner: crate::async_::router::Router,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_realm(&self, name: impl Into<String>, config: RealmConfig) -> Result<(), Error> {
        self.inner.add_realm(name, config)
    }

    pub fn has_realm(&self, name: &str) -> bool {
        self.inner.has_realm(name)
    }

    pub fn sessions(&self, name: &str) -> Result<Vec<wampproto::types::SessionDetails>, Error> {
        self.inner.sessions(name)
    }

    pub fn connect(
        &self,
        realm: &str,
        spec: Box<dyn SerializerSpec>,
        auth: Box<dyn wampproto::authenticators::authenticator::ClientAuthenticator + Send + Sync>,
    ) -> Result<Session, Error> {
        block_on(self.inner.connect(realm, spec, auth)).map(Session::from_async)
    }

    pub fn as_async(&self) -> &crate::async_::router::Router {
        &self.inner
    }

    pub fn connect_internal(
        &self,
        realm: &str,
        authid: &str,
        role: &str,
        spec: Box<dyn SerializerSpec>,
    ) -> Result<Session, Error> {
        block_on(self.inner.connect_internal(realm, authid, role, spec)).map(Session::from_async)
    }

    pub fn add_alias(&self, realm: &str, alias: impl Into<String>) -> Result<(), Error> {
        self.inner.add_alias(realm, alias)
    }

    pub fn remove_realm(&self, realm: &str) -> Result<(), Error> {
        self.inner.remove_realm(realm)
    }

    pub fn add_role(&self, realm: &str, name: String, permissions: Vec<Permission>) -> Result<(), Error> {
        self.inner.add_role(realm, name, permissions)
    }

    pub fn remove_role(&self, realm: &str, name: &str) -> Result<(), Error> {
        self.inner.remove_role(realm, name)
    }

    pub fn close(&self) {
        self.inner.close();
    }

    pub fn websocket(&self, addr: std::net::SocketAddr) -> Result<Server, Error> {
        block_on(crate::async_::server::Server::websocket(self.inner.clone(), addr)).map(Server)
    }

    pub fn rawsocket(&self, addr: std::net::SocketAddr) -> Result<Server, Error> {
        block_on(crate::async_::server::Server::rawsocket(self.inner.clone(), addr)).map(Server)
    }
}

pub struct Server(crate::async_::server::Server);

impl Server {
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.0.local_addr
    }

    pub fn close(&self) {
        self.0.close();
    }
}
