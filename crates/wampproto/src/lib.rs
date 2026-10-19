//! Sans-I/O WAMP protocol components for clients and routers.
//!
//! This crate owns message validation, codecs, authentication, and routing state.
//! The host application owns network I/O, scheduling, and connection lifetimes.
pub mod acceptor;
pub mod authenticators;
pub mod broker;
pub mod dealer;
pub mod idgen;
pub mod joiner;
pub mod messages;
pub mod options;
pub mod routing;
pub mod serializers;
pub mod transports;
pub mod types;
pub mod uris;

mod roles;
