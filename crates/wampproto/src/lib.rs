//! Sans-I/O WAMP protocol components for clients and routers.
//!
//! This crate owns message validation, codecs, authentication, and routing state.
//! The host application owns network I/O, scheduling, and connection lifetimes.
pub mod messages;
pub mod options;
pub mod serializers;
pub mod uris;
