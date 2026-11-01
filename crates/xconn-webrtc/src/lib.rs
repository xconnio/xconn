//! Reusable WebRTC data transport. WAMP integration is an optional layer.
pub mod transport;

#[cfg(feature = "wamp")]
pub mod wamp;
pub use transport::*;

#[cfg(feature = "rpc-streams")]
mod rpc_streams;
