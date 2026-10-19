#![doc = include_str!("../README.md")]

//! WAMP client and router runtime built on the sans-I/O `wampproto` engine.
//!
//! Async APIs live in [`async_`]; blocking APIs live in [`sync`].
#[cfg(feature = "async")]
pub mod async_;

#[cfg(feature = "sync")]
pub mod sync;

mod common;

pub use common::payload::{Payload, PayloadError};
pub use common::types::*;
// Keep std::result::Result unambiguous inside the runtime.
pub(crate) use common::types::Result as RpcResult;
pub use wampproto;
