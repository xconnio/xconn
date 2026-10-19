pub mod cbor;
pub mod codec;
pub mod helpers;
pub mod json;
pub mod msgpack;
pub mod serializer;

/// Payload serializer ID for unencoded binary data.
pub const NONE_SERIALIZER_ID: u8 = 0;
pub mod payload;
