//! Value encoding shared by message serializers and language bindings.
//! This layer encodes data without parsing or normalizing WAMP messages.

use serde::{Serialize, de::DeserializeOwned};

use crate::messages::types::Error;

#[derive(Debug, Clone, Copy)]
pub enum Codec {
    Json,
    MsgPack,
    Cbor,
}

impl Codec {
    pub fn encode<T: Serialize>(self, value: &T) -> Result<Vec<u8>, Error> {
        match self {
            Self::Json => serde_json::to_vec(value).map_err(|e| Error::new(e.to_string())),
            Self::MsgPack => rmp_serde::to_vec(value).map_err(|e| Error::new(e.to_string())),
            Self::Cbor => serde_cbor::to_vec(value).map_err(|e| Error::new(e.to_string())),
        }
    }

    pub fn decode<T: DeserializeOwned>(self, payload: &[u8]) -> Result<T, Error> {
        match self {
            Self::Json => serde_json::from_slice(payload).map_err(|e| Error::new(e.to_string())),
            Self::MsgPack => {
                let mut decoder = rmp_serde::Deserializer::new(std::io::Cursor::new(payload));
                let value = T::deserialize(&mut decoder).map_err(|e| Error::new(e.to_string()))?;

                if decoder.position() != payload.len() as u64 {
                    return Err(Error::new("trailing data after MessagePack value"));
                }

                Ok(value)
            }
            Self::Cbor => serde_cbor::from_slice(payload).map_err(|e| Error::new(e.to_string())),
        }
    }
}

impl Codec {
    /// Preserve host signed/unsigned integer types where MessagePack has distinct tags.
    /// Other formats retain their normal encoding. Default compact encoding is unchanged.
    pub fn encode_preserving_integers(self, value: &crate::messages::types::Value) -> Result<Vec<u8>, Error> {
        if !matches!(self, Self::MsgPack) {
            return self.encode(value);
        }

        use crate::messages::types::Value;

        fn write(out: &mut Vec<u8>, value: &Value, depth: usize) -> Result<(), Error> {
            if depth > 64 {
                return Err(Error::new("value nesting exceeds 64"));
            }

            match value {
                Value::Int(v) => rmp::encode::write_i64(out, *v).map_err(|e| Error::new(e.to_string()))?,
                Value::UInt(v) => rmp::encode::write_u64(out, *v).map_err(|e| Error::new(e.to_string()))?,
                Value::List(values) => {
                    rmp::encode::write_array_len(
                        out,
                        u32::try_from(values.len()).map_err(|_| Error::new("array too large"))?,
                    )
                    .map_err(|e| Error::new(e.to_string()))?;

                    for value in values {
                        write(out, value, depth + 1)?;
                    }
                }
                Value::Dict(values) => {
                    rmp::encode::write_map_len(
                        out,
                        u32::try_from(values.len()).map_err(|_| Error::new("dictionary too large"))?,
                    )
                    .map_err(|e| Error::new(e.to_string()))?;

                    for (key, value) in values {
                        rmp::encode::write_str(out, key).map_err(|e| Error::new(e.to_string()))?;
                        write(out, value, depth + 1)?;
                    }
                }
                _ => out.extend(Codec::MsgPack.encode(value)?),
            }

            Ok(())
        }

        let mut out = Vec::new();
        write(&mut out, value, 0)?;
        Ok(out)
    }
}
