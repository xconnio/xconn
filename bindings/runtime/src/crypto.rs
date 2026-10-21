//! Host-value adaptation for the portable packet-encryption implementation.
use crate::{ByteArray, Result, Value};
use std::collections::HashMap;

pub(super) fn call(method: &str, args: &[Value]) -> Result<Value> {
    let bytes = |index| match args.get(index) {
        Some(Value::Bytes(value)) => Ok(value.0.as_slice()),
        _ => Err(format!("expected bytes at argument {index}")),
    };
    let value = match method {
        "crypto_key_pair" => {
            let pair = xconn_core::crypto::generate_key_pair().map_err(|error| error.to_string())?;
            return Ok(Value::Dict(HashMap::from([
                ("public".into(), Value::Bytes(ByteArray(pair.public.to_vec()))),
                ("private".into(), Value::Bytes(ByteArray(pair.private.to_vec()))),
            ])));
        }
        "crypto_shared_secret" => xconn_core::crypto::shared_secret(bytes(0)?, bytes(1)?).map(|secret| secret.to_vec()),
        "crypto_derive_key" => xconn_core::crypto::derive_key(bytes(0)?, bytes(1)?).map(|key| key.to_vec()),
        "crypto_encrypt" => xconn_core::crypto::encrypt(bytes(0)?, bytes(1)?, bytes(2)?),
        "crypto_decrypt" => xconn_core::crypto::decrypt(bytes(0)?, bytes(1)?, bytes(2)?),
        _ => return Err(format!("unknown crypto operation {method}")),
    }
    .map_err(|error| error.to_string())?;
    Ok(Value::Bytes(ByteArray(value)))
}
