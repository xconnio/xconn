//! Payload envelopes shared by language bindings and opaque RPC routing.
use super::codec::Codec;
use crate::messages::types::{ByteArray, Error, Value};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Payload {
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
}

fn codec(id: u8) -> Result<Codec, Error> {
    match id {
        1 => Ok(Codec::Json),
        2 => Ok(Codec::MsgPack),
        3 => Ok(Codec::Cbor),
        _ => Err(Error::new("unknown payload serializer")),
    }
}

pub fn contains_binary(value: &Value) -> bool {
    match value {
        Value::Bytes(_) => true,
        Value::List(v) => v.iter().any(contains_binary),
        Value::Dict(v) => v.values().any(contains_binary),
        _ => false,
    }
}

pub fn encode(id: u8, payload: &Payload) -> Result<Option<Vec<u8>>, Error> {
    let args = payload.args.as_deref().unwrap_or_default();
    let kwargs = payload.kwargs.as_ref();

    if args.is_empty() && kwargs.is_none_or(HashMap::is_empty) {
        return Ok(None);
    }

    if id == 0 {
        return match args {
            [Value::Bytes(v)] if kwargs.is_none_or(HashMap::is_empty) => Ok(Some(v.0.clone())),
            _ => Err(Error::new("serializer 0 requires one binary argument")),
        };
    }

    let mut fields = vec![Value::List(args.to_vec())];

    if let Some(kwargs) = kwargs.filter(|v| !v.is_empty()) {
        fields.push(Value::Dict(kwargs.clone()));
    }

    let value = Value::List(fields);

    if id == 1 && contains_binary(&value) {
        return Err(Error::new("JSON does not support binary values"));
    }

    Ok(Some(codec(id)?.encode(&value)?))
}

pub fn decode(id: u8, data: &[u8]) -> Result<Payload, Error> {
    if id == 0 {
        return Ok(Payload {
            args: Some(vec![Value::Bytes(ByteArray(data.to_vec()))]),
            kwargs: None,
        });
    }

    let fields: Vec<Value> = codec(id)?.decode(data)?;

    if fields.len() > 2 {
        return Err(Error::new("too many payload fields"));
    }

    let args = match fields.first() {
        Some(Value::List(v)) => Some(v.clone()),
        None => None,
        _ => return Err(Error::new("payload args must be an array")),
    };
    let kwargs = match fields.get(1) {
        Some(Value::Dict(v)) => Some(v.clone()),
        None => None,
        _ => return Err(Error::new("payload kwargs must be a dictionary")),
    };
    Ok(Payload { args, kwargs })
}

/// Original serialized payload plus one validated decode, shared across routing copies.
#[derive(Clone, Debug)]
pub struct OpaquePayload {
    serializer: u8,
    data: Arc<[u8]>,
    decoded: Arc<Payload>,
}

impl OpaquePayload {
    pub fn new(serializer: u8, data: Vec<u8>) -> Result<Self, Error> {
        let decoded = Arc::new(decode(serializer, &data)?);
        Ok(Self {
            serializer,
            data: data.into(),
            decoded,
        })
    }

    pub fn serializer(&self) -> u8 {
        self.serializer
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn decoded(&self) -> &Payload {
        &self.decoded
    }
}
