//! RPC messages can retain an original payload for a host's static serializer.
use super::{
    message::Message,
    schema,
    types::{Error, Value},
};
use crate::serializers::{helpers::to_message, payload::OpaquePayload};
use std::{any::Any, collections::HashMap};

pub struct PayloadMessage {
    inner: Box<dyn Message>,
    payload: OpaquePayload,
}

impl PayloadMessage {
    pub fn new(message: Box<dyn Message>, payload: OpaquePayload) -> Result<Self, Error> {
        let code =
            schema::WireValue::integer(&message.message_type()).ok_or_else(|| Error::new("invalid message type"))?;
        let metadata = match code {
            48 | 70 => "options",
            50 | 68 => "details",
            _ => return Err(Error::new("opaque payload is only supported for RPC messages")),
        };
        let spec = schema::schema(code).unwrap();
        let mut fields = spec
            .parse(&Value::List(message.marshal()))
            .map_err(|e| Error::new(e.join("; ")))?;
        let mut details = match fields.remove(metadata) {
            Some(Value::Dict(d)) => d,
            _ => HashMap::new(),
        };
        details.insert(
            "x_payload_serializer".into(),
            Value::Int(i64::from(payload.serializer())),
        );
        fields.insert(metadata.into(), Value::Dict(details));
        fields.insert(
            "args".into(),
            payload.decoded().args.clone().map(Value::List).unwrap_or(Value::Null),
        );
        fields.insert(
            "kwargs".into(),
            payload.decoded().kwargs.clone().map(Value::Dict).unwrap_or(Value::Null),
        );
        let Value::List(wire) = spec.marshal(&Value::Null, &fields) else {
            unreachable!()
        };
        Ok(Self {
            inner: to_message(wire)?,
            payload,
        })
    }
}

impl Message for PayloadMessage {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn message_type(&self) -> Value {
        self.inner.message_type()
    }

    fn marshal(&self) -> Vec<Value> {
        self.inner.marshal()
    }

    fn opaque_payload(&self) -> Option<&OpaquePayload> {
        Some(&self.payload)
    }

    fn parse(_: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        Err(Error::new("opaque payload requires host-provided bytes"))
    }
}
