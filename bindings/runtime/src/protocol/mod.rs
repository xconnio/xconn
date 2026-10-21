//! Internal adapters for XConn client and router host types.
mod client;
mod router;
pub mod values;
use wampproto::messages::{
    schema::{self, WireValue},
    types::Value,
};
use wampproto::serializers::codec::Codec;
use wampproto::transports::rawsocket::SerializerID;
use xconn_core::wampproto;

pub const ENCODE: u32 = 1;
pub const DECODE: u32 = 2;

fn codec(id: u32) -> Result<Codec, String> {
    match id {
        id if id == SerializerID::JSON as u32 => Ok(Codec::Json),
        id if id == SerializerID::MSGPACK as u32 => Ok(Codec::MsgPack),
        id if id == SerializerID::CBOR as u32 => Ok(Codec::Cbor),
        _ => Err("unknown serializer ID".into()),
    }
}

fn validate(value: &Value) -> Result<(), String> {
    let code = value
        .sequence()
        .and_then(|values| values.first().and_then(WireValue::integer))
        .ok_or("expected WAMP message array with integer code")?;
    let spec = schema::schema(code).ok_or("unknown WAMP message type")?;
    spec.parse(value).map(|_| ()).map_err(|errors| errors.join("; "))
}

fn binary(value: &Value) -> bool {
    wampproto::serializers::payload::contains_binary(value)
}

pub fn dispatch(operation: u32, serializer: u32, input: &[u8]) -> Result<Vec<u8>, String> {
    let codec = codec(serializer)?;

    match operation {
        ENCODE => {
            let value = values::decode(input)?;
            validate(&value)?;

            if matches!(codec, Codec::Json) && binary(&value) {
                return Err("JSON does not support binary values".into());
            }

            codec.encode(&value).map_err(|err| err.message)
        }
        DECODE => {
            let value: Value = codec.decode(input).map_err(|err| err.message)?;
            validate(&value)?;
            values::encode(&value)
        }
        _ => Err("unknown ABI operation".into()),
    }
}

pub fn call(input: Value) -> Result<Value, String> {
    client::call(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wampproto::messages::types::ByteArray;

    #[test]
    fn out_of_range_serializer_ids_are_rejected() {
        let id = Value::UInt(u64::from(u32::MAX) + 2);
        let decode = Value::List(vec![
            Value::Str("decode_message".into()),
            Value::List(vec![
                id,
                Value::Bytes(ByteArray(b"[6,{},\"wamp.close.normal\"]".to_vec())),
            ]),
        ]);
        assert_eq!(call(decode).unwrap_err(), "unknown serializer ID");
    }
}
