use xconn_core::wampproto;
// Resource ownership and Go API adaptation around existing Rust primitives.
use base64::Engine as _;
use std::collections::HashMap;
use wampproto::authenticators::{
    anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator, cryptosign,
    cryptosign::CryptoSignAuthenticator, ticket::TicketAuthenticator, wampcra,
};
use wampproto::messages::{
    challenge::Challenge,
    message::Message,
    schema::{self, Rule, WireValue},
    types::{ByteArray, Error, Value},
};
use wampproto::transports::rawsocket as raw;

pub(crate) type Result<T> = std::result::Result<T, String>;

pub(crate) fn err(error: Error) -> String {
    error.message
}

pub(crate) fn bytes(v: Vec<u8>) -> Value {
    Value::Bytes(ByteArray(v))
}

pub(crate) fn list(v: Vec<Value>) -> Value {
    Value::List(v)
}

pub(crate) fn int(v: u64) -> Value {
    Value::UInt(v)
}

pub(crate) fn text(v: impl Into<String>) -> Value {
    Value::Str(v.into())
}

pub(crate) fn as_u(v: &Value) -> Result<u64> {
    match v {
        Value::Int(v) => u64::try_from(*v).map_err(|_| "negative integer".into()),
        Value::UInt(v) => Ok(*v),
        _ => Err("expected unsigned integer".into()),
    }
}

pub(crate) fn serializer_id(v: &Value) -> Result<u32> {
    u32::try_from(as_u(v)?).map_err(|_| "unknown serializer ID".into())
}

pub(crate) fn as_i(v: &Value) -> Result<i64> {
    v.integer().ok_or("expected integer".into())
}

pub(crate) fn as_s(v: &Value) -> Result<&str> {
    if let Value::Str(s) = v {
        Ok(s)
    } else {
        Err("expected string".into())
    }
}

pub(crate) fn as_b(v: &Value) -> Result<&[u8]> {
    if let Value::Bytes(b) = v {
        Ok(&b.0)
    } else {
        Err("expected bytes".into())
    }
}

pub(crate) fn as_l(v: &Value) -> Result<Vec<Value>> {
    if let Value::List(v) = v {
        Ok(v.clone())
    } else {
        Err("expected array".into())
    }
}

pub(crate) fn as_d(v: &Value) -> Result<HashMap<String, Value>> {
    match v {
        Value::Dict(v) => Ok(v.clone()),
        Value::Null => Ok(HashMap::new()),
        _ => Err("expected dictionary".into()),
    }
}

// Normalize only protocol integer fields, preserving signedness in application payloads.
pub(crate) fn typed_message(wire: Vec<Value>) -> Result<Box<dyn Message>> {
    wampproto::serializers::helpers::to_typed_message(wire).map_err(err)
}

pub fn call(input: Value) -> Result<Value> {
    let input = as_l(&input)?;
    let method = as_s(input.first().ok_or("missing operation")?)?;
    let args = as_l(input.get(1).unwrap_or(&Value::Null))?;
    let a = |index: usize| args.get(index).ok_or_else(|| format!("missing argument {index}"));

    if matches!(
        method,
        "crypto_key_pair" | "crypto_shared_secret" | "crypto_derive_key" | "crypto_encrypt" | "crypto_decrypt"
    ) {
        return crate::crypto::call(method, &args);
    }

    match method {
        "definitions" => Ok(list(
            schema::CODES
                .iter()
                .map(|code| {
                    let s = schema::schema(*code).unwrap();
                    Value::Dict(HashMap::from([
                        ("code".into(), int(*code as u64)),
                        ("name".into(), text(s.name)),
                        ("class".into(), text(s.class_name)),
                    ]))
                })
                .collect(),
        )),
        "parse" => {
            let s = schema::schema(as_i(a(0)?)?).ok_or("unknown message code")?;
            let fields = s.parse(a(1)?).map_err(|e| e.join("; "))?;
            Ok(Value::Dict(fields.into_iter().collect()))
        }
        "marshal" => {
            let s = schema::schema(as_i(a(0)?)?).ok_or("unknown message code")?;
            let fields = as_d(a(1)?)?;

            if s.fields.iter().any(|f| matches!(f.rule, Rule::Welcome)) {
                let mut wire = vec![int(s.code as u64)];

                for field in s.fields {
                    wire.push(
                        fields
                            .get(if matches!(field.rule, Rule::Welcome) {
                                "details"
                            } else {
                                field.name
                            })
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }

                Ok(list(wire))
            } else {
                Ok(s.marshal(&Value::Null, &fields.into_iter().collect()))
            }
        }
        "identify" => {
            let value = a(0)?;
            let code = value
                .sequence()
                .and_then(|v| v.first().and_then(WireValue::integer))
                .ok_or("invalid message")?;
            let s = schema::schema(code).ok_or("unknown message")?;
            s.parse(value).map_err(|e| e.join("; "))?;
            Ok(text(s.class_name))
        }
        "encode_message" => {
            let wire = call(list(vec![text("marshal"), list(vec![a(1)?.clone(), a(2)?.clone()])]))?;
            super::validate(&wire)?;
            let codec = super::codec(serializer_id(a(0)?)?)?;

            if matches!(codec, wampproto::serializers::codec::Codec::Json) && super::binary(&wire) {
                return Err("JSON does not support binary values".into());
            }

            Ok(bytes(codec.encode_preserving_integers(&wire).map_err(err)?))
        }
        "decode_message" => {
            let id = serializer_id(a(0)?)?;
            let codec = super::codec(id)?;
            let wire: Value = if id == 1 {
                codec.decode(as_b(a(1)?)?).map_err(err)?
            } else {
                codec
                    .decode::<wampproto::messages::types::UnsignedValue>(as_b(a(1)?)?)
                    .map_err(err)?
                    .0
            };
            let code = as_l(&wire)?
                .first()
                .and_then(WireValue::integer)
                .ok_or("invalid message code")?;
            let spec = schema::schema(code).ok_or("unknown message code")?;
            let fields = spec.parse(&wire).map_err(|e| e.join("; "))?;
            Ok(list(vec![
                text(spec.class_name),
                Value::Dict(fields.into_iter().collect()),
            ]))
        }
        "binary_metadata" => {
            let mut fields = as_d(a(0)?)?;
            fields.insert("x_payload_serializer".into(), a(1)?.clone());
            Ok(Value::Dict(fields))
        }
        "roles" => Ok(Value::Dict(wampproto::joiner::get_client_roles())),
        "generate_id" => Ok(int(wampproto::idgen::generate_session_id().map_err(err)? as u64)),
        "encode_wire" => {
            super::validate(a(1)?)?;
            let codec = super::codec(serializer_id(a(0)?)?)?;

            if matches!(codec, wampproto::serializers::codec::Codec::Json) && super::binary(a(1)?) {
                return Err("JSON does not support binary values".into());
            }

            Ok(bytes(codec.encode_preserving_integers(a(1)?).map_err(err)?))
        }
        "cbor_pair_encode" => Ok(bytes(
            super::codec(3)?
                .encode(&list(vec![a(0)?.clone(), a(1)?.clone()]))
                .map_err(err)?,
        )),
        "cbor_pair_decode" => {
            let values: Vec<Value> = super::codec(3)?.decode(as_b(a(0)?)?).map_err(err)?;

            if values.len() != 2 || !matches!(values[0], Value::List(_)) || !matches!(values[1], Value::Dict(_)) {
                return Err("expected CBOR [args, kwargs] pair".into());
            }

            Ok(list(values))
        }
        "payload_encode" => {
            let id = u8::try_from(as_u(a(0)?)?).map_err(|_| "invalid payload serializer")?;
            let args = match a(1)? {
                Value::Null => None,
                v => Some(as_l(v)?),
            };
            let kwargs = match a(2)? {
                Value::Null => None,
                v => Some(as_d(v)?),
            };
            Ok(
                wampproto::serializers::payload::encode(id, &wampproto::serializers::payload::Payload { args, kwargs })
                    .map_err(err)?
                    .map(bytes)
                    .unwrap_or(Value::Null),
            )
        }
        "payload_decode" => {
            let id = u8::try_from(as_u(a(0)?)?).map_err(|_| "invalid payload serializer")?;
            let result = wampproto::serializers::payload::decode(id, as_b(a(1)?)?).map_err(err)?;
            Ok(list(vec![
                result.args.map(list).unwrap_or(Value::Null),
                result.kwargs.map(Value::Dict).unwrap_or(Value::Null),
            ]))
        }
        "auth" => {
            let method = as_s(a(0)?)?;
            let authid = as_s(a(1)?)?;
            let credential = as_s(a(2)?)?;
            let auth: Box<dyn ClientAuthenticator> = match method {
                "anonymous" => Box::new(AnonymousAuthenticator::new(authid, HashMap::new())),
                "ticket" => Box::new(TicketAuthenticator::new(authid, credential, HashMap::new())),
                "wampcra" => Box::new(wampcra::WAMPCRAAuthenticator::new(authid, credential, HashMap::new())),
                "cryptosign" => {
                    Box::new(CryptoSignAuthenticator::try_new(authid, credential, HashMap::new()).map_err(err)?)
                }
                _ => return Err("unknown authentication method".into()),
            };
            let message = typed_message(as_l(a(3)?)?)?;
            let ch = message
                .as_any()
                .downcast_ref::<Challenge>()
                .ok_or("expected challenge")?;
            let mut ch = Challenge {
                auth_method: ch.auth_method.clone(),
                extra: ch.extra.clone(),
            };

            if method == "wampcra" {
                if matches!(ch.extra.get("salt"),Some(Value::Str(v))if v.is_empty()) {
                    ch.extra.remove("salt");
                }

                for (name, default) in [("iterations", 1000), ("keylen", 32)] {
                    if matches!(ch.extra.get("salt"), Some(Value::Str(_)))
                        && ch.extra.get(name).is_none_or(|v| v.integer() == Some(0))
                    {
                        ch.extra.insert(name.into(), Value::Int(default));
                    }
                }
            }

            Ok(list(auth.authenticate(&ch).map_err(err)?.marshal()))
        }
        "cra_sign" => Ok(text(
            wampcra::sign_cra_challenge(as_s(a(0)?)?, as_b(a(1)?)?).map_err(err)?,
        )),
        "cra_sign_bytes" => Ok(bytes(
            base64::engine::general_purpose::STANDARD
                .decode(wampcra::sign_cra_challenge(as_s(a(0)?)?, as_b(a(1)?)?).map_err(err)?)
                .map_err(|e| e.to_string())?,
        )),
        "cra_verify" => Ok(Value::Bool(wampcra::verify_wampcra_signature(
            as_s(a(0)?)?,
            as_s(a(1)?)?,
            as_b(a(2)?)?,
        ))),
        "cra_derive" => {
            let iterations = as_i(a(2)?)?;
            let length = as_i(a(3)?)?;
            let (iterations, length) = wampcra::validate_cra_parameters(
                if iterations == 0 { 1000 } else { iterations },
                if length == 0 { 32 } else { length },
            )
            .map_err(err)?;
            Ok(bytes(
                wampcra::derive_wamp_cra_key(as_s(a(1)?)?, as_s(a(0)?)?, iterations, length)
                    .map_err(err)?
                    .into_bytes(),
            ))
        }
        "cra_nonce" => Ok(text(wampcra::generate_wampcra_nonce().map_err(err)?)),
        "utc_now" => Ok(text(wampcra::utcnow())),
        "cra_challenge" => Ok(text(
            wampcra::generate_wampcra_challenge(as_i(a(0)?)?, as_s(a(1)?)?, as_s(a(2)?)?, as_s(a(3)?)?).map_err(err)?,
        )),
        "crypto_extra" => {
            let auth = CryptoSignAuthenticator::try_new(as_s(a(0)?)?, as_s(a(1)?)?, HashMap::new()).map_err(err)?;
            let mut extra = as_d(a(2)?)?;
            let public = auth.auth_extra().remove("pubkey").unwrap();

            if let Some(Value::Str(value)) = extra.get("pubkey")
                && value.as_str() != as_s(&public)?
            {
                return Err("provided pubkey does not correspond to the private key".into());
            }

            extra
                .entry("pubkey".into())
                .and_modify(|v| {
                    if matches!(v, Value::Null) {
                        *v = public.clone();
                    }
                })
                .or_insert(public);
            Ok(Value::Dict(extra))
        }
        "crypto_sign" => {
            let private = as_b(a(1)?)?;

            if private.len() != 64 {
                return Err("private key must contain 64 bytes".into());
            }

            Ok(text(
                cryptosign::sign_cryptosign_challenge(as_s(a(0)?)?, &private[..32]).map_err(err)?,
            ))
        }
        "crypto_verify" => {
            let signature = as_s(a(0)?)?;
            let decoded = hex::decode(signature).map_err(|e| e.to_string())?;

            if decoded.len() != 96 {
                return Err("signature length must be 96 bytes".into());
            }

            let key = as_b(a(1)?)?;

            if key.len() != 32 {
                return Err("public key must contain 32 bytes".into());
            }

            Ok(Value::Bool(cryptosign::verify_cryptosign_signature(signature, key)))
        }
        "crypto_challenge" => Ok(text(cryptosign::generate_cryptosign_challenge().map_err(err)?)),
        "crypto_keypair" => {
            let (public, private) = cryptosign::generate_cryptosign_keypair().map_err(err)?;
            Ok(list(vec![text(public), text(private)]))
        }
        "handshake_send" => {
            let serializer = raw::SerializerID::from_u8(u8::try_from(as_u(a(0)?)?).map_err(|_| "invalid serializer")?)
                .ok_or("invalid serializer")?;
            Ok(bytes(
                raw::send_handshake(&raw::Handshake::new(
                    serializer,
                    usize::try_from(as_u(a(1)?)?).map_err(|_| "invalid size")?,
                ))
                .map_err(err)?,
            ))
        }
        "handshake_receive" => {
            let hs = raw::receive_handshake(as_b(a(0)?)?).map_err(err)?;
            Ok(list(vec![
                int(hs.serializer_id() as u64),
                int(hs.max_message_size() as u64),
            ]))
        }
        "header_send" => {
            let kind = raw::Message::from_u8(u8::try_from(as_u(a(0)?)?).map_err(|_| "invalid message kind")?)
                .ok_or("invalid message kind")?;
            Ok(bytes(
                raw::try_send_message_header(&raw::MessageHeader::new(
                    kind,
                    usize::try_from(as_u(a(1)?)?).map_err(|_| "invalid length")?,
                ))
                .map_err(err)?,
            ))
        }
        "header_receive" => {
            let h = raw::receive_message_header(as_b(a(0)?)?).map_err(err)?;
            Ok(list(vec![int(h.kind() as u64), int(h.length() as u64)]))
        }
        "length_encode" => Ok(bytes(
            raw::encode_message_length(usize::try_from(as_u(a(0)?)?).map_err(|_| "invalid length")?)
                .map_err(err)?
                .to_vec(),
        )),
        "length_decode" => Ok(int(raw::decode_message_length(as_b(a(0)?)?).map_err(err)? as u64)),
        _ => super::router::call(method, &args),
    }
}
