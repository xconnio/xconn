//! Metadata and opaque payload adaptation for XConn's Go interfaces.
use super::client::*;
use std::collections::HashMap;
use wampproto::{
    messages::{message::Message, payload::PayloadMessage, types::Value},
    serializers::payload::OpaquePayload,
    types::SessionDetails,
};
use xconn_core::wampproto;

fn boolean(v: &Value) -> Result<bool> {
    if let Value::Bool(v) = v {
        Ok(*v)
    } else {
        Err("expected bool".into())
    }
}

fn get<'a>(d: &'a HashMap<String, Value>, name: &str) -> Result<&'a Value> {
    d.get(name).ok_or_else(|| format!("missing {name}"))
}

fn strings(v: &Value) -> Result<Vec<String>> {
    as_l(v)?.iter().map(|v| as_s(v).map(str::to_string)).collect()
}

pub(crate) fn details(d: &SessionDetails) -> Value {
    Value::Dict(HashMap::from([
        ("id".into(), Value::Int(d.id)),
        ("realm".into(), text(&d.realm)),
        ("authid".into(), text(&d.authid)),
        ("authrole".into(), text(&d.auth_role)),
        ("authmethod".into(), text(&d.auth_method)),
        ("static".into(), Value::Bool(d.static_serializer)),
        ("roles".into(), Value::Dict(d.roles.clone())),
        ("authextra".into(), Value::Dict(d.auth_extra.clone())),
        ("created_at".into(), text(&d.created_at)),
    ]))
}

fn read_details(v: &Value) -> Result<SessionDetails> {
    let d = as_d(v)?;
    Ok(SessionDetails::new(
        as_i(get(&d, "id")?)?,
        as_s(get(&d, "realm")?)?.into(),
        as_s(get(&d, "authid")?)?.into(),
        as_s(get(&d, "authrole")?)?.into(),
        boolean(get(&d, "static")?)?,
    )
    .with_authentication(
        as_s(get(&d, "authmethod")?)?.into(),
        as_d(get(&d, "authextra")?)?,
        as_d(get(&d, "roles")?)?,
        as_s(get(&d, "created_at")?)?.into(),
    ))
}

fn message(v: &Value) -> Result<Box<dyn Message>> {
    let p = as_l(v)?;

    if p.len() != 3 {
        return Err("invalid message packet".into());
    }

    let m = typed_message(as_l(&p[0])?)?;

    if matches!(p[1], Value::Null) {
        return Ok(m);
    }

    let id = u8::try_from(as_u(&p[1])?).map_err(|_| "invalid payload serializer")?;
    let data = if matches!(p[2], Value::Null) {
        vec![]
    } else {
        as_b(&p[2])?.to_vec()
    };
    Ok(Box::new(
        PayloadMessage::new(m, OpaquePayload::new(id, data).map_err(err)?).map_err(err)?,
    ))
}

pub(crate) fn call(method: &str, args: &[Value]) -> Result<Value> {
    let a = |i: usize| args.get(i).ok_or_else(|| format!("missing argument {i}"));

    match method {
        "now" => Ok(text(wampproto::authenticators::wampcra::utcnow())),
        "router_roles" => Ok(Value::Dict(wampproto::acceptor::router_roles())),
        "details_new" => {
            let mut d = as_d(a(0)?)?;
            d.insert("created_at".into(), text(wampproto::authenticators::wampcra::utcnow()));
            Ok(details(&read_details(&Value::Dict(d))?))
        }
        "select_auth" => Ok(text(
            wampproto::authenticators::server::select_auth_method(&strings(a(0)?)?, &strings(a(1)?)?).map_err(err)?,
        )),
        "encode_packet" => {
            let m = message(a(1)?)?;
            let wire = m.marshal();
            let codec = super::codec(serializer_id(a(0)?)?)?;

            if matches!(codec, wampproto::serializers::codec::Codec::Json)
                && wire.iter().any(wampproto::serializers::payload::contains_binary)
            {
                return Err("JSON does not support binary values".into());
            }

            Ok(bytes(
                codec.encode_preserving_integers(&Value::List(wire)).map_err(err)?,
            ))
        }
        _ => Err(format!("unknown client adapter operation: {method}")),
    }
}
