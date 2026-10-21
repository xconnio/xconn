//! Conversion between host request fields and Rust values.
use crate::{Result, Value};
use std::collections::HashMap;
use xconn_core::{CallRequest, Result as RpcResult, WampError};

pub(super) fn session_fields(details: &xconn_core::SessionDetails) -> HashMap<String, Value> {
    HashMap::from([
        ("session_id".into(), Value::Int(details.id())),
        ("realm".into(), Value::Str(details.realm())),
        ("authid".into(), Value::Str(details.authid())),
        ("authrole".into(), Value::Str(details.auth_role())),
        ("authmethod".into(), Value::Str(details.auth_method().into())),
        ("authextra".into(), Value::Dict(details.auth_extra().clone())),
    ])
}

pub(super) fn dictionary(value: &Value) -> Result<HashMap<String, Value>> {
    match value {
        Value::Dict(v) => Ok(v.clone()),
        Value::Null => Ok(HashMap::new()),
        _ => Err("expected object".into()),
    }
}

pub(super) fn string_field(fields: &HashMap<String, Value>, key: &str) -> Result<String> {
    match fields.get(key) {
        Some(Value::Str(v)) => Ok(v.clone()),
        _ => Err(format!("expected string {key}")),
    }
}

pub(super) fn handle_field(fields: &HashMap<String, Value>, key: &str) -> Result<u64> {
    match fields.get(key) {
        Some(Value::UInt(v)) => Ok(*v),
        Some(Value::Int(v)) if *v >= 0 => Ok(*v as u64),
        _ => Err(format!("expected nonnegative integer {key}")),
    }
}

pub(super) fn request_options(fields: &HashMap<String, Value>) -> Result<HashMap<String, Value>> {
    dictionary(fields.get("options").unwrap_or(&Value::Null))
}

pub(super) fn request_args(fields: &HashMap<String, Value>) -> Result<Vec<Value>> {
    match fields.get("args") {
        Some(Value::List(v)) => Ok(v.clone()),
        None | Some(Value::Null) => Ok(Vec::new()),
        _ => Err("expected args array".into()),
    }
}

pub(super) fn request_kwargs(fields: &HashMap<String, Value>) -> Result<HashMap<String, Value>> {
    dictionary(fields.get("kwargs").unwrap_or(&Value::Null))
}

pub(super) fn object_value(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Dict(fields.into_iter().map(|(k, v)| (k.into(), v)).collect())
}

pub(super) fn wamp_error(error: Option<WampError>) -> Value {
    match error {
        None => Value::Null,
        Some(e) => object_value([
            ("uri", Value::Str(e.uri)),
            ("args", e.args.map(Value::List).unwrap_or(Value::Null)),
            ("kwargs", e.kwargs.map(Value::Dict).unwrap_or(Value::Null)),
        ]),
    }
}

pub(super) fn request(fields: &HashMap<String, Value>) -> Result<CallRequest> {
    let mut request = CallRequest::new(string_field(fields, "uri")?).options(request_options(fields)?);

    if let Some(value) = fields.get("args")
        && !matches!(value, Value::Null)
    {
        request = request.args(request_args(fields)?);
    }

    if let Some(value) = fields.get("kwargs")
        && !matches!(value, Value::Null)
    {
        request = request.kwargs(request_kwargs(fields)?);
    }

    Ok(request)
}

pub(super) fn call_response(response: RpcResult) -> Value {
    object_value([
        ("args", response.args.map(Value::List).unwrap_or(Value::Null)),
        ("kwargs", response.kwargs.map(Value::Dict).unwrap_or(Value::Null)),
        ("details", Value::Dict(response.details)),
        ("error", wamp_error(response.error)),
    ])
}

pub(super) fn connection_timeout(fields: &HashMap<String, Value>) -> Result<Option<std::time::Duration>> {
    let seconds = match fields.get("open_timeout") {
        Some(Value::Null) => return Ok(None),
        Some(Value::Float(seconds)) => *seconds,
        Some(Value::Int(seconds)) => *seconds as f64,
        Some(Value::UInt(seconds)) => *seconds as f64,
        None => return Ok(Some(std::time::Duration::from_secs(20))),
        _ => return Err("open_timeout must be a number or None".into()),
    };
    std::time::Duration::try_from_secs_f64(seconds)
        .map(Some)
        .map_err(|_| "invalid connection timeout".into())
}

pub(super) fn duration_field(
    fields: &HashMap<String, Value>,
    key: &str,
    default: Option<std::time::Duration>,
) -> Result<Option<std::time::Duration>> {
    let seconds = match fields.get(key) {
        None => return Ok(default),
        Some(Value::Null) => return Ok(None),
        Some(Value::Float(seconds)) => *seconds,
        Some(Value::Int(seconds)) => *seconds as f64,
        Some(Value::UInt(seconds)) => *seconds as f64,
        _ => return Err(format!("{key} must be a number or None")),
    };
    std::time::Duration::try_from_secs_f64(seconds)
        .map(Some)
        .map_err(|_| format!("invalid {key}"))
}
