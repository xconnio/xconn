//! Strict host payload access shared by invocations, events and call results.
use crate::{_IncomingRequest, RpcResult, Value};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadError {
    Missing,
    Null,
    Type,
    Range,
}

impl std::fmt::Display for PayloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Missing => "missing payload value",
            Self::Null => "payload value is null",
            Self::Type => "incorrect payload type",
            Self::Range => "payload value out of range",
        })
    }
}

impl std::error::Error for PayloadError {}

fn non_null(value: &Value) -> Result<&Value, PayloadError> {
    if matches!(value, Value::Null) {
        Err(PayloadError::Null)
    } else {
        Ok(value)
    }
}

fn integer_float(value: f64) -> Result<i64, PayloadError> {
    if !value.is_finite() || value.abs() > 9_007_199_254_740_991.0 {
        return Err(PayloadError::Range);
    }

    if value.fract() != 0.0 {
        return Err(PayloadError::Type);
    }

    Ok(value as i64)
}

fn int(value: &Value) -> Result<i64, PayloadError> {
    match non_null(value)? {
        Value::Int(value) => Ok(*value),
        Value::UInt(value) => i64::try_from(*value).map_err(|_| PayloadError::Range),
        Value::Float(value) => integer_float(*value),
        _ => Err(PayloadError::Type),
    }
}

fn uint(value: &Value) -> Result<u64, PayloadError> {
    match non_null(value)? {
        Value::UInt(value) => Ok(*value),
        Value::Int(value) => u64::try_from(*value).map_err(|_| PayloadError::Range),
        Value::Float(value) => u64::try_from(integer_float(*value)?).map_err(|_| PayloadError::Range),
        _ => Err(PayloadError::Type),
    }
}

fn float(value: &Value) -> Result<f64, PayloadError> {
    let result = match non_null(value)? {
        Value::Int(value) => *value as f64,
        Value::UInt(value) => *value as f64,
        Value::Float(value) => *value,
        _ => return Err(PayloadError::Type),
    };

    if result.is_finite() {
        Ok(result)
    } else {
        Err(PayloadError::Range)
    }
}

fn str(value: &Value) -> Result<&str, PayloadError> {
    match non_null(value)? {
        Value::Str(value) => Ok(value),
        _ => Err(PayloadError::Type),
    }
}

fn bool(value: &Value) -> Result<bool, PayloadError> {
    match non_null(value)? {
        Value::Bool(value) => Ok(*value),
        _ => Err(PayloadError::Type),
    }
}

fn bytes(value: &Value) -> Result<&[u8], PayloadError> {
    match non_null(value)? {
        Value::Bytes(value) => Ok(value.as_slice()),
        _ => Err(PayloadError::Type),
    }
}

fn list(value: &Value) -> Result<&[Value], PayloadError> {
    match non_null(value)? {
        Value::List(value) => Ok(value),
        _ => Err(PayloadError::Type),
    }
}

fn dict(value: &Value) -> Result<&HashMap<String, Value>, PayloadError> {
    match non_null(value)? {
        Value::Dict(value) => Ok(value),
        _ => Err(PayloadError::Type),
    }
}

pub trait Payload {
    fn payload_args(&self) -> &[Value];

    fn payload_kwargs(&self) -> Option<&HashMap<String, Value>>;

    fn arg(&self, index: usize) -> Result<&Value, PayloadError> {
        self.payload_args().get(index).ok_or(PayloadError::Missing)
    }

    fn kwarg(&self, key: &str) -> Result<&Value, PayloadError> {
        self.payload_kwargs()
            .and_then(|values| values.get(key))
            .ok_or(PayloadError::Missing)
    }

    fn arg_str(&self, index: usize) -> Result<&str, PayloadError> {
        str(self.arg(index)?)
    }

    fn arg_str_or<'a>(&'a self, index: usize, fallback: &'a str) -> &'a str {
        self.arg_str(index).unwrap_or(fallback)
    }

    fn arg_bool(&self, index: usize) -> Result<bool, PayloadError> {
        bool(self.arg(index)?)
    }

    fn arg_bool_or(&self, index: usize, fallback: bool) -> bool {
        self.arg_bool(index).unwrap_or(fallback)
    }

    fn arg_int(&self, index: usize) -> Result<i64, PayloadError> {
        int(self.arg(index)?)
    }

    fn arg_int_or(&self, index: usize, fallback: i64) -> i64 {
        self.arg_int(index).unwrap_or(fallback)
    }

    fn arg_uint(&self, index: usize) -> Result<u64, PayloadError> {
        uint(self.arg(index)?)
    }

    fn arg_uint_or(&self, index: usize, fallback: u64) -> u64 {
        self.arg_uint(index).unwrap_or(fallback)
    }

    fn arg_float(&self, index: usize) -> Result<f64, PayloadError> {
        float(self.arg(index)?)
    }

    fn arg_float_or(&self, index: usize, fallback: f64) -> f64 {
        self.arg_float(index).unwrap_or(fallback)
    }

    fn arg_bytes(&self, index: usize) -> Result<&[u8], PayloadError> {
        bytes(self.arg(index)?)
    }

    fn arg_bytes_or<'a>(&'a self, index: usize, fallback: &'a [u8]) -> &'a [u8] {
        self.arg_bytes(index).unwrap_or(fallback)
    }

    fn arg_list(&self, index: usize) -> Result<&[Value], PayloadError> {
        list(self.arg(index)?)
    }

    fn arg_list_or<'a>(&'a self, index: usize, fallback: &'a [Value]) -> &'a [Value] {
        self.arg_list(index).unwrap_or(fallback)
    }

    fn arg_dict(&self, index: usize) -> Result<&HashMap<String, Value>, PayloadError> {
        dict(self.arg(index)?)
    }

    fn arg_dict_or<'a>(&'a self, index: usize, fallback: &'a HashMap<String, Value>) -> &'a HashMap<String, Value> {
        self.arg_dict(index).unwrap_or(fallback)
    }

    fn kwarg_str(&self, key: &str) -> Result<&str, PayloadError> {
        str(self.kwarg(key)?)
    }

    fn kwarg_str_or<'a>(&'a self, key: &str, fallback: &'a str) -> &'a str {
        self.kwarg_str(key).unwrap_or(fallback)
    }

    fn kwarg_bool(&self, key: &str) -> Result<bool, PayloadError> {
        bool(self.kwarg(key)?)
    }

    fn kwarg_bool_or(&self, key: &str, fallback: bool) -> bool {
        self.kwarg_bool(key).unwrap_or(fallback)
    }

    fn kwarg_int(&self, key: &str) -> Result<i64, PayloadError> {
        int(self.kwarg(key)?)
    }

    fn kwarg_int_or(&self, key: &str, fallback: i64) -> i64 {
        self.kwarg_int(key).unwrap_or(fallback)
    }

    fn kwarg_uint(&self, key: &str) -> Result<u64, PayloadError> {
        uint(self.kwarg(key)?)
    }

    fn kwarg_uint_or(&self, key: &str, fallback: u64) -> u64 {
        self.kwarg_uint(key).unwrap_or(fallback)
    }

    fn kwarg_float(&self, key: &str) -> Result<f64, PayloadError> {
        float(self.kwarg(key)?)
    }

    fn kwarg_float_or(&self, key: &str, fallback: f64) -> f64 {
        self.kwarg_float(key).unwrap_or(fallback)
    }

    fn kwarg_bytes(&self, key: &str) -> Result<&[u8], PayloadError> {
        bytes(self.kwarg(key)?)
    }

    fn kwarg_bytes_or<'a>(&'a self, key: &str, fallback: &'a [u8]) -> &'a [u8] {
        self.kwarg_bytes(key).unwrap_or(fallback)
    }

    fn kwarg_list(&self, key: &str) -> Result<&[Value], PayloadError> {
        list(self.kwarg(key)?)
    }

    fn kwarg_list_or<'a>(&'a self, key: &str, fallback: &'a [Value]) -> &'a [Value] {
        self.kwarg_list(key).unwrap_or(fallback)
    }

    fn kwarg_dict(&self, key: &str) -> Result<&HashMap<String, Value>, PayloadError> {
        dict(self.kwarg(key)?)
    }

    fn kwarg_dict_or<'a>(&'a self, key: &str, fallback: &'a HashMap<String, Value>) -> &'a HashMap<String, Value> {
        self.kwarg_dict(key).unwrap_or(fallback)
    }
}

impl Payload for _IncomingRequest {
    fn payload_args(&self) -> &[Value] {
        &self.args
    }

    fn payload_kwargs(&self) -> Option<&HashMap<String, Value>> {
        Some(&self.kwargs)
    }
}

impl Payload for RpcResult {
    fn payload_args(&self) -> &[Value] {
        self.args.as_deref().unwrap_or_default()
    }

    fn payload_kwargs(&self) -> Option<&HashMap<String, Value>> {
        self.kwargs.as_ref()
    }
}
