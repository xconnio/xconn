use std::collections::HashMap;
use xconn_core::wampproto::{messages::types::ByteArray, serializers::codec::Codec};
use xconn_core::{Event, Invocation, Payload, PayloadError, Result as RpcResult, Value};

fn read(payload: &dyn Payload, kind: &str, keyword: bool) -> std::result::Result<Value, PayloadError> {
    if kind == "string" {
        return if keyword {
            payload.kwarg_str("value")
        } else {
            payload.arg_str(0)
        }
        .map(|value| Value::Str(value.to_owned()));
    }
    if kind == "bool" {
        return if keyword {
            payload.kwarg_bool("value")
        } else {
            payload.arg_bool(0)
        }
        .map(Value::Bool);
    }
    if kind == "int" {
        return if keyword {
            payload.kwarg_int("value")
        } else {
            payload.arg_int(0)
        }
        .map(Value::Int);
    }
    if kind == "uint" {
        return if keyword {
            payload.kwarg_uint("value")
        } else {
            payload.arg_uint(0)
        }
        .map(Value::UInt);
    }
    if kind == "float" {
        return if keyword {
            payload.kwarg_float("value")
        } else {
            payload.arg_float(0)
        }
        .map(Value::Float);
    }
    if kind == "bytes" {
        return if keyword {
            payload.kwarg_bytes("value")
        } else {
            payload.arg_bytes(0)
        }
        .map(|value| Value::Bytes(ByteArray(value.to_vec())));
    }
    if kind == "list" {
        return if keyword {
            payload.kwarg_list("value")
        } else {
            payload.arg_list(0)
        }
        .map(|value| Value::List(value.to_owned()));
    }
    if kind == "dict" {
        return if keyword {
            payload.kwarg_dict("value")
        } else {
            payload.arg_dict(0)
        }
        .map(|value| Value::Dict(value.to_owned()));
    }
    unreachable!()
}

#[test]
fn shared_payload_matrix() {
    let Value::List(cases) = Codec::Json
        .decode::<Value>(include_bytes!("fixtures/payload.json"))
        .unwrap()
    else {
        panic!()
    };
    for entry in cases {
        let Value::Dict(entry) = entry else { panic!() };
        let Value::Str(tag) = &entry["tag"] else { panic!() };
        let original = &entry["value"];
        let value = match tag.as_str() {
            "int" => {
                let Value::Str(value) = original else { panic!() };
                Value::Int(value.parse().unwrap())
            }
            "uint" => {
                let Value::Str(value) = original else { panic!() };
                Value::UInt(value.parse().unwrap())
            }
            "special" => {
                let Value::Str(value) = original else { panic!() };
                Value::Float(match value.as_str() {
                    "nan" => f64::NAN,
                    "infinity" => f64::INFINITY,
                    _ => f64::NEG_INFINITY,
                })
            }
            "bytes" => {
                let Value::List(values) = original else { panic!() };
                Value::Bytes(ByteArray(
                    values
                        .iter()
                        .map(|value| {
                            let Value::Int(value) = value else { panic!() };
                            *value as u8
                        })
                        .collect(),
                ))
            }
            _ => original.clone(),
        };
        let invocation = Invocation {
            args: vec![value.clone()],
            kwargs: HashMap::from([("value".into(), value.clone())]),
            details: HashMap::new(),
        };
        let event = Event {
            args: invocation.args.clone(),
            kwargs: invocation.kwargs.clone(),
            details: HashMap::new(),
        };
        let result = RpcResult {
            args: Some(invocation.args.clone()),
            kwargs: Some(invocation.kwargs.clone()),
            ..Default::default()
        };
        let Value::List(accepted) = &entry["accept"] else {
            panic!()
        };
        for payload in [&invocation as &dyn Payload, &event, &result] {
            for kind in ["string", "bool", "int", "uint", "float", "bytes", "list", "dict"] {
                for keyword in [false, true] {
                    let parsed = read(payload, kind, keyword);
                    assert_eq!(
                        parsed.is_ok(),
                        accepted.contains(&Value::Str(kind.into())),
                        "{:?} {kind}",
                        entry["name"]
                    );
                    if let Ok(parsed) = parsed {
                        match (&parsed, &value) {
                            (Value::Float(actual), Value::Int(expected)) => assert_eq!(*actual, *expected as f64),
                            (Value::Float(actual), Value::UInt(expected)) => assert_eq!(*actual, *expected as f64),
                            (Value::Int(actual), Value::Float(expected)) => assert_eq!(*actual as f64, *expected),
                            (Value::UInt(actual), Value::Float(expected)) => assert_eq!(*actual as f64, *expected),
                            (Value::UInt(actual), Value::Int(expected)) => assert_eq!(*actual, *expected as u64),
                            _ => assert_eq!(parsed, value),
                        }
                    }
                }
            }
            assert_eq!(payload.arg(1), Err(PayloadError::Missing));
            assert_eq!(payload.kwarg("absent"), Err(PayloadError::Missing));
            assert_eq!(payload.arg_str_or(1, "fallback"), "fallback");
            assert_eq!(payload.kwarg_str_or("absent", "fallback"), "fallback");
            assert!(!payload.arg_bool_or(1, false));
            assert!(!payload.kwarg_bool_or("absent", false));
            assert_eq!(payload.arg_int_or(1, 7), 7);
            assert_eq!(payload.kwarg_int_or("absent", 7), 7);
            assert_eq!(payload.arg_uint_or(1, 7), 7);
            assert_eq!(payload.kwarg_uint_or("absent", 7), 7);
            assert_eq!(payload.arg_float_or(1, 7.0), 7.0);
            assert_eq!(payload.kwarg_float_or("absent", 7.0), 7.0);
            assert_eq!(payload.arg_bytes_or(1, &[]), &[] as &[u8]);
            assert_eq!(payload.kwarg_bytes_or("absent", &[]), &[] as &[u8]);
            assert_eq!(payload.arg_list_or(1, &[]), &[]);
            assert_eq!(payload.kwarg_list_or("absent", &[]), &[]);
            assert_eq!(payload.arg_dict_or(1, &HashMap::new()), &HashMap::new());
            assert_eq!(payload.kwarg_dict_or("absent", &HashMap::new()), &HashMap::new());
        }
    }
    let event = Event {
        args: vec![Value::Null],
        kwargs: HashMap::new(),
        details: HashMap::new(),
    };
    assert_eq!(event.arg_str(0), Err(PayloadError::Null));
    assert_eq!(event.arg_str_or(0, "fallback"), "fallback");
}
