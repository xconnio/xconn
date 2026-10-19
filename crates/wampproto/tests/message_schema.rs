use std::collections::BTreeMap;

use wampproto::messages::{hello::Hello, message::Message, schema, types::Value};

#[test]
fn schema_reports_all_field_errors_in_wire_order() {
    let value = Value::List(vec![
        Value::Int(48),
        Value::Str("bad".into()),
        Value::Int(23),
        Value::Dict(Default::default()),
    ]);
    assert_eq!(
        schema::schema(48).unwrap().parse(&value).unwrap_err(),
        vec![
            "CALL: value at index 1 must be of type 'int' but was str",
            "CALL: value at index 2 must be of type 'dict' but was int",
            "CALL: value at index 3 must be of type 'string' but was dict",
        ]
    );
}

#[test]
fn typed_rust_parsers_use_shared_range_and_detail_validation() {
    use wampproto::messages::call::Call;
    for id in [-1, 0, (1_i64 << 53) + 1] {
        assert!(
            Call::parse(vec![
                Value::Int(48),
                Value::Int(id),
                Value::Dict(Default::default()),
                Value::Str("procedure".into())
            ])
            .is_err()
        );
    }
    let minimal = vec![
        Value::Int(1),
        Value::Str("realm".into()),
        Value::Dict(
            [(
                "roles".into(),
                Value::Dict([("caller".into(), Value::Dict(Default::default()))].into()),
            )]
            .into(),
        ),
    ];
    assert!(Hello::parse(minimal.clone()).is_ok());
    let fields = schema::schema(1).unwrap().parse(&Value::List(minimal)).unwrap();
    assert_eq!(fields["authid"], Value::Null);
    assert!(
        Hello::parse(vec![
            Value::Int(1),
            Value::Str("realm".into()),
            Value::Dict(Default::default())
        ])
        .is_err()
    );
}

#[test]
fn shared_marshaler_handles_keyword_only_and_abort_payloads() {
    let fields: BTreeMap<_, _> = [
        ("request_id".into(), Value::Int(1)),
        ("procedure".into(), Value::Str("procedure".into())),
        ("options".into(), Value::Dict(Default::default())),
        ("kwargs".into(), Value::Dict([("key".into(), Value::Int(42))].into())),
    ]
    .into();
    let wire = schema::schema(48).unwrap().marshal(&Value::Null, &fields);
    let expected: Vec<Value> = serde_json::from_str(r#"[48,1,{},"procedure",[],{"key":42}]"#).unwrap();
    assert_eq!(wire, Value::List(expected));
    let abort: Vec<Value> =
        serde_json::from_str(r#"[3,{},"wamp.error.not_authorized",["denied"],{"code":403}]"#).unwrap();
    let message = wampproto::serializers::helpers::to_message(abort.clone()).unwrap();
    assert_eq!(message.marshal(), abort);
}
