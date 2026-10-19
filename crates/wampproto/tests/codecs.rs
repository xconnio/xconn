use wampproto::messages::types::{ByteArray, Value};
use wampproto::serializers::codec::Codec;

#[test]
fn codecs_preserve_wire_fields_without_message_normalization() {
    for fixture in [
        r#"[1,"realm",{"roles":{"subscriber":{}}}]"#,
        r#"[2,123,{"roles":{"broker":{}}}]"#,
        r#"[3,{},"wamp.error.not_authorized",["denied"],{"code":403}]"#,
        r#"[[42],{"color":"orange"}]"#,
    ] {
        let expected: Vec<Value> = serde_json::from_str(fixture).unwrap();
        for codec in [Codec::Json, Codec::MsgPack, Codec::Cbor] {
            let encoded = codec.encode(&expected).unwrap();
            assert_eq!(codec.decode::<Vec<Value>>(&encoded).unwrap(), expected);
        }
    }
}

#[test]
fn binary_codecs_preserve_bytes_unsigned_integers_and_float_types() {
    let expected = vec![
        Value::UInt(u64::MAX),
        Value::Float(1.0),
        Value::Bytes(ByteArray(vec![0, 255])),
    ];
    for codec in [Codec::MsgPack, Codec::Cbor] {
        let payload = codec.encode(&expected).unwrap();
        assert_eq!(codec.decode::<Vec<Value>>(&payload).unwrap(), expected);
    }
}

#[test]
fn codecs_reject_truncated_and_trailing_data() {
    for (codec, valid) in [
        (Codec::Json, b"[17,1,2]".to_vec()),
        (Codec::MsgPack, vec![0x93, 17, 1, 2]),
        (Codec::Cbor, vec![0x83, 17, 1, 2]),
    ] {
        assert!(codec.decode::<Vec<Value>>(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(codec.decode::<Vec<Value>>(&trailing).is_err());
    }
}

#[test]
fn binary_data_is_not_coerced_to_text_even_when_it_is_valid_utf8() {
    use wampproto::messages::types::{ByteArray, Value};
    use wampproto::serializers::codec::Codec;
    let values = vec![
        Value::Str("hello".into()),
        Value::Bytes(ByteArray(b"hello".to_vec())),
        Value::Bytes(ByteArray(Vec::new())),
        Value::Str(String::new()),
        Value::UInt(u64::MAX),
        Value::Int(-1),
    ];
    for codec in [Codec::MsgPack, Codec::Cbor] {
        let encoded = codec.encode(&values).unwrap();
        let decoded: Vec<Value> = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }
    let values: Vec<Value> = Codec::Json.decode(br#"[1,1.0,-1,"hello",null,true]"#).unwrap();
    assert_eq!(
        values,
        vec![
            Value::Int(1),
            Value::Float(1.0),
            Value::Int(-1),
            Value::Str("hello".into()),
            Value::Null,
            Value::Bool(true)
        ]
    );
}

#[test]
fn language_profile_preserves_messagepack_signed_and_unsigned_integer_tags() {
    use wampproto::messages::types::UnsignedValue;
    let value = Value::List(vec![
        Value::Int(100),
        Value::UInt(100),
        Value::Dict(std::collections::HashMap::from([
            ("signed".into(), Value::Int(7)),
            ("unsigned".into(), Value::UInt(7)),
        ])),
    ]);
    let encoded = Codec::MsgPack.encode_preserving_integers(&value).unwrap();
    assert_eq!(encoded[0], 0x93);
    assert_eq!(encoded[1], 0xd3);
    assert_eq!(encoded[10], 0xcf);
    assert_eq!(Codec::MsgPack.decode::<UnsignedValue>(&encoded).unwrap().0, value);
    // The existing compact API and its default Rust integer convention remain available.
    let compact = Codec::MsgPack.encode(&Value::Int(100)).unwrap();
    assert_eq!(compact, vec![100]);
    assert_eq!(Codec::MsgPack.decode::<Value>(&compact).unwrap(), Value::Int(100));
}
