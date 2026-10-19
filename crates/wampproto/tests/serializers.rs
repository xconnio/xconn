use wampproto::messages::types::Value;
use wampproto::serializers::{
    cbor::CBORSerializer, json::JSONSerializer, msgpack::MsgPackSerializer, serializer::Serializer,
};

fn serializers() -> Vec<Box<dyn Serializer>> {
    vec![
        Box::new(JSONSerializer {}),
        Box::new(MsgPackSerializer {}),
        Box::new(CBORSerializer {}),
    ]
}

#[test]
fn independently_encoded_messages_decode_and_reencode_in_every_format() {
    // Wire fixtures exercise dispatch and field ordering independently of constructors.
    let fixtures = [
        r#"[1,"realm",{"authid":"alice","authmethods":["ticket"],"authextra":{},"roles":{"caller":{}}}]"#,
        r#"[2,123,{"roles":{"broker":{}},"authid":"alice","authrole":"user"}]"#,
        r#"[3,{},"wamp.error.not_authorized"]"#,
        r#"[4,"ticket",{}]"#,
        r#"[5,"secret",{}]"#,
        r#"[6,{},"wamp.close.normal"]"#,
        r#"[8,48,1,{},"wamp.error.no_such_procedure"]"#,
        r#"[16,1,{},"topic"]"#,
        r#"[17,1,2]"#,
        r#"[32,1,{},"topic"]"#,
        r#"[33,1,2]"#,
        r#"[34,1,2]"#,
        r#"[35,1]"#,
        r#"[36,1,2,{}]"#,
        r#"[48,1,{},"procedure"]"#,
        r#"[49,1,{}]"#,
        r#"[50,1,{}]"#,
        r#"[64,1,{},"procedure"]"#,
        r#"[65,1,2]"#,
        r#"[66,1,2]"#,
        r#"[67,1]"#,
        r#"[68,1,2,{}]"#,
        r#"[69,1,{}]"#,
        r#"[70,1,{}]"#,
        r#"[16,9007199254740991,{"acknowledge":true},"topic",[null,true,-7,1.5,"hello",[1],{"nested":false}],{"key":"value"}]"#,
        r#"[48,1,{},"procedure",[] ,{"keyword":42}]"#,
        r#"[50,1,{},[42]]"#,
        r#"[8,48,1,{},"wamp.error.runtime_error",["failed"],{"code":42}]"#,
        r#"[36,1,2,{},[42],{"key":true}]"#,
        r#"[68,1,2,{},[42],{"key":true}]"#,
        r#"[70,1,{},[42],{"key":true}]"#,
    ];
    for fixture in fixtures {
        let values: Vec<Value> = serde_json::from_str(fixture).unwrap();
        let encodings = [
            serde_json::to_vec(&values).unwrap(),
            rmp_serde::to_vec(&values).unwrap(),
            serde_cbor::to_vec(&values).unwrap(),
        ];
        for (serializer, wire) in serializers().iter().zip(encodings) {
            let message = serializer
                .deserialize(wire)
                .unwrap_or_else(|err| panic!("{serializer:?}: {fixture}: {err}"));
            assert_eq!(message.marshal(), values, "{serializer:?}: {fixture}");
            let decoded = serializer
                .deserialize(serializer.serialize(message.as_ref()).unwrap())
                .unwrap();
            assert_eq!(decoded.marshal(), values);
        }
    }
}

#[test]
fn invalid_messages_are_errors_in_every_format() {
    for fixture in [
        "[]",
        "[999]",
        r#"["48",1,{},"proc"]"#,
        "[48]",
        r#"[48,"bad",{},"proc"]"#,
        r#"[48,1,[],"proc"]"#,
        r#"[48,1,{},42]"#,
        r#"[48,1,{},"proc",{},{}]"#,
        r#"[48,1,{},"proc",[],{},0]"#,
    ] {
        let values: Vec<Value> = serde_json::from_str(fixture).unwrap();
        let encodings = [
            serde_json::to_vec(&values).unwrap(),
            rmp_serde::to_vec(&values).unwrap(),
            serde_cbor::to_vec(&values).unwrap(),
        ];
        for (serializer, wire) in serializers().iter().zip(encodings) {
            assert!(serializer.deserialize(wire).is_err(), "{serializer:?}: {fixture}");
        }
    }
    for serializer in serializers() {
        assert!(serializer.deserialize(vec![]).is_err());
        assert!(serializer.deserialize(vec![0xff]).is_err());
    }
}

#[test]
fn known_wire_bytes_match_each_serializer() {
    let message = JSONSerializer {}.deserialize(b"[17,1,2]".to_vec()).unwrap();
    for (serializer, expected) in
        serializers()
            .iter()
            .zip([b"[17,1,2]".to_vec(), vec![0x93, 17, 1, 2], vec![0x83, 17, 1, 2]])
    {
        assert_eq!(serializer.serialize(message.as_ref()).unwrap(), expected);
    }
}
