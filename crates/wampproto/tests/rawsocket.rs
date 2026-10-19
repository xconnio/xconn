use wampproto::transports::rawsocket::{self, Handshake, Message, MessageHeader, SerializerID};

#[test]
fn handshake_wire_layout_covers_every_size_and_serializer() {
    for exponent in 9..=24 {
        for serializer in [SerializerID::JSON, SerializerID::MSGPACK, SerializerID::CBOR] {
            let expected = vec![0x7f, ((exponent - 9) << 4) | serializer as u8, 0, 0];
            let handshake = Handshake::new(serializer, 1 << exponent);
            assert_eq!(rawsocket::send_handshake(&handshake).unwrap(), expected);
            let decoded = rawsocket::receive_handshake(&expected).unwrap();
            assert_eq!(decoded.serializer_id() as u8, serializer as u8);
            assert_eq!(decoded.max_message_size(), 1 << exponent);
        }
    }
}

#[test]
fn invalid_handshakes_are_rejected() {
    for size in [0, 1, 256, 511, 513, (1 << 24) + 1] {
        assert!(rawsocket::send_handshake(&Handshake::new(SerializerID::JSON, size)).is_err());
    }
    for bytes in [
        vec![],
        vec![0x7f, 1, 0],
        vec![0x7f, 1, 0, 0, 0],
        vec![0, 1, 0, 0],
        vec![0x7f, 0, 0, 0],
        vec![0x7f, 4, 0, 0],
        vec![0x7f, 1, 1, 0],
        vec![0x7f, 1, 0, 1],
    ] {
        assert!(rawsocket::receive_handshake(&bytes).is_err(), "{bytes:?}");
    }
}

#[test]
fn headers_use_three_byte_big_endian_lengths_for_data_ping_and_pong() {
    for kind in [Message::Wamp, Message::Ping, Message::Pong] {
        for (length, bytes) in [
            (0, [0, 0, 0]),
            (255, [0, 0, 255]),
            (256, [0, 1, 0]),
            (0x123456, [0x12, 0x34, 0x56]),
            (0xffffff, [255, 255, 255]),
        ] {
            let wire = vec![kind as u8, bytes[0], bytes[1], bytes[2]];
            assert_eq!(rawsocket::send_message_header(&MessageHeader::new(kind, length)), wire);
            let decoded = rawsocket::receive_message_header(&wire).unwrap();
            assert_eq!(decoded.kind() as u8, kind as u8);
            assert_eq!(decoded.length(), length);
        }
    }
}

#[test]
fn malformed_headers_are_rejected() {
    for bytes in [
        vec![],
        vec![0, 0, 0],
        vec![0, 0, 0, 0, 0],
        vec![3, 0, 0, 0],
        vec![255, 0, 0, 0],
    ] {
        assert!(rawsocket::receive_message_header(&bytes).is_err());
    }
}

#[test]
fn checked_header_encoder_rejects_lengths_that_would_be_truncated() {
    assert!(rawsocket::try_send_message_header(&MessageHeader::new(Message::Wamp, 1 << 24)).is_err());
    assert!(rawsocket::encode_message_length(usize::MAX).is_err());
    for (length, expected) in [(0, [0, 0, 0]), (0x123456, [0x12, 0x34, 0x56]), (0xffffff, [255; 3])] {
        assert_eq!(rawsocket::encode_message_length(length).unwrap(), expected);
        assert_eq!(rawsocket::decode_message_length(&expected).unwrap(), length);
        assert_eq!(
            rawsocket::try_send_message_header(&MessageHeader::new(Message::Wamp, length)).unwrap(),
            vec![0, expected[0], expected[1], expected[2]]
        );
    }
    for bytes in [&[][..], &[0, 0], &[0, 0, 0, 0]] {
        assert!(rawsocket::decode_message_length(bytes).is_err());
    }
}
