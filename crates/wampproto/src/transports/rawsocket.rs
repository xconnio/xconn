use crate::messages::types::Error;

pub const MAGIC: u8 = 0x7F;
pub const PROTOCOL_MAX_MSG_SIZE: usize = 1 << 24;
pub const DEFAULT_MAX_MSG_SIZE: usize = 1 << 20;
pub const MAX_MESSAGE_LENGTH: usize = (1 << 24) - 1;

#[derive(Debug, Clone, Copy)]
pub enum SerializerID {
    JSON = 1,
    MSGPACK = 2,
    CBOR = 3,
}

impl SerializerID {
    pub fn from_u8(value: u8) -> Option<SerializerID> {
        match value {
            1 => Some(SerializerID::JSON),
            2 => Some(SerializerID::MSGPACK),
            3 => Some(SerializerID::CBOR),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Message {
    Wamp = 0,
    Ping = 1,
    Pong = 2,
}

impl Message {
    pub fn from_u8(value: u8) -> Option<Message> {
        match value {
            0 => Some(Message::Wamp),
            1 => Some(Message::Ping),
            2 => Some(Message::Pong),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Handshake {
    serializer_id: SerializerID,
    max_message_size: usize,
}

impl Handshake {
    pub fn new(serializer: SerializerID, max_message_size: usize) -> Self {
        Handshake {
            serializer_id: serializer,
            max_message_size,
        }
    }

    pub fn serializer_id(&self) -> SerializerID {
        self.serializer_id
    }

    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }
}

pub fn send_handshake(hs: &Handshake) -> Result<Vec<u8>, Error> {
    if hs.max_message_size() > PROTOCOL_MAX_MSG_SIZE {
        return Err(Error::new("max_message_size must not be more than 16 megabytes"));
    }

    if hs.max_message_size() < 512 || !hs.max_message_size().is_power_of_two() {
        return Err(Error::new("max_message_size must be a power of 2 and >= 512"));
    }

    let log2 = hs.max_message_size().ilog2() as u8;

    let b1 = ((log2 - 9) << 4) | ((hs.serializer_id() as u8) & 0x0F);
    Ok(vec![MAGIC, b1, 0x00, 0x00])
}

pub fn receive_handshake(data: &[u8]) -> Result<Handshake, Error> {
    if data.len() != 4 {
        return Err(Error::new(format!(
            "expected 4 bytes for handshake response, got {}",
            data.len()
        )));
    }

    if data[0] != MAGIC {
        return Err(Error::new(format!("expected MAGIC, got {}", data[0])));
    }

    if data[2] != 0x00 || data[3] != 0x00 {
        return Err(Error::new(format!(
            "expected 0x00 for third and fourth byte, got {} and {}",
            data[2], data[3]
        )));
    }

    if let Some(serializer) = SerializerID::from_u8(data[1] & 0x0F) {
        let size_shift = (data[1] >> 4) + 9;
        let max_message_size = 1 << size_shift;

        Ok(Handshake::new(serializer, max_message_size))
    } else {
        Err(Error::new("got invalid serializer byte"))
    }
}

#[derive(Debug)]
pub struct MessageHeader {
    kind: Message,
    length: usize,
}

impl MessageHeader {
    pub fn new(kind: Message, length: usize) -> Self {
        MessageHeader { kind, length }
    }

    pub fn kind(&self) -> Message {
        self.kind
    }

    pub fn length(&self) -> usize {
        self.length
    }
}

pub fn send_message_header(header: &MessageHeader) -> Vec<u8> {
    let bytes = int_to_bytes(header.length());
    vec![header.kind as u8, bytes[0], bytes[1], bytes[2]]
}

/// Encode a header without silently truncating a length above the wire limit.
pub fn try_send_message_header(header: &MessageHeader) -> Result<Vec<u8>, Error> {
    encode_message_length(header.length())?;
    Ok(send_message_header(header))
}

pub fn encode_message_length(length: usize) -> Result<[u8; 3], Error> {
    if length > MAX_MESSAGE_LENGTH {
        return Err(Error::new("message length exceeds the 24-bit wire limit"));
    }

    Ok(int_to_bytes(length))
}

pub fn decode_message_length(bytes: &[u8]) -> Result<usize, Error> {
    if bytes.len() != 3 {
        return Err(Error::new("expected 3 bytes for message length"));
    }

    Ok(bytes_to_int(bytes))
}

pub fn receive_message_header(data: &[u8]) -> Result<MessageHeader, Error> {
    if data.len() != 4 {
        return Err(Error::new("expected 4 bytes for message header"));
    }

    if let Some(kind) = Message::from_u8(data[0]) {
        let length = bytes_to_int(&data[1..4]);
        Ok(MessageHeader::new(kind, length))
    } else {
        Err(Error::new("received invalid message type"))
    }
}

fn int_to_bytes(i: usize) -> [u8; 3] {
    [((i >> 16) & 0xFF) as u8, ((i >> 8) & 0xFF) as u8, (i & 0xFF) as u8]
}

fn bytes_to_int(b: &[u8]) -> usize {
    b.iter().fold(0, |acc, &byte| (acc << 8) | byte as usize)
}
