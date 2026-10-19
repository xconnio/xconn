use crate::messages;

use crate::messages::types::Error;
use crate::serializers::codec::Codec;
use crate::serializers::helpers::to_message;
use crate::serializers::serializer::Serializer;
use messages::message::Message;

#[derive(Debug, Clone)]
pub struct CBORSerializer {}

impl Serializer for CBORSerializer {
    fn serialize(&self, message: &dyn Message) -> Result<Vec<u8>, Error> {
        Codec::Cbor
            .encode(&message.marshal())
            .map_err(|_| Error::new(format!("failed to serialize message {:?}", message.message_type())))
    }

    fn deserialize(&self, payload: Vec<u8>) -> Result<Box<dyn Message>, Error> {
        let raw = Codec::Cbor
            .decode(&payload)
            .map_err(|e| Error::new(format!("failed to deserialize message {e}")))?;
        to_message(raw)
    }

    fn is_static(&self) -> bool {
        false
    }
}
