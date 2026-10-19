use crate::messages;
use std::any::Any;
use std::collections::HashMap;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_AUTHENTICATE: Value = Value::Int(5);
pub const MESSAGE_NAME_AUTHENTICATE: &str = "AUTHENTICATE";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_AUTHENTICATE,
};

#[derive(Debug)]
pub struct Authenticate {
    pub signature: String,
    pub extra: HashMap<String, Value>,
}

impl Message for Authenticate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_AUTHENTICATE
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            5,
            [
                ("signature", Value::Str(self.signature.clone())),
                ("extra", Value::Dict(self.extra.clone())),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Str(signature), Value::Dict(extra)] = &data[..] {
            Ok(Box::new(Authenticate {
                signature: signature.clone(),
                extra: extra.clone(),
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
