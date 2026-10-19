use crate::messages;
use std::any::Any;
use std::collections::HashMap;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_CHALLENGE: Value = Value::Int(4);
pub const MESSAGE_NAME_CHALLENGE: &str = "CHALLENGE";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_CHALLENGE,
};

#[derive(Debug)]
pub struct Challenge {
    pub auth_method: String,
    pub extra: HashMap<String, Value>,
}

impl Message for Challenge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_CHALLENGE
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            4,
            [
                ("authmethod", Value::Str(self.auth_method.clone())),
                ("extra", Value::Dict(self.extra.clone())),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Str(auth_method), Value::Dict(extra)] = &data[..] {
            Ok(Box::new(Challenge {
                auth_method: auth_method.clone(),
                extra: extra.clone(),
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
