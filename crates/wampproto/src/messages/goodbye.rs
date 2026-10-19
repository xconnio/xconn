use crate::messages;
use std::any::Any;
use std::collections::HashMap;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_GOODBYE: Value = Value::Int(6);
pub const MESSAGE_NAME_GOODBYE: &str = "GOODBYE";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_GOODBYE,
};

#[derive(Debug)]
pub struct Goodbye {
    pub details: HashMap<String, Value>,
    pub reason: String,
}

impl Message for Goodbye {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_GOODBYE
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            6,
            [
                ("details", Value::Dict(self.details.clone())),
                ("reason", Value::Str(self.reason.clone())),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Dict(details), Value::Str(reason)] = &data[..] {
            Ok(Box::new(Goodbye {
                reason: reason.clone(),
                details: details.clone(),
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
