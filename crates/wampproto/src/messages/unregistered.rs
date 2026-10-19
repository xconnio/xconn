use crate::messages;
use std::any::Any;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_UNREGISTERED: Value = Value::Int(67);
pub const MESSAGE_NAME_UNREGISTERED: &str = "UNREGISTERED";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 2,
    max_length: 2,
    name: MESSAGE_NAME_UNREGISTERED,
};

#[derive(Debug)]
pub struct Unregistered {
    pub request_id: i64,
}

impl Message for Unregistered {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_UNREGISTERED
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(67, [("request_id", Value::Int(self.request_id))])
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id)] = &data[..] {
            Ok(Box::new(Unregistered {
                request_id: *request_id,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
