use crate::messages;
use std::any::Any;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_UNREGISTER: Value = Value::Int(66);
pub const MESSAGE_NAME_UNREGISTER: &str = "UNREGISTER";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_UNREGISTER,
};

#[derive(Debug)]
pub struct Unregister {
    pub request_id: i64,
    pub registration_id: i64,
}

impl Message for Unregister {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_UNREGISTER
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            66,
            [
                ("request_id", Value::Int(self.request_id)),
                ("registration_id", Value::Int(self.registration_id)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id), Value::Int(registration_id)] = &data[..] {
            Ok(Box::new(Unregister {
                request_id: *request_id,
                registration_id: *registration_id,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
