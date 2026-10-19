use crate::messages;
use std::any::Any;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_UNSUBSCRIBED: Value = Value::Int(35);
pub const MESSAGE_NAME_UNSUBSCRIBED: &str = "UNSUBSCRIBED";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 2,
    max_length: 2,
    name: MESSAGE_NAME_UNSUBSCRIBED,
};

#[derive(Debug)]
pub struct Unsubscribed {
    pub request_id: i64,
}

impl Message for Unsubscribed {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_UNSUBSCRIBED
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(35, [("request_id", Value::Int(self.request_id))])
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id)] = &data[..] {
            Ok(Box::new(Unsubscribed {
                request_id: *request_id,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
