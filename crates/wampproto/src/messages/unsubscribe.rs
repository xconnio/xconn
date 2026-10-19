use crate::messages;
use std::any::Any;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_UNSUBSCRIBE: Value = Value::Int(34);
pub const MESSAGE_NAME_UNSUBSCRIBE: &str = "UNSUBSCRIBE";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_UNSUBSCRIBE,
};

#[derive(Debug)]
pub struct Unsubscribe {
    pub request_id: i64,
    pub subscription_id: i64,
}

impl Message for Unsubscribe {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_UNSUBSCRIBE
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            34,
            [
                ("request_id", Value::Int(self.request_id)),
                ("subscription_id", Value::Int(self.subscription_id)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id), Value::Int(subscription_id)] = &data[..] {
            Ok(Box::new(Unsubscribe {
                request_id: *request_id,
                subscription_id: *subscription_id,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
