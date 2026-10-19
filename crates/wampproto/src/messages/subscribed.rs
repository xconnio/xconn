use crate::messages;
use std::any::Any;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_SUBSCRIBED: Value = Value::Int(33);
pub const MESSAGE_NAME_SUBSCRIBED: &str = "SUBSCRIBED";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_SUBSCRIBED,
};

#[derive(Debug)]
pub struct Subscribed {
    pub request_id: i64,
    pub subscription_id: i64,
}

impl Message for Subscribed {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_SUBSCRIBED
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            33,
            [
                ("request_id", Value::Int(self.request_id)),
                ("subscription_id", Value::Int(self.subscription_id)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id), Value::Int(subscription_id)] = &data[..] {
            Ok(Box::new(Subscribed {
                request_id: *request_id,
                subscription_id: *subscription_id,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
