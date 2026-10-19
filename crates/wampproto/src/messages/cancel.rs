use crate::messages;
use std::any::Any;
use std::collections::HashMap;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_CANCEL: Value = Value::Int(49);
pub const MESSAGE_NAME_CANCEL: &str = "CANCEL";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_CANCEL,
};

#[derive(Debug)]
pub struct Cancel {
    pub request_id: i64,
    pub options: HashMap<String, Value>,
}

impl Message for Cancel {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_CANCEL
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            49,
            [
                ("request_id", Value::Int(self.request_id)),
                ("options", Value::Dict(self.options.clone())),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id), Value::Dict(options)] = &data[..] {
            Ok(Box::new(Cancel {
                request_id: *request_id,
                options: options.clone(),
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
