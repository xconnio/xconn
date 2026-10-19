use crate::messages;
use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;
use std::any::Any;
use std::collections::HashMap;

pub const MESSAGE_TYPE_SUBSCRIBE: Value = Value::Int(32);
pub const MESSAGE_NAME_SUBSCRIBE: &str = "SUBSCRIBE";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 4,
    max_length: 4,
    name: MESSAGE_NAME_SUBSCRIBE,
};

#[derive(Debug)]
pub struct Subscribe {
    pub request_id: i64,
    pub options: HashMap<String, Value>,
    pub topic: String,
}

impl Message for Subscribe {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_SUBSCRIBE
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            32,
            [
                ("request_id", Value::Int(self.request_id)),
                ("options", Value::Dict(self.options.clone())),
                ("topic", Value::Str(self.topic.clone())),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id), Value::Dict(options), Value::Str(topic)] = &data[..] {
            Ok(Box::new(Subscribe {
                request_id: *request_id,
                options: options.clone(),
                topic: topic.clone(),
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
