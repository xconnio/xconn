use crate::messages;
use std::any::Any;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_PUBLISHED: Value = Value::Int(17);
pub const MESSAGE_NAME_PUBLISHED: &str = "PUBLISHED";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_PUBLISHED,
};

#[derive(Debug)]
pub struct Published {
    pub request_id: i64,
    pub publication_id: i64,
}

impl Message for Published {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_PUBLISHED
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            17,
            [
                ("request_id", Value::Int(self.request_id)),
                ("publication_id", Value::Int(self.publication_id)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id), Value::Int(publication_id)] = &data[..] {
            Ok(Box::new(Published {
                request_id: *request_id,
                publication_id: *publication_id,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
