use crate::messages;
use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;
use std::any::Any;
use std::collections::HashMap;

pub const MESSAGE_TYPE_REGISTER: Value = Value::Int(64);
pub const MESSAGE_NAME_REGISTER: &str = "REGISTER";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 4,
    max_length: 4,
    name: MESSAGE_NAME_REGISTER,
};

#[derive(Debug)]
pub struct Register {
    pub request_id: i64,
    pub options: HashMap<String, Value>,
    pub procedure: String,
}

impl Message for Register {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_REGISTER
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            64,
            [
                ("request_id", Value::Int(self.request_id)),
                ("options", Value::Dict(self.options.clone())),
                ("procedure", Value::Str(self.procedure.clone())),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(request_id), Value::Dict(options), Value::Str(procedure)] = &data[..] {
            Ok(Box::new(Register {
                request_id: *request_id,
                options: options.clone(),
                procedure: procedure.clone(),
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
