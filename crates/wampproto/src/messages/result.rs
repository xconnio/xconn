use crate::messages;
use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;
use std::any::Any;
use std::collections::HashMap;
use std::option::Option;

pub const MESSAGE_TYPE_RESULT: Value = Value::Int(50);
pub const MESSAGE_NAME_RESULT: &str = "RESULT";

const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 5,
    name: MESSAGE_NAME_RESULT,
};

#[derive(Debug)]
pub struct Result_ {
    pub request_id: i64,
    pub details: HashMap<String, Value>,
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
}

impl Message for Result_ {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_RESULT
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            50,
            [
                ("request_id", Value::Int(self.request_id)),
                ("details", Value::Dict(self.details.clone())),
                ("args", self.args.clone().map(Value::List).unwrap_or(Value::Null)),
                ("kwargs", self.kwargs.clone().map(Value::Dict).unwrap_or(Value::Null)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if data.len() == VALIDATION_SPEC.min_length {
            if let [_, Value::Int(request_id), Value::Dict(details)] = &data[..] {
                Ok(Box::new(Result_ {
                    request_id: *request_id,
                    details: details.clone(),
                    args: None,
                    kwargs: None,
                }))
            } else {
                Err(VALIDATION_SPEC.err_invalid_message())
            }
        } else if data.len() == VALIDATION_SPEC.max_length {
            if let [
                _,
                Value::Int(request_id),
                Value::Dict(details),
                Value::List(args),
                Value::Dict(kwargs),
            ] = &data[..]
            {
                Ok(Box::new(Result_ {
                    request_id: *request_id,
                    details: details.clone(),
                    args: Some(args.clone()),
                    kwargs: Some(kwargs.clone()),
                }))
            } else {
                Err(VALIDATION_SPEC.err_invalid_message())
            }
        } else if let [_, Value::Int(request_id), Value::Dict(details), Value::List(args)] = &data[..] {
            Ok(Box::new(Result_ {
                request_id: *request_id,
                details: details.clone(),
                args: Some(args.clone()),
                kwargs: None,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
