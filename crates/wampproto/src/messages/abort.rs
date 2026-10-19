use crate::messages;
use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;
use std::any::Any;
use std::collections::HashMap;
use std::option::Option;

pub const MESSAGE_TYPE_ABORT: Value = Value::Int(3);
pub const MESSAGE_NAME_ABORT: &str = "ABORT";

const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 5,
    name: MESSAGE_NAME_ABORT,
};

#[derive(Debug)]
pub struct Abort {
    pub details: HashMap<String, Value>,
    pub reason: String,
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
}

impl Message for Abort {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_ABORT
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            3,
            [
                ("details", Value::Dict(self.details.clone())),
                ("reason", Value::Str(self.reason.clone())),
                ("args", self.args.clone().map(Value::List).unwrap_or(Value::Null)),
                ("kwargs", self.kwargs.clone().map(Value::Dict).unwrap_or(Value::Null)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if data.len() == VALIDATION_SPEC.min_length {
            if let [_, Value::Dict(details), Value::Str(reason)] = &data[..] {
                Ok(Box::new(Abort {
                    details: details.clone(),
                    reason: reason.clone(),
                    args: None,
                    kwargs: None,
                }))
            } else {
                Err(VALIDATION_SPEC.err_invalid_message())
            }
        } else if data.len() == VALIDATION_SPEC.max_length {
            if let [
                _,
                Value::Dict(details),
                Value::Str(reason),
                Value::List(args),
                Value::Dict(kwargs),
            ] = &data[..]
            {
                Ok(Box::new(Abort {
                    details: details.clone(),
                    reason: reason.clone(),
                    args: Some(args.clone()),
                    kwargs: Some(kwargs.clone()),
                }))
            } else {
                Err(VALIDATION_SPEC.err_invalid_message())
            }
        } else if let [_, Value::Dict(details), Value::Str(reason), Value::List(args)] = &data[..] {
            Ok(Box::new(Abort {
                details: details.clone(),
                reason: reason.clone(),
                args: Some(args.clone()),
                kwargs: None,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
