use crate::messages;
use messages::message::Message;
use messages::types::{Error as XError, Value};
use messages::validator::ValidationSpec;
use std::any::Any;
use std::collections::HashMap;
use std::option::Option;

pub const MESSAGE_TYPE_ERROR: Value = Value::Int(8);
pub const MESSAGE_NAME_ERROR: &str = "ERROR";

const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 5,
    max_length: 7,
    name: MESSAGE_NAME_ERROR,
};

#[derive(Debug)]
pub struct Error {
    pub message_type: Value,
    pub request_id: i64,
    pub options: HashMap<String, Value>,
    pub uri: String,
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
}

impl Message for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_ERROR
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            8,
            [
                ("message_type", self.message_type.clone()),
                ("request_id", Value::Int(self.request_id)),
                ("details", Value::Dict(self.options.clone())),
                ("uri", Value::Str(self.uri.clone())),
                ("args", self.args.clone().map(Value::List).unwrap_or(Value::Null)),
                ("kwargs", self.kwargs.clone().map(Value::Dict).unwrap_or(Value::Null)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, XError> {
        VALIDATION_SPEC.validate_message(&data)?;

        if data.len() == VALIDATION_SPEC.min_length {
            if let [
                _,
                Value::Int(message_type),
                Value::Int(request_id),
                Value::Dict(options),
                Value::Str(uri),
            ] = &data[..]
            {
                Ok(Box::new(Error {
                    message_type: Value::Int(message_type.to_owned()),
                    request_id: *request_id,
                    options: options.clone(),
                    uri: uri.clone(),
                    args: None,
                    kwargs: None,
                }))
            } else {
                Err(VALIDATION_SPEC.err_invalid_message())
            }
        } else if data.len() == VALIDATION_SPEC.max_length {
            if let [
                _,
                Value::Int(message_type),
                Value::Int(request_id),
                Value::Dict(options),
                Value::Str(uri),
                Value::List(args),
                Value::Dict(kwargs),
            ] = &data[..]
            {
                Ok(Box::new(Error {
                    message_type: Value::Int(message_type.to_owned()),
                    request_id: *request_id,
                    options: options.clone(),
                    uri: uri.clone(),
                    args: Some(args.clone()),
                    kwargs: Some(kwargs.clone()),
                }))
            } else {
                Err(VALIDATION_SPEC.err_invalid_message())
            }
        } else if let [
            _,
            Value::Int(message_type),
            Value::Int(request_id),
            Value::Dict(options),
            Value::Str(uri),
            Value::List(args),
        ] = &data[..]
        {
            Ok(Box::new(Error {
                message_type: Value::Int(message_type.to_owned()),
                request_id: *request_id,
                options: options.clone(),
                uri: uri.clone(),
                args: Some(args.clone()),
                kwargs: None,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
