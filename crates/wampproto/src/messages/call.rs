use crate::messages;
use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;
use std::any::Any;
use std::collections::HashMap;
use std::option::Option;

pub const MESSAGE_TYPE_CALL: Value = Value::Int(48);
pub const MESSAGE_NAME_CALL: &str = "CALL";

const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 4,
    max_length: 6,
    name: MESSAGE_NAME_CALL,
};

#[derive(Debug)]
pub struct Call {
    pub request_id: i64,
    pub options: HashMap<String, Value>,
    pub procedure: String,
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
}

impl Message for Call {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_CALL
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            48,
            [
                ("request_id", Value::Int(self.request_id)),
                ("options", Value::Dict(self.options.clone())),
                ("procedure", Value::Str(self.procedure.clone())),
                ("args", self.args.clone().map(Value::List).unwrap_or(Value::Null)),
                ("kwargs", self.kwargs.clone().map(Value::Dict).unwrap_or(Value::Null)),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if data.len() == VALIDATION_SPEC.min_length {
            if let [_, Value::Int(request_id), Value::Dict(options), Value::Str(procedure)] = &data[..] {
                Ok(Box::new(Call {
                    request_id: *request_id,
                    options: options.clone(),
                    procedure: procedure.clone(),
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
                Value::Dict(options),
                Value::Str(procedure),
                Value::List(args),
                Value::Dict(kwargs),
            ] = &data[..]
            {
                Ok(Box::new(Call {
                    request_id: *request_id,
                    options: options.clone(),
                    procedure: procedure.clone(),
                    args: Some(args.clone()),
                    kwargs: Some(kwargs.clone()),
                }))
            } else {
                Err(VALIDATION_SPEC.err_invalid_message())
            }
        } else if let [
            _,
            Value::Int(request_id),
            Value::Dict(options),
            Value::Str(procedure),
            Value::List(args),
        ] = &data[..]
        {
            Ok(Box::new(Call {
                request_id: *request_id,
                options: options.clone(),
                procedure: procedure.clone(),
                args: Some(args.clone()),
                kwargs: None,
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
