use crate::messages;
use std::any::Any;
use std::collections::HashMap;

use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;

pub const MESSAGE_TYPE_WELCOME: Value = Value::Int(2);
pub const MESSAGE_NAME_WELCOME: &str = "WELCOME";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_WELCOME,
};

#[derive(Debug)]
pub struct Welcome {
    pub session_id: i64,
    pub realm: String,
    pub authid: String,
    pub auth_role: String,
    pub details: HashMap<String, Value>,
}

impl Message for Welcome {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_WELCOME
    }

    fn marshal(&self) -> Vec<Value> {
        vec![
            MESSAGE_TYPE_WELCOME,
            Value::Int(self.session_id),
            Value::Dict(self.details.clone()),
        ]
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Int(session_id), Value::Dict(details)] = &data[..] {
            let realm = match details.get("realm") {
                Some(Value::Str(s)) => s,
                Some(v) => return Err(Error::new(format!("Invalid type for 'realm': {v:?}"))),
                // some routers don't return realm in welcome and the one sent in HELLO
                // is assumed to be the one.
                None => "",
            };

            let authid = match details.get("authid") {
                Some(Value::Str(s)) => s,
                None | Some(Value::Null) => "",
                Some(v) => return Err(Error::new(format!("Invalid type for 'authid': {v:?}"))),
            };

            let auth_role = match details.get("authrole") {
                Some(Value::Str(s)) => s,
                None | Some(Value::Null) => "",
                Some(v) => return Err(Error::new(format!("Invalid type for 'authrole': {v:?}"))),
            };

            Ok(Box::new(Welcome {
                session_id: *session_id,
                realm: realm.to_string(),
                authid: authid.to_string(),
                auth_role: auth_role.to_string(),
                details: details.clone(),
            }))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}
