use crate::messages;
use messages::message::Message;
use messages::types::{Error, Value};
use messages::validator::ValidationSpec;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

pub const MESSAGE_TYPE_HELLO: Value = Value::Int(1);
pub const MESSAGE_NAME_HELLO: &str = "HELLO";

pub const VALIDATION_SPEC: ValidationSpec = ValidationSpec {
    min_length: 3,
    max_length: 3,
    name: MESSAGE_NAME_HELLO,
};

#[derive(Debug)]
pub struct Hello {
    pub realm: String,
    pub authid: String,
    pub auth_methods: Vec<String>,
    pub auth_extra: HashMap<String, Value>,
    pub roles: HashMap<String, Value>,
}

impl Message for Hello {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> Value {
        MESSAGE_TYPE_HELLO
    }

    fn marshal(&self) -> Vec<Value> {
        crate::messages::schema::marshal_values(
            1,
            [
                ("realm", Value::Str(self.realm.clone())),
                ("authid", Value::Str(self.authid.clone())),
                (
                    "authmethods",
                    Value::List(self.auth_methods.iter().cloned().map(Value::Str).collect()),
                ),
                ("authextra", Value::Dict(self.auth_extra.clone())),
                ("roles", Value::Dict(self.roles.clone())),
            ],
        )
    }

    fn parse(data: Vec<Value>) -> Result<Box<dyn Message>, Error> {
        VALIDATION_SPEC.validate_message(&data)?;

        if let [_, Value::Str(realm), Value::Dict(details)] = &data[..] {
            let empty = HashMap::new();
            let authid = match details.get("authid") {
                Some(Value::Str(s)) => s,
                None | Some(Value::Null) => "",
                Some(v) => return Err(Error::new(format!("Invalid type for 'authid': {v:?}"))),
            };

            let auth_methods = match details.get("authmethods") {
                Some(Value::List(s)) => s.to_vec(),
                None | Some(Value::Null) => Vec::new(),
                Some(v) => {
                    return Err(Error::new(format!("Invalid type for 'authemthods': {v:?}")));
                }
            };

            let auth_extra = match details.get("authextra") {
                Some(Value::Dict(s)) => s,
                None | Some(Value::Null) => &empty,
                Some(v) => return Err(Error::new(format!("Invalid type for 'authextra': {v:?}"))),
            };

            let roles = match details.get("roles") {
                Some(Value::Dict(s)) => s,
                Some(v) => return Err(Error::new(format!("Invalid type for 'authroles': {v:?}"))),
                None => return Err(Error::new("Missing field: 'authroles'")),
            };

            let mut methods: Vec<String> = Default::default();

            for method in auth_methods {
                // essentially ignore any invalid authmethod
                if let Value::Str(str) = method {
                    methods.push(str.to_string());
                }
            }

            Ok(Box::new(Hello::new(
                realm,
                authid,
                auth_extra.clone(),
                roles.clone(),
                methods,
            )))
        } else {
            Err(VALIDATION_SPEC.err_invalid_message())
        }
    }
}

impl Hello {
    pub fn new(
        realm: &str,
        authid: &str,
        auth_extra: HashMap<String, Value>,
        roles: HashMap<String, Value>,
        auth_methods: Vec<String>,
    ) -> Hello {
        Hello {
            realm: realm.to_string(),
            authid: authid.to_string(),
            auth_extra,
            auth_methods,
            roles,
        }
    }
}
