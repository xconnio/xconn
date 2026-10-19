use crate::messages::types::{Error, Value};
use std::any::type_name;

pub struct ValidationSpec {
    pub min_length: usize,
    pub max_length: usize,
    pub name: &'static str,
}

impl ValidationSpec {
    pub fn err_invalid_message(&self) -> Error {
        Error::new(format!("{} received invalid message format", self.name))
    }

    pub fn validate_message(&self, data: &[Value]) -> Result<(), Error> {
        if let Some(schema) = crate::messages::schema::named(self.name) {
            return schema
                .parse(&Value::List(data.to_vec()))
                .map(|_| ())
                .map_err(|errors| Error::new(errors.join("; ")));
        }

        sanity_check(data, self)
    }
}

pub fn get_type<T>(_: &T) -> String {
    type_name::<T>().to_string()
}

fn sanity_check(wamp_msg: &[Value], spec: &ValidationSpec) -> Result<(), Error> {
    if wamp_msg.len() < spec.min_length {
        let message = format!(
            "unexpected message length for {}: must be at least {}, but was {}",
            spec.name,
            spec.min_length,
            wamp_msg.len()
        );

        Err(Error::new(message))
    } else if wamp_msg.len() > spec.max_length {
        let message: String = format!(
            "unexpected message length for {}, must be at most {}, but was {}",
            spec.name,
            spec.max_length,
            wamp_msg.len()
        );

        Err(Error::new(message))
    } else {
        Ok(())
    }
}
