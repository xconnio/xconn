pub const AUTH_METHOD: &str = "anonymous";

use crate::authenticators::authenticator::ClientAuthenticator;
use crate::messages::authenticate::Authenticate;
use crate::messages::challenge::Challenge;
use crate::messages::types::{Error, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct AnonymousAuthenticator {
    authid: String,
    extra: HashMap<String, Value>,
}

impl ClientAuthenticator for AnonymousAuthenticator {
    fn auth_method(&self) -> String {
        AUTH_METHOD.to_string()
    }

    fn authid(&self) -> String {
        self.authid.clone()
    }

    fn auth_extra(&self) -> HashMap<String, Value> {
        self.extra.clone()
    }

    fn authenticate(&self, _: &Challenge) -> Result<Authenticate, Error> {
        Err(Error::new(
            "authenticate() must not be called for anonymous authentication",
        ))
    }
}

impl AnonymousAuthenticator {
    pub fn new(authid: &str, extra: HashMap<String, Value>) -> Self {
        AnonymousAuthenticator {
            authid: authid.to_string(),
            extra,
        }
    }
}

impl Default for AnonymousAuthenticator {
    fn default() -> Self {
        AnonymousAuthenticator::new("", Default::default())
    }
}
