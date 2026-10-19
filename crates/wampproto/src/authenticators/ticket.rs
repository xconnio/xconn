pub const AUTH_METHOD: &str = "ticket";

use crate::authenticators::authenticator::ClientAuthenticator;
use crate::messages::authenticate::Authenticate;
use crate::messages::challenge::Challenge;
use crate::messages::types::{Error, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TicketAuthenticator {
    authid: String,
    ticket: String,
    extra: HashMap<String, Value>,
}

impl ClientAuthenticator for TicketAuthenticator {
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
        Ok(Authenticate {
            signature: self.ticket.clone(),
            extra: self.extra.clone(),
        })
    }
}

impl TicketAuthenticator {
    pub fn new(authid: &str, ticket: &str, extra: HashMap<String, Value>) -> Self {
        TicketAuthenticator {
            authid: authid.to_string(),
            ticket: ticket.to_string(),
            extra,
        }
    }
}
