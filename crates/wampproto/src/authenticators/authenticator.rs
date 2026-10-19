use crate::messages::authenticate::Authenticate;
use crate::messages::challenge::Challenge;
use crate::messages::types::{Error, Value};
use std::collections::HashMap;
use std::fmt::Debug;

pub trait ClientAuthenticator: XClone + Debug + Send + Sync {
    fn auth_method(&self) -> String;

    fn authid(&self) -> String;

    fn auth_extra(&self) -> HashMap<String, Value>;

    fn authenticate(&self, challenge: &Challenge) -> Result<Authenticate, Error>;
}

pub trait XClone {
    fn clone_box(&self) -> Box<dyn ClientAuthenticator + Send + Sync>;
}

impl<T> XClone for T
where
    T: ClientAuthenticator + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn ClientAuthenticator + Send + Sync> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ClientAuthenticator + Send + Sync> {
    fn clone(&self) -> Box<dyn ClientAuthenticator + Send + Sync> {
        self.clone_box()
    }
}
