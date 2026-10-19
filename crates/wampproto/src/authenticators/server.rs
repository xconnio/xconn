//! Application-supplied authentication policy; protocol verification lives in Acceptor.
use crate::messages::types::{Error, Value};
use std::{collections::HashMap, time::Duration};

#[derive(Clone, Debug)]
pub struct AuthRequest {
    pub realm: String,
    pub method: String,
    pub authid: String,
    pub extra: HashMap<String, Value>,
    pub ticket: Option<String>,
    pub public_key: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Salt {
    pub salt: String,
    pub iterations: u32,
    pub key_length: usize,
}

#[derive(Clone, Debug)]
pub struct CRAKey {
    /// Bytes used for HMAC. For salted CRA this is the base64-encoded derived key.
    pub key: Vec<u8>,
    pub salt: Option<Salt>,
}

impl CRAKey {
    pub fn from_secret(secret: &str, salt: Option<Salt>) -> Result<Self, Error> {
        let key = match &salt {
            None => secret.as_bytes().to_vec(),
            Some(s) => {
                super::wampcra::validate_cra_parameters(
                    i64::from(s.iterations),
                    i64::try_from(s.key_length).map_err(|_| Error::new("invalid key length"))?,
                )?;
                super::wampcra::derive_wamp_cra_key(secret, &s.salt, s.iterations, s.key_length)?.into_bytes()
            }
        };
        Ok(Self { key, salt })
    }
}

#[derive(Clone, Debug)]
pub struct AuthResponse {
    pub authid: String,
    pub auth_role: String,
    pub ttl: Duration,
    pub cra: Option<CRAKey>,
    pub extra: HashMap<String, Value>,
}

impl AuthResponse {
    pub fn new(authid: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            authid: authid.into(),
            auth_role: role.into(),
            ttl: Duration::ZERO,
            cra: None,
            extra: HashMap::new(),
        }
    }
}

pub trait ServerAuthenticator: Send {
    fn methods(&self) -> Vec<String>;

    fn authenticate(&self, request: &AuthRequest) -> Result<AuthResponse, Error>;
}

#[derive(Default)]
pub struct AnonymousAuthenticator;

impl ServerAuthenticator for AnonymousAuthenticator {
    fn methods(&self) -> Vec<String> {
        vec!["anonymous".into()]
    }

    fn authenticate(&self, request: &AuthRequest) -> Result<AuthResponse, Error> {
        if request.method != "anonymous" {
            return Err(Error::new("only anonymous authentication is enabled"));
        }

        Ok(AuthResponse::new("anonymous", "anonymous"))
    }
}

pub fn select_auth_method(allowed: &[String], proposed: &[String]) -> Result<String, Error> {
    let fallback = vec!["anonymous".into()];
    let proposed = if proposed.is_empty() { &fallback } else { proposed };
    proposed
        .iter()
        .find(|p| allowed.contains(p))
        .cloned()
        .ok_or_else(|| Error::new("no matching authentication method"))
}
