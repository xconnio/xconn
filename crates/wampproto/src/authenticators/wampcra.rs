pub const AUTH_METHOD: &str = "wampcra";

use crate::authenticators::authenticator::ClientAuthenticator;
use crate::messages::authenticate::Authenticate;
use crate::messages::challenge::Challenge;
use crate::messages::types::{Error, Value};
use base64::{Engine as _, engine::general_purpose};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct WAMPCRAAuthenticator {
    authid: String,
    secret: String,
    extra: HashMap<String, Value>,
}

impl ClientAuthenticator for WAMPCRAAuthenticator {
    fn auth_method(&self) -> String {
        AUTH_METHOD.to_string()
    }

    fn authid(&self) -> String {
        self.authid.clone()
    }

    fn auth_extra(&self) -> HashMap<String, Value> {
        self.extra.clone()
    }

    fn authenticate(&self, challenge: &Challenge) -> Result<Authenticate, Error> {
        let challenge_hex = match challenge.extra.get("challenge") {
            Some(Value::Str(s)) => s.to_string(),
            _ => return Err(Error::new("challenge must be a string")),
        };

        if let Some(salt) = challenge
            .extra
            .get("salt")
            .filter(|value| !matches!(value, Value::Null))
        {
            let iterations = match challenge.extra.get("iterations") {
                None => 0,
                Some(value) => crate::messages::schema::WireValue::integer(value)
                    .ok_or_else(|| Error::new("iterations must be an int"))?,
            };
            let keylen = match challenge.extra.get("keylen") {
                None => 0,
                Some(value) => crate::messages::schema::WireValue::integer(value)
                    .ok_or_else(|| Error::new("keylen must be an int"))?,
            };
            let (iterations, keylen) = validate_cra_parameters(iterations, keylen)?;
            let salt = match salt {
                Value::Str(value) => value,
                _ => return Err(Error::new("salt must be a string")),
            };

            let key = derive_wamp_cra_key(self.secret.as_str(), salt.as_str(), iterations, keylen)
                .map_err(|e| Error::new(e.to_string()))?;

            let signature =
                sign_cra_challenge(challenge_hex.as_str(), key.as_bytes()).map_err(|e| Error::new(e.to_string()))?;

            Ok(Authenticate {
                signature,
                extra: self.extra.clone(),
            })
        } else {
            let signature = sign_cra_challenge(challenge_hex.as_str(), self.secret.as_bytes())
                .map_err(|e| Error::new(e.to_string()))?;

            Ok(Authenticate {
                signature,
                extra: self.extra.clone(),
            })
        }
    }
}

impl WAMPCRAAuthenticator {
    pub fn new(authid: &str, secret: &str, extra: HashMap<String, Value>) -> Self {
        WAMPCRAAuthenticator {
            authid: authid.to_string(),
            secret: secret.to_string(),
            extra,
        }
    }
}

// Type alias for HMAC-SHA256
type HmacSha256 = Hmac<Sha256>;

pub fn sign_cra_challenge(challenge: &str, key: &[u8]) -> Result<String, Error> {
    sign_cra_message(challenge.as_bytes(), key)
}

/// Sign arbitrary message bytes, also used by host-language HMAC helpers.
pub fn sign_cra_message(message: &[u8], key: &[u8]) -> Result<String, Error> {
    match HmacSha256::new_from_slice(key) {
        Ok(mut mac) => {
            mac.update(message);
            let signature = mac.finalize().into_bytes();
            Ok(general_purpose::STANDARD.encode(signature))
        }
        Err(err) => Err(Error::new(format!("Failed to generate mac {err}"))),
    }
}

pub fn derive_wamp_cra_key(secret: &str, salt: &str, iterations: u32, keylen: usize) -> Result<String, Error> {
    if iterations == 0 {
        return Err(Error::new(INVALID_ITERATIONS));
    }

    if keylen == 0 {
        return Err(Error::new(INVALID_KEY_LENGTH));
    }

    let mut derived_key = vec![0u8; keylen];

    pbkdf2_hmac::<Sha256>(secret.as_bytes(), salt.as_bytes(), iterations, &mut derived_key);
    Ok(general_purpose::STANDARD.encode(&derived_key))
}

pub fn verify_wampcra_signature(signature: &str, challenge: &str, key: &[u8]) -> bool {
    let Ok(signature) = general_purpose::STANDARD.decode(signature) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(key) else {
        return false;
    };
    mac.update(challenge.as_bytes());
    mac.verify_slice(&signature).is_ok()
}

pub fn generate_wampcra_nonce() -> Result<String, Error> {
    let mut bytes = [0u8; 16];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| Error::new(format!("failed to generate nonce: {e}")))?;
    Ok(hex::encode(bytes))
}

const INVALID_ITERATIONS: &str = "iterations string required in extra & should be greater than 0";
const INVALID_KEY_LENGTH: &str = "keylen string missing in extra & should be greater than 0";

/// Validate signed parameters before converting to PBKDF2's input types.
pub fn validate_cra_parameters(iterations: i64, keylen: i64) -> Result<(u32, usize), Error> {
    if iterations <= 0 {
        return Err(Error::new(INVALID_ITERATIONS));
    }

    if keylen <= 0 {
        return Err(Error::new(INVALID_KEY_LENGTH));
    }

    Ok((
        iterations
            .try_into()
            .map_err(|_| Error::new("iterations exceeds u32 range"))?,
        keylen
            .try_into()
            .map_err(|_| Error::new("keylen exceeds platform range"))?,
    ))
}

/// Current UTC timestamp in WAMP-CRA's millisecond format.
pub fn utcnow() -> String {
    chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Build the complete JSON challenge; callers sign this exact string.
pub fn generate_wampcra_challenge(
    session_id: i64,
    authid: &str,
    authrole: &str,
    provider: &str,
) -> Result<String, Error> {
    generate_wampcra_challenge_with_timestamp(session_id, authid, authrole, provider, &utcnow())
}

/// Hosts without a system clock (including WASM) supply their UTC timestamp.
pub fn generate_wampcra_challenge_with_timestamp(
    session_id: i64,
    authid: &str,
    authrole: &str,
    provider: &str,
    timestamp: &str,
) -> Result<String, Error> {
    let challenge = serde_json::json!({
        "nonce": generate_wampcra_nonce()?,
        "authprovider": provider,
        "authid": authid,
        "authrole": authrole,
        "authmethod": AUTH_METHOD,
        "session": session_id,
        "timestamp": timestamp,
    });
    serde_json::to_string(&challenge).map_err(|error| Error::new(error.to_string()))
}
