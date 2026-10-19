pub const AUTH_METHOD: &str = "cryptosign";

use crate::authenticators::authenticator::ClientAuthenticator;
use crate::messages::authenticate::Authenticate;
use crate::messages::challenge::Challenge;
use crate::messages::types::{Error, Value};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use hex::{FromHex, ToHex};
use rand_core::{OsRng, RngCore};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CryptoSignAuthenticator {
    authid: String,
    private_key: SigningKey,
    extra: HashMap<String, Value>,
}

impl ClientAuthenticator for CryptoSignAuthenticator {
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
        if let Some(Value::Str(challenge_hex)) = challenge.extra.get("challenge") {
            match sign_crypto_sign_challenge(challenge_hex, &self.private_key) {
                Ok(signed) => Ok(Authenticate {
                    signature: signed,
                    extra: HashMap::new(),
                }),
                Err(err) => Err(Error::new(format!("failed to sign challenge: {err}"))),
            }
        } else {
            Err(Error::new("challenge missing in authextra or is none"))
        }
    }
}

impl CryptoSignAuthenticator {
    pub fn try_new(authid: &str, private_key: &str, extra: HashMap<String, Value>) -> Result<Self, Error> {
        match signing_key_from_hex(private_key) {
            Ok(key) => {
                let pub_key: String = key.verifying_key().encode_hex();
                let mut cloned = extra.clone();
                cloned.insert("pubkey".to_string(), Value::Str(pub_key));
                Ok(CryptoSignAuthenticator {
                    authid: authid.to_string(),
                    private_key: key,
                    extra: cloned,
                })
            }
            Err(err) => Err(Error::new(format!("{err}"))),
        }
    }
}

fn signing_key_from_hex(hex_str: &str) -> Result<SigningKey, Error> {
    match <[u8; 32]>::from_hex(hex_str) {
        Ok(bytes) => Ok(SigningKey::from_bytes(&bytes)),
        Err(e) => Err(Error::new(format!("invalid hex or wrong length: {e}"))),
    }
}

fn sign_crypto_sign_challenge(challenge: &str, private_key: &SigningKey) -> Result<String, Error> {
    match hex::decode(challenge) {
        Ok(challenge_raw) => {
            let signature = private_key.sign(&challenge_raw);
            let signed_hex = hex::encode(signature.to_vec());

            Ok(format!("{signed_hex}{challenge}"))
        }
        Err(e) => Err(Error::new(format!("failed to decode challenge hex: {e}"))),
    }
}

pub fn sign_cryptosign_challenge(challenge: &str, private_key: &[u8]) -> Result<String, Error> {
    let seed: &[u8; 32] = private_key
        .try_into()
        .map_err(|_| Error::new("private key must contain 32 bytes"))?;
    sign_crypto_sign_challenge(challenge, &SigningKey::from_bytes(seed))
}

/// Verify the hex-encoded signature followed by its signed challenge.
pub fn verify_cryptosign_signature(signed: &str, public_key: &[u8]) -> bool {
    let Ok(key_bytes) = <&[u8; 32]>::try_from(public_key) else {
        return false;
    };
    let Ok(key) = VerifyingKey::from_bytes(key_bytes) else {
        return false;
    };
    let Ok(signed) = hex::decode(signed) else {
        return false;
    };

    if signed.len() < 64 {
        return false;
    }

    let Ok(signature) = Signature::from_slice(&signed[..64]) else {
        return false;
    };
    key.verify_strict(&signed[64..], &signature).is_ok()
}

pub fn generate_cryptosign_keypair() -> Result<(String, String), Error> {
    let mut seed = [0u8; 32];
    OsRng
        .try_fill_bytes(&mut seed)
        .map_err(|e| Error::new(format!("failed to generate private key: {e}")))?;
    let key = SigningKey::from_bytes(&seed);
    Ok((hex::encode(key.verifying_key().as_bytes()), hex::encode(key.as_bytes())))
}

pub fn generate_cryptosign_challenge() -> Result<String, Error> {
    let mut bytes = [0u8; 32];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| Error::new(format!("failed to generate challenge: {e}")))?;
    Ok(hex::encode(bytes))
}
