use super::config::{Authenticators, RpcAuthenticationConfig};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use wampproto::{
    authenticators::{
        server::{AuthRequest, AuthResponse, CRAKey, Salt, ServerAuthenticator},
        wampcra::validate_cra_parameters,
    },
    messages::{
        schema::WireValue,
        types::{Error, Value},
    },
};
use xconn::async_::{router::AsyncAuthenticator, session::Session};

pub struct RpcAuthentication {
    pub config: RpcAuthenticationConfig,
    control_realm: String,
    session: Session,
    local: Authenticators,
    remote_methods: Vec<String>,
    password_attempts: Mutex<PasswordAttempts>,
}

impl RpcAuthentication {
    pub fn new(
        config: RpcAuthenticationConfig,
        control_realm: String,
        session: Session,
        local: Authenticators,
    ) -> Self {
        let remote_methods = [("wampcra", &config.wampcra), ("cryptosign", &config.cryptosign)]
            .into_iter()
            .filter(|(_, uri)| uri.is_some())
            .map(|(method, _)| method.into())
            .collect();
        Self {
            config,
            control_realm,
            session,
            local,
            remote_methods,
            password_attempts: Mutex::new(PasswordAttempts::default()),
        }
    }
}

#[async_trait::async_trait]
impl AsyncAuthenticator for RpcAuthentication {
    fn methods(&self, realm: &str, authid: &str) -> Vec<String> {
        if realm != self.control_realm {
            return self.remote_methods.clone();
        }
        let local = self.local.identity_methods(authid);
        if !local.is_empty() {
            return local;
        }
        let mut methods = self.remote_methods.clone();
        if self.local.methods().iter().any(|method| method == "anonymous") {
            methods.push("anonymous".into());
        }
        methods
    }

    async fn authenticate(&self, request: &AuthRequest) -> Result<AuthResponse, Error> {
        let control = request.realm == self.control_realm;
        if control && self.local.is_static_identity(request) {
            return self.local.authenticate(request);
        }

        let mut args = vec![Value::Str(request.authid.clone())];
        let uri = match request.method.as_str() {
            "wampcra" => {
                if !self.password_attempts.lock().unwrap().admit(&request.authid) {
                    return Err(Error::new("too many authentication attempts"));
                }
                &self.config.wampcra
            }
            "cryptosign" => {
                args.push(Value::Str(
                    request
                        .public_key
                        .clone()
                        .ok_or_else(|| Error::new("missing public key"))?,
                ));
                &self.config.cryptosign
            }
            _ => return Err(Error::new("unsupported remote authentication method")),
        };
        args.push(Value::Str(request.realm.clone()));

        let uri = uri
            .as_ref()
            .ok_or_else(|| Error::new("remote authentication method disabled"))?;
        let reply = self
            .session
            .call(uri)
            .args(args)
            .execute()
            .await
            .map_err(|_| Error::new("authentication RPC failed"))?;
        if reply.error.is_some() {
            return Err(Error::new("authentication denied"));
        }
        let Some([Value::Dict(response)]) = reply.args.as_deref() else {
            return Err(Error::new("invalid authentication response"));
        };
        let role = string(response, "authrole")?;
        if request.method == "cryptosign" && string(response, "authid")? != request.authid {
            return Err(Error::new("authentication identity mismatch"));
        }
        let (allowed, machine_role) = if control {
            (
                self.config.control_roles.iter().any(|name| name == role),
                &self.config.control_machine_role,
            )
        } else {
            (
                self.config.realm_roles.iter().any(|entry| entry.name == role),
                &self.config.realm_machine_role,
            )
        };
        if !allowed
            && !machine_role
                .as_ref()
                .is_some_and(|entry| entry.machine_name(&request.authid) == role)
        {
            return Err(Error::new("authentication role denied"));
        }

        let mut result = AuthResponse::new(&request.authid, role);
        if request.method == "wampcra" {
            let salt = string(response, "salt")?;
            let (iterations, key_length) = validate_cra_parameters(
                integer(response, "iterations", 1000)?,
                integer(response, "key_length", 32)?,
            )?;
            result.cra = Some(CRAKey {
                key: string(response, "password")?.as_bytes().to_vec(),
                salt: Some(Salt {
                    salt: salt.into(),
                    iterations,
                    key_length,
                }),
            });
        }
        Ok(result)
    }
}

fn string<'a>(response: &'a HashMap<String, Value>, field: &str) -> Result<&'a str, Error> {
    match response.get(field) {
        Some(Value::Str(value)) if !value.is_empty() => Ok(value),
        _ => Err(Error::new("invalid authentication response")),
    }
}

fn integer(response: &HashMap<String, Value>, field: &str, default: i64) -> Result<i64, Error> {
    response.get(field).map_or(Ok(default), |value| {
        value
            .integer()
            .ok_or_else(|| Error::new("invalid authentication response"))
    })
}

#[derive(Default)]
struct PasswordAttempts {
    started: Option<Instant>,
    counts: HashMap<String, u8>,
}

impl PasswordAttempts {
    // Match the per-account limit used by Deskconn Hub.
    fn admit(&mut self, authid: &str) -> bool {
        if self
            .started
            .is_none_or(|start| start.elapsed() >= Duration::from_secs(15 * 60))
        {
            self.started = Some(Instant::now());
            self.counts.clear();
        }
        let account = authid.trim().to_lowercase();
        if self.counts.len() >= 100_000 && !self.counts.contains_key(&account) {
            return false;
        }
        let count = self.counts.entry(account).or_default();
        if *count >= 20 {
            return false;
        }
        *count += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_password_window_does_not_reset_account_limits() {
        let mut attempts = PasswordAttempts {
            started: Some(Instant::now()),
            counts: (0..100_000).map(|account| (account.to_string(), 20)).collect(),
        };
        assert!(!attempts.admit("new-account"));
        assert!(!attempts.admit(" 0 "));
        assert_eq!(attempts.counts.len(), 100_000);

        attempts.started = Some(Instant::now() - Duration::from_secs(15 * 60));
        assert!(attempts.admit("0"));
        assert_eq!(attempts.counts.len(), 1);
    }
}
