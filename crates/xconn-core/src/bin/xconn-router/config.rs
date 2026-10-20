use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};
use wampproto::{
    authenticators::server::{AuthRequest, AuthResponse, CRAKey, Salt, ServerAuthenticator},
    messages::types::Error,
    routing::MatchPolicy,
};
use xconn_core::async_::router::{Permission as Rule, RealmConfig, Router};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    version: String,
    #[serde(default)]
    config: Settings,
    realms: Vec<Realm>,
    pub transports: Vec<Transport>,
    #[serde(default)]
    authenticators: Authenticators,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Settings {
    loglevel: String,
    management: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Realm {
    name: String,
    roles: Vec<Role>,
    #[serde(default)]
    meta_api: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Role {
    name: String,
    permissions: Vec<Permission>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Permission {
    uri: String,
    #[serde(rename = "match")]
    matching: String,
    #[serde(default)]
    allow_call: bool,
    #[serde(default)]
    allow_register: bool,
    #[serde(default)]
    allow_publish: bool,
    #[serde(default)]
    allow_subscribe: bool,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transport {
    #[serde(rename = "type")]
    pub kind: String,
    pub listener: String,
    pub address: String,
    pub serializers: Vec<String>,
    #[serde(default)]
    pub tls: Option<Tls>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub certificate: PathBuf,
    pub key: PathBuf,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Authenticators {
    anonymous: Vec<Credential>,
    ticket: Vec<Credential>,
    wampcra: Vec<Credential>,
    cryptosign: Vec<Credential>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    authid: String,
    realm: String,
    role: String,
    #[serde(default)]
    ticket: String,
    #[serde(default)]
    secret: String,
    #[serde(default)]
    derive_secret: bool,
    #[serde(default)]
    salt: String,
    #[serde(default)]
    iterations: u32,
    #[serde(default)]
    keylen: usize,
    #[serde(default)]
    authorized_keys: Vec<String>,
}

fn matching(value: &str) -> Result<MatchPolicy> {
    match value {
        "exact" => Ok(MatchPolicy::Exact),
        "prefix" => Ok(MatchPolicy::Prefix),
        "wildcard" => Ok(MatchPolicy::Wildcard),
        _ => Err(format!("unknown match policy: {value}").into()),
    }
}

fn name(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(|c| c.is_whitespace() || c == '#')
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let mut value: Self = serde_yaml_ng::from_str(&std::fs::read_to_string(path)?)?;

        for transport in &mut value.transports {
            if let Some(tls) = &mut transport.tls {
                for file in [&mut tls.certificate, &mut tls.key] {
                    if file.is_relative() {
                        *file = path.parent().unwrap_or(Path::new(".")).join(&*file);
                    }
                }
            }

            if transport.listener == "unix" {
                if let Some(rest) = transport.address.strip_prefix("~/") {
                    transport.address = PathBuf::from(std::env::var("HOME")?)
                        .join(rest)
                        .to_string_lossy()
                        .into_owned();
                } else if Path::new(&transport.address).is_relative() {
                    transport.address = path
                        .parent()
                        .unwrap_or(Path::new("."))
                        .join(&transport.address)
                        .to_string_lossy()
                        .into_owned();
                }
            }
        }

        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        if self.version != "1" {
            return Err("only configuration version '1' is supported".into());
        }

        if self.config.management {
            return Err(
                "config.management is not supported by this executable; use the protected library management API"
                    .into(),
            );
        }

        if !["", "trace", "debug", "info", "warn", "error"].contains(&self.config.loglevel.as_str()) {
            return Err("invalid loglevel".into());
        }

        if self.realms.is_empty() || self.transports.is_empty() {
            return Err("at least one realm and transport are required".into());
        }

        let mut realms = HashMap::new();

        for realm in &self.realms {
            if !name(&realm.name) || realm.name.split('.').any(str::is_empty) || realms.contains_key(&realm.name) {
                return Err("invalid or duplicate realm".into());
            }

            let mut roles = HashSet::new();

            for role in &realm.roles {
                if !name(&role.name) || !roles.insert(role.name.clone()) {
                    return Err("invalid or duplicate role".into());
                }

                for permission in &role.permissions {
                    matching(&permission.matching)?;
                }
            }

            realms.insert(realm.name.clone(), roles);
        }

        for transport in &self.transports {
            if !["websocket", "rawsocket", "universal"].contains(&transport.kind.as_str()) {
                return Err(format!("unsupported transport: {}", transport.kind).into());
            }

            if !["tcp", "unix"].contains(&transport.listener.as_str()) || transport.address.is_empty() {
                return Err("invalid listener/address".into());
            }

            #[cfg(not(unix))]
            if transport.listener == "unix" {
                return Err("Unix listeners require a Unix platform".into());
            }

            if transport.serializers.is_empty()
                || transport
                    .serializers
                    .iter()
                    .any(|s| !["json", "cbor", "msgpack"].contains(&s.as_str()))
            {
                return Err("serializers must contain json, cbor or msgpack".into());
            }
        }

        for (method, credentials) in self.authenticators.entries() {
            let mut seen = HashSet::new();

            for item in credentials {
                if !name(&item.authid) || !realms.get(&item.realm).is_some_and(|r| r.contains(&item.role)) {
                    return Err(
                        "authenticator must reference an existing realm and role and have a nonempty authid".into(),
                    );
                }

                if !seen.insert((&item.realm, if method == "anonymous" { "" } else { &item.authid })) {
                    return Err("duplicate authenticator identity".into());
                }

                match method {
                    "ticket" if item.ticket.is_empty() => return Err("ticket is required".into()),
                    "wampcra" => {
                        if item.secret.is_empty() {
                            return Err("CRA secret is required".into());
                        }

                        item.cra()?;
                    }
                    "cryptosign"
                        if item.authorized_keys.is_empty()
                            || item
                                .authorized_keys
                                .iter()
                                .any(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit())) =>
                    {
                        return Err("cryptosign requires 32-byte hexadecimal public keys".into());
                    }
                    _ => {}
                }
            }
        }

        for realm in &self.realms {
            if !self
                .authenticators
                .entries()
                .iter()
                .any(|(_, items)| items.iter().any(|i| i.realm == realm.name))
            {
                return Err(format!("realm {} has no authenticators", realm.name).into());
            }
        }

        Ok(())
    }

    pub fn log_level(&self) -> tracing::Level {
        match self.config.loglevel.as_str() {
            "trace" => tracing::Level::TRACE,
            "debug" => tracing::Level::DEBUG,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        }
    }

    pub fn router(&self) -> Result<Router> {
        let router = Router::new();

        for realm in &self.realms {
            let auth = self.authenticators.clone();
            let mut roles = HashMap::new();

            for role in &realm.roles {
                let rules = role
                    .permissions
                    .iter()
                    .map(|p| {
                        Ok(Rule {
                            uri: p.uri.clone(),
                            matching: matching(&p.matching)?,
                            call: p.allow_call,
                            register: p.allow_register,
                            publish: p.allow_publish,
                            subscribe: p.allow_subscribe,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                roles.insert(role.name.clone(), rules);
            }

            router.add_realm(
                &realm.name,
                RealmConfig {
                    authentication: Arc::new(move || Box::new(auth.clone())),
                    roles,
                    meta_api: realm.meta_api,
                    ..RealmConfig::default()
                },
            )?;
        }

        Ok(router)
    }
}

impl Credential {
    fn cra(&self) -> std::result::Result<CRAKey, Error> {
        let salt = if self.salt.is_empty() {
            None
        } else {
            wampproto::authenticators::wampcra::validate_cra_parameters(
                i64::from(self.iterations),
                self.keylen as i64,
            )?;
            Some(Salt {
                salt: self.salt.clone(),
                iterations: self.iterations,
                key_length: self.keylen,
            })
        };

        if self.derive_secret {
            CRAKey::from_secret(&self.secret, salt)
        } else {
            Ok(CRAKey {
                key: self.secret.as_bytes().to_vec(),
                salt,
            })
        }
    }
}

impl Authenticators {
    fn entries(&self) -> [(&str, &Vec<Credential>); 4] {
        [
            ("anonymous", &self.anonymous),
            ("ticket", &self.ticket),
            ("wampcra", &self.wampcra),
            ("cryptosign", &self.cryptosign),
        ]
    }
}

impl ServerAuthenticator for Authenticators {
    fn methods(&self) -> Vec<String> {
        self.entries()
            .iter()
            .filter(|(_, items)| !items.is_empty())
            .map(|(m, _)| m.to_string())
            .collect()
    }

    fn authenticate(&self, request: &AuthRequest) -> std::result::Result<AuthResponse, Error> {
        let entry = self
            .entries()
            .into_iter()
            .find(|(m, _)| *m == request.method)
            .and_then(|(_, items)| {
                items.iter().find(|i| {
                    i.realm == request.realm
                        && (request.method == "anonymous" || i.authid == request.authid)
                        && match request.method.as_str() {
                            "ticket" => request.ticket.as_deref() == Some(&i.ticket),
                            "cryptosign" => request
                                .public_key
                                .as_ref()
                                .is_some_and(|key| i.authorized_keys.iter().any(|s| s.eq_ignore_ascii_case(key))),
                            _ => true,
                        }
                })
            })
            .ok_or_else(|| Error::new("authentication denied"))?;
        let authid = if request.method == "anonymous" && request.authid != entry.authid {
            format!("{:013x}", wampproto::idgen::generate_session_id()?)
        } else {
            entry.authid.clone()
        };
        let mut response = AuthResponse::new(authid, &entry.role);

        if request.method == "wampcra" {
            response.cra = Some(entry.cra()?);
        }

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_configuration_fails_before_listening() {
        let sample = include_str!("config.yaml");
        let cases = [
            sample.replace("version: '1'", "version: '2'"),
            sample.replace("management: false", "management: true"),
            sample.replace("loglevel: info", "loglevel: nonsense"),
            sample.replace("realm: realm1", "realm: absent"),
            sample.replace("role: anonymous", "role: absent"),
            sample.replace("type: websocket", "type: nonsense"),
            sample.replace("serializers: [json, cbor, msgpack]", "serializers: [protobuf]"),
            sample.replace("match: prefix", "match: nonsense"),
        ];

        for text in cases {
            let config: Config = serde_yaml_ng::from_str(&text).unwrap();
            assert!(config.validate().is_err());
        }

        let config: Config = serde_yaml_ng::from_str(sample).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn salted_cra_supports_go_derived_keys_and_explicit_plain_secrets() {
        let mut item: Credential = serde_yaml_ng::from_str(
            "authid: user\nrealm: realm\nrole: role\nsecret: secret\nsalt: test\niterations: 1000\nkeylen: 32\n",
        )
        .unwrap();
        item.derive_secret = true;
        let derived = item.cra().unwrap();
        item.derive_secret = false;
        item.secret = String::from_utf8(derived.key.clone()).unwrap();
        assert_eq!(item.cra().unwrap().key, derived.key);
        item.iterations = 0;
        assert!(item.cra().is_err());
    }
}
