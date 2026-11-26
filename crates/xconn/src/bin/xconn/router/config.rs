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
use xconn::{
    Value,
    async_::{
        management,
        router::{Permission as Rule, RealmConfig, Router},
    },
};

pub(super) const LOG_LEVELS: [&str; 5] = ["trace", "debug", "info", "warn", "error"];

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    version: String,
    #[serde(default)]
    config: Settings,
    realms: Vec<Realm>,
    pub transports: Vec<Transport>,
    pub realm_source: Option<RealmSourceConfig>,
    #[serde(default)]
    authenticators: Authenticators,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealmSourceConfig {
    pub control_realm: String,
    pub role: String,
    pub list: String,
    pub add: String,
    pub remove: String,
    pub authentication: Option<RpcAuthenticationConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcAuthenticationConfig {
    pub wampcra: Option<String>,
    pub cryptosign: Option<String>,
    #[serde(default)]
    pub control_roles: Vec<String>,
    #[serde(default)]
    pub realm_roles: Vec<Role>,
    pub realm_machine_role: Option<Role>,
    pub control_machine_role: Option<Role>,
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
    #[serde(default)]
    disclose_caller: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RealmProvision {
    roles: Vec<Role>,
    #[serde(default)]
    meta_api: bool,
    authenticators: Authenticators,
}

pub fn provision_realm(name: &str, spec: &Value) -> std::result::Result<RealmConfig, xconn::Error> {
    let provision: RealmProvision = serde_yaml_ng::to_value(spec)
        .and_then(serde_yaml_ng::from_value)
        .map_err(|error| xconn::Error::new(error.to_string()))?;
    let realm = Realm {
        name: name.into(),
        roles: provision.roles,
        meta_api: provision.meta_api,
        disclose_caller: false,
    };
    validate_realms(std::slice::from_ref(&realm), &provision.authenticators, false)
        .and_then(|()| realm.router_config(provision.authenticators))
        .map_err(|error| xconn::Error::new(error.to_string()))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Role {
    pub name: String,
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
    pub serializers: Option<Vec<String>>,
    #[serde(default)]
    pub tls: Option<Tls>,
    pub path: Option<String>,
    pub cert_hash_address: Option<std::net::SocketAddr>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub certificate: PathBuf,
    pub key: PathBuf,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Authenticators {
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

pub fn valid_uri(value: &str) -> bool {
    name(value) && !value.split('.').any(str::is_empty)
}

pub fn sourced_realm_config() -> RealmConfig {
    RealmConfig {
        authentication: Arc::new(|| Box::<Authenticators>::default()),
        roles: HashMap::new(),
        ..RealmConfig::default()
    }
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

        if !self.config.loglevel.is_empty() && !LOG_LEVELS.contains(&self.config.loglevel.as_str()) {
            return Err(format!("Invalid config.loglevel. Expected one of {}.", LOG_LEVELS.join(", ")).into());
        }

        if self.realms.is_empty() || self.transports.is_empty() {
            return Err("at least one realm and transport are required".into());
        }

        validate_realms(&self.realms, &self.authenticators, self.config.management)?;

        if let Some(source) = &self.realm_source {
            let control = self
                .realms
                .iter()
                .find(|realm| realm.name == source.control_realm)
                .ok_or("realm_source requires a configured control realm")?;
            if !control.disclose_caller || (self.config.management && control.name == management::REALM) {
                return Err("realm_source requires a control realm with disclose_caller enabled".into());
            }
            let role = control
                .roles
                .iter()
                .find(|role| role.name == source.role)
                .ok_or("realm_source requires a configured control realm role")?;
            if self.authenticators.entries().iter().any(|(_, credentials)| {
                credentials
                    .iter()
                    .any(|item| item.realm == control.name && item.role == role.name)
            }) {
                return Err("realm_source role must not have network credentials".into());
            }
            let procedures = [&source.list, &source.add, &source.remove];
            if procedures.iter().any(|uri| !valid_uri(uri)) || procedures.into_iter().collect::<HashSet<_>>().len() != 3
            {
                return Err("realm_source requires distinct list, add and remove procedure URIs".into());
            }
            let allows = |uri: &str, enabled: fn(&Permission) -> bool| {
                role.permissions.iter().any(|rule| {
                    enabled(rule) && matching(&rule.matching).is_ok_and(|policy| policy.matches(&rule.uri, uri))
                })
            };
            if !allows(&source.list, |rule| rule.allow_call) {
                return Err(format!(
                    "realm_source role {} must be allowed to call {}",
                    role.name, source.list
                )
                .into());
            }
            for uri in [&source.add, &source.remove] {
                if !allows(uri, |rule| rule.allow_register) {
                    return Err(format!("realm_source role {} must be allowed to register {uri}", role.name).into());
                }
            }
            if let Some(auth) = &source.authentication {
                if auth.wampcra.is_none() && auth.cryptosign.is_none() {
                    return Err("realm source authentication requires at least one RPC URI".into());
                }
                for uri in auth.wampcra.iter().chain(&auth.cryptosign) {
                    if !valid_uri(uri) || [&source.list, &source.add, &source.remove].contains(&uri) {
                        return Err("invalid realm source authentication procedure URI".into());
                    }
                    if !allows(uri, |rule| rule.allow_call) {
                        return Err(format!("realm_source role {} must be allowed to call {uri}", role.name).into());
                    }
                }
                for role in &auth.control_roles {
                    if role == &source.role || !control.roles.iter().any(|item| &item.name == role) {
                        return Err(
                            "remote authentication must use configured control roles other than the source role".into(),
                        );
                    }
                }
                let mut names = HashSet::new();
                for role in &auth.realm_roles {
                    if !name(&role.name) || !names.insert(&role.name) {
                        return Err("invalid or duplicate sourced realm role".into());
                    }
                    role.rules(None)?;
                }
                for role in auth.realm_machine_role.iter().chain(&auth.control_machine_role) {
                    role.machine_name_parts()?;
                    role.rules(Some("machine"))?;
                }
                if let Some(template) = &auth.control_machine_role {
                    let (prefix, suffix) = template.machine_name_parts()?;
                    if control.roles.iter().any(|role| {
                        role.name
                            .strip_prefix(prefix)
                            .and_then(|name| name.strip_suffix(suffix))
                            .is_some_and(|authid| !authid.is_empty())
                    }) {
                        return Err("control machine role template overlaps a configured role".into());
                    }
                }
            }
        }

        for transport in &self.transports {
            if transport.kind != "webtransport" && (transport.path.is_some() || transport.cert_hash_address.is_some()) {
                return Err("path and cert_hash_address are only supported for WebTransport".into());
            }
            if let Some(serializers) = &transport.serializers
                && (serializers.is_empty()
                    || serializers
                        .iter()
                        .any(|name| !matches!(name.as_str(), "json" | "cbor" | "msgpack")))
            {
                return Err("serializers must contain json, cbor or msgpack".into());
            }
            match transport.kind.as_str() {
                "quic" | "webtransport" => {
                    if transport.listener != "udp" || transport.address.parse::<std::net::SocketAddr>().is_err() {
                        return Err(
                            format!("{} requires a UDP listener and an IP socket address", transport.kind).into(),
                        );
                    }
                    if transport.kind == "quic" && transport.tls.is_none() {
                        return Err("QUIC requires a TLS certificate and key".into());
                    }
                    if let Some(path) = &transport.path
                        && (!path.starts_with('/')
                            || !url::Url::parse(&format!("https://localhost{path}")).is_ok_and(|url| {
                                url.path() == path && url.query().is_none() && url.fragment().is_none()
                            }))
                    {
                        return Err("WebTransport path must be a URL path without a query or fragment".into());
                    }
                    if transport.cert_hash_address.is_some() && transport.tls.is_some() {
                        return Err(
                            "cert_hash_address requires an automatically generated WebTransport certificate".into(),
                        );
                    }
                    continue;
                }
                "websocket" | "rawsocket" | "universal" => {}
                _ => return Err(format!("unsupported transport: {}", transport.kind).into()),
            }

            if !["tcp", "unix"].contains(&transport.listener.as_str()) || transport.address.is_empty() {
                return Err("invalid listener/address".into());
            }

            #[cfg(not(unix))]
            if transport.listener == "unix" {
                return Err("Unix listeners require a Unix platform".into());
            }

            if transport.serializers.is_none() {
                return Err("serializers are required for stream listeners".into());
            }
        }

        Ok(())
    }

    pub fn log_level(&self) -> tracing::Level {
        self.config.loglevel.parse().unwrap_or(tracing::Level::INFO)
    }

    pub fn log_realms(&self) {
        for realm in &self.realms {
            let roles: Vec<_> = realm.roles.iter().map(|role| role.name.as_str()).collect();
            let names = if roles.is_empty() {
                "none".to_owned()
            } else {
                roles.join(", ")
            };
            tracing::info!(
                target: super::logging::LIFECYCLE,
                realm = %realm.name,
                roles = ?roles,
                "Realm {} (roles: {names})",
                realm.name
            );
        }
    }

    pub fn source_credentials(&self) -> Authenticators {
        self.realm_source
            .as_ref()
            .map_or_else(Authenticators::default, |source| {
                self.authenticators.for_realm(&source.control_realm)
            })
    }

    pub fn router(&self) -> Result<(Router, Option<RealmConfig>)> {
        let router = Router::new();
        let mut management_config = None;

        for realm in &self.realms {
            let mut config = realm.router_config(self.authenticators.for_realm(&realm.name))?;
            if self.config.management && realm.name == management::REALM {
                config.roles.insert(management::INTERNAL_ROLE.into(), vec![Rule::all()]);
                config.disclose_caller = true;
                management_config = Some(config);
            } else {
                router.add_realm(&realm.name, config)?;
            }
        }

        Ok((router, management_config))
    }
}

impl Realm {
    fn router_config(&self, auth: Authenticators) -> Result<RealmConfig> {
        let mut roles = HashMap::new();

        for role in &self.roles {
            roles.insert(role.name.clone(), role.rules(None)?);
        }

        Ok(RealmConfig {
            authentication: Arc::new(move || Box::new(auth.clone())),
            roles,
            meta_api: self.meta_api,
            disclose_caller: self.disclose_caller,
            ..RealmConfig::default()
        })
    }
}

impl Role {
    fn machine_name_parts(&self) -> Result<(&str, &str)> {
        self.name
            .split_once("{authid}")
            .filter(|(_, suffix)| name(&self.name) && !suffix.contains("{authid}"))
            .ok_or_else(|| "machine role name must contain exactly one {authid}".into())
    }

    pub fn machine_name(&self, authid: &str) -> String {
        self.name.replace("{authid}", authid)
    }

    pub fn rules(&self, authid: Option<&str>) -> Result<Vec<Rule>> {
        self.permissions
            .iter()
            .map(|permission| {
                let matching = matching(&permission.matching)?;
                if authid.is_some() && permission.uri.contains("{authid}") && matching != MatchPolicy::Exact {
                    return Err("machine permission URIs with {authid} must use exact matching".into());
                }
                Ok(Rule {
                    uri: authid.map_or_else(|| permission.uri.clone(), |id| permission.uri.replace("{authid}", id)),
                    matching,
                    call: permission.allow_call,
                    register: permission.allow_register,
                    publish: permission.allow_publish,
                    subscribe: permission.allow_subscribe,
                })
            })
            .collect()
    }
}

fn validate_realms(realm_configs: &[Realm], authenticators: &Authenticators, management_enabled: bool) -> Result<()> {
    let mut realms = HashMap::new();

    for realm in realm_configs {
        if !name(&realm.name) || realm.name.split('.').any(str::is_empty) || realms.contains_key(&realm.name) {
            return Err("invalid or duplicate realm".into());
        }

        let mut roles = HashSet::new();

        for role in &realm.roles {
            if !name(&role.name) || !roles.insert(role.name.clone()) {
                return Err("invalid or duplicate role".into());
            }

            if management_enabled && realm.name == management::REALM && role.name == management::INTERNAL_ROLE {
                return Err(format!("role {} is reserved for the internal management session", role.name).into());
            }

            for permission in &role.permissions {
                matching(&permission.matching)?;
            }
        }

        realms.insert(realm.name.clone(), roles);
    }

    if management_enabled && !realms.contains_key(management::REALM) {
        return Err(format!("config.management requires a {} realm", management::REALM).into());
    }

    for (method, credentials) in authenticators.entries() {
        let mut seen = HashSet::new();

        for item in credentials {
            if !name(&item.authid) || !realms.get(&item.realm).is_some_and(|r| r.contains(&item.role)) {
                return Err(
                    "authenticator must reference an existing realm and role and have a nonempty authid".into(),
                );
            }

            if management_enabled && item.realm == management::REALM && method == "anonymous" {
                return Err("the management realm requires authenticated credentials".into());
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

    for realm in realm_configs {
        if !authenticators
            .entries()
            .iter()
            .any(|(_, items)| items.iter().any(|i| i.realm == realm.name))
        {
            return Err(format!("realm {} has no authenticators", realm.name).into());
        }
    }

    Ok(())
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
    fn for_realm(&self, realm: &str) -> Self {
        let credentials = |items: &[Credential]| items.iter().filter(|item| item.realm == realm).cloned().collect();
        Self {
            anonymous: credentials(&self.anonymous),
            ticket: credentials(&self.ticket),
            wampcra: credentials(&self.wampcra),
            cryptosign: credentials(&self.cryptosign),
        }
    }

    pub fn identity_methods(&self, authid: &str) -> Vec<String> {
        self.entries()
            .into_iter()
            .filter(|(method, credentials)| {
                *method != "anonymous" && credentials.iter().any(|item| item.authid == authid)
            })
            .map(|(method, _)| method.into())
            .collect()
    }

    pub fn is_static_identity(&self, request: &AuthRequest) -> bool {
        self.entries().iter().any(|(method, credentials)| {
            credentials.iter().any(|item| {
                item.realm == request.realm
                    && if *method == "anonymous" {
                        request.method == "anonymous"
                    } else {
                        item.authid == request.authid
                    }
            })
        })
    }

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
            sample.replace("- msgpack", "- protobuf"),
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
