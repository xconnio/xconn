//! An optional WAMP endpoint directory with explicit application authorization.
//!
//! Entries advertise connection hints, never credentials. They belong to their
//! announcing session and disappear on leave. The hosting realm must disclose
//! callers and enable session meta events. No roles or URI permissions are imposed.
use super::bridge::Caller;
use crate::async_::{
    session::{Session, Subscription},
    types::{RegisterRequest, SubscribeRequest},
};
use crate::wampproto::messages::schema::WireValue;
use crate::{CallRequest, Error, RpcResult, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone, Debug, PartialEq)]
pub struct Endpoint {
    pub id: String,
    pub addresses: Vec<String>,
    pub capabilities: Vec<String>,
    pub metadata: HashMap<String, Value>,
}

impl Endpoint {
    pub fn to_value(&self) -> Value {
        Value::Dict(HashMap::from([
            ("id".into(), Value::Str(self.id.clone())),
            (
                "addresses".into(),
                Value::List(self.addresses.iter().cloned().map(Value::Str).collect()),
            ),
            (
                "capabilities".into(),
                Value::List(self.capabilities.iter().cloned().map(Value::Str).collect()),
            ),
            ("metadata".into(), Value::Dict(self.metadata.clone())),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self, Error> {
        let Value::Dict(fields) = value else {
            return Err(Error::new("expected endpoint object"));
        };
        let Some(Value::Str(id)) = fields.get("id") else {
            return Err(Error::new("endpoint id is required"));
        };
        if id.is_empty() {
            return Err(Error::new("endpoint id must not be empty"));
        }
        let strings = |key: &str| match fields.get(key) {
            None => Ok(Vec::new()),
            Some(Value::List(values)) => values
                .iter()
                .map(|value| match value {
                    Value::Str(value) => Ok(value.clone()),
                    _ => Err(Error::new(format!("endpoint {key} must contain strings"))),
                })
                .collect(),
            _ => Err(Error::new(format!("endpoint {key} must be an array"))),
        };
        let metadata = match fields.get("metadata") {
            None => HashMap::new(),
            Some(Value::Dict(metadata)) => metadata.clone(),
            _ => return Err(Error::new("endpoint metadata must be an object")),
        };
        Ok(Self {
            id: id.clone(),
            addresses: strings("addresses")?,
            capabilities: strings("capabilities")?,
            metadata,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DiscoveryAction {
    Advertise,
    Discover,
    Withdraw,
}

/// Authorization runs for every endpoint, including list/get.
/// Returning false omits an entry from discovery and prevents its mutation.
pub type DiscoveryPolicy = Arc<dyn Fn(&Caller, DiscoveryAction, &Endpoint) -> bool + Send + Sync>;

#[derive(Clone)]
struct Entry {
    caller: Caller,
    endpoint: Endpoint,
}

struct Directory {
    entries: Mutex<HashMap<String, Entry>>,
    policy: DiscoveryPolicy,
}

impl Directory {
    fn caller(details: &HashMap<String, Value>) -> Result<Caller, Error> {
        let id = details
            .get("caller")
            .and_then(Value::integer)
            .filter(|id| *id > 0)
            .ok_or_else(|| Error::new("caller disclosure is required"))?;
        let text = |key| match details.get(key) {
            Some(Value::Str(value)) if !value.is_empty() => Ok(value.clone()),
            _ => Err(Error::new(format!("{key} disclosure is required"))),
        };
        Ok(Caller {
            session_id: id,
            authid: text("caller_authid")?,
            authrole: text("caller_authrole")?,
        })
    }

    fn apply(&self, method: &str, args: &[Value], caller: Caller) -> RpcResult {
        match method {
            "advertise" => {
                let endpoint = match args.first().map(Endpoint::from_value) {
                    Some(Ok(endpoint)) => endpoint,
                    _ => return RpcResult::error("wamp.error.invalid_argument"),
                };
                if !(self.policy)(&caller, DiscoveryAction::Advertise, &endpoint) {
                    return RpcResult::error("wamp.error.not_authorized");
                }
                let mut entries = self.entries.lock().unwrap();
                if entries
                    .get(&endpoint.id)
                    .is_some_and(|entry| entry.caller.authid != caller.authid)
                {
                    return RpcResult::error("wamp.error.not_authorized");
                }
                let value = endpoint.to_value();
                entries.insert(endpoint.id.clone(), Entry { caller, endpoint });
                RpcResult::arg(value)
            }
            "withdraw" | "get" => {
                let Some(Value::Str(id)) = args.first() else {
                    return RpcResult::error("wamp.error.invalid_argument");
                };
                let Some(entry) = self.entries.lock().unwrap().get(id).cloned() else {
                    return RpcResult::error("wamp.error.no_such_object");
                };
                let action = if method == "get" {
                    DiscoveryAction::Discover
                } else {
                    DiscoveryAction::Withdraw
                };
                if !(self.policy)(&caller, action, &entry.endpoint)
                    || (method == "withdraw" && entry.caller.authid != caller.authid)
                {
                    return RpcResult::error("wamp.error.not_authorized");
                }
                let value = entry.endpoint.to_value();
                if method == "withdraw" {
                    let mut entries = self.entries.lock().unwrap();
                    if !entries.get(id).is_some_and(|current| {
                        current.caller.session_id == entry.caller.session_id && current.endpoint == entry.endpoint
                    }) {
                        return RpcResult::error("wamp.error.conflict");
                    }
                    entries.remove(id);
                }
                RpcResult::arg(value)
            }
            "list" => {
                let snapshot = self
                    .entries
                    .lock()
                    .unwrap()
                    .values()
                    .map(|entry| entry.endpoint.clone())
                    .collect::<Vec<_>>();
                let mut endpoints = snapshot
                    .into_iter()
                    .filter(|endpoint| (self.policy)(&caller, DiscoveryAction::Discover, endpoint))
                    .collect::<Vec<_>>();
                endpoints.sort_by(|left, right| left.id.cmp(&right.id));
                RpcResult::arg(Value::List(endpoints.iter().map(Endpoint::to_value).collect()))
            }
            _ => RpcResult::error("wamp.error.no_such_procedure"),
        }
    }
}

/// Hosts discovery under an application-selected URI prefix. Explicit closure
/// unregisters its procedures; dropping it also schedules cleanup.
pub struct DiscoveryService {
    session: Session,
    registrations: AsyncMutex<Vec<i64>>,
    subscription: AsyncMutex<Option<Subscription>>,
    executor: tokio::runtime::Handle,
}

impl DiscoveryService {
    pub async fn start(session: Session, prefix: &str, policy: DiscoveryPolicy) -> Result<Self, Error> {
        validate_prefix(prefix)?;
        let directory = Arc::new(Directory {
            entries: Mutex::new(HashMap::new()),
            policy,
        });
        let service = Self {
            session: session.clone(),
            registrations: AsyncMutex::new(Vec::new()),
            subscription: AsyncMutex::new(None),
            executor: tokio::runtime::Handle::current(),
        };
        for method in ["advertise", "withdraw", "get", "list"] {
            let directory = directory.clone();
            let response = session
                .execute_register(RegisterRequest::new(format!("{prefix}.{method}"), move |invocation| {
                    let directory = directory.clone();
                    async move {
                        let caller = match Directory::caller(&invocation.details) {
                            Ok(caller) => caller,
                            Err(_) => return RpcResult::error("wamp.error.not_authorized"),
                        };
                        directory.apply(method, &invocation.args, caller)
                    }
                }))
                .await?;
            if let Some(error) = response.error {
                return Err(Error::application(error));
            }
            service.registrations.lock().await.push(response.registration_id);
        }
        let cleanup = directory.clone();
        let subscription = session
            .subscribe_handle(SubscribeRequest::new("wamp.session.on_leave", move |event| {
                let cleanup = cleanup.clone();
                async move {
                    if let Some(id) = event.args.first().and_then(Value::integer) {
                        cleanup
                            .entries
                            .lock()
                            .unwrap()
                            .retain(|_, entry| entry.caller.session_id != id);
                    }
                }
            }))
            .await?;
        *service.subscription.lock().await = Some(subscription);
        Ok(service)
    }

    pub async fn close(&self) -> Result<(), Error> {
        let mut failure = None;
        for id in self.registrations.lock().await.drain(..) {
            match self.session.unregister(id).await {
                Ok(Some(error)) => {
                    failure.get_or_insert(Error::application(error));
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
                _ => {}
            }
        }
        if let Some(subscription) = self.subscription.lock().await.take() {
            match subscription.unsubscribe().await {
                Ok(Some(error)) => {
                    failure.get_or_insert(Error::application(error));
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
                _ => {}
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for DiscoveryService {
    fn drop(&mut self) {
        let session = self.session.clone();
        let registrations = std::mem::take(self.registrations.get_mut());
        let subscription = self.subscription.get_mut().take();
        self.executor.spawn(async move {
            for id in registrations {
                let _ = session.unregister(id).await;
            }
            if let Some(subscription) = subscription {
                let _ = subscription.unsubscribe().await;
            }
        });
    }
}

/// A directory client; connection selection and trust remain application policy.
pub struct DiscoveryClient {
    session: Session,
    prefix: String,
}

impl DiscoveryClient {
    pub fn new(session: Session, prefix: impl Into<String>) -> Result<Self, Error> {
        let prefix = prefix.into();
        validate_prefix(&prefix)?;
        Ok(Self { session, prefix })
    }

    pub async fn request(&self, method: &str, args: Vec<Value>) -> Result<Value, Error> {
        if !matches!(method, "advertise" | "withdraw" | "get" | "list") {
            return Err(Error::new("unknown discovery operation"));
        }
        if method == "advertise" {
            Endpoint::from_value(args.first().ok_or_else(|| Error::new("endpoint is required"))?)?;
        }
        let response = self
            .session
            .execute_call(CallRequest::new(format!("{}.{method}", self.prefix)).args(args))
            .await?;
        if let Some(error) = response.error {
            return Err(Error::application(error));
        }
        let value = response
            .args
            .and_then(|args| args.into_iter().next())
            .ok_or_else(|| Error::new("directory returned no result"))?;
        if method == "list" {
            let Value::List(values) = value else {
                return Err(Error::new("directory returned an invalid endpoint list"));
            };
            let endpoints = values
                .iter()
                .map(|value| Endpoint::from_value(value).map(|endpoint| endpoint.to_value()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(endpoints))
        } else {
            Ok(Endpoint::from_value(&value)?.to_value())
        }
    }
}

fn validate_prefix(prefix: &str) -> Result<(), Error> {
    if prefix.is_empty()
        || prefix
            .split('.')
            .any(|part| part.is_empty() || part.chars().any(char::is_whitespace))
    {
        return Err(Error::new("discovery URI prefix is invalid"));
    }
    Ok(())
}
