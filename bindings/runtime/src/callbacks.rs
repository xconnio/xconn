//! Foreign authentication, serialization, peer and callback adapters.
use crate::{
    ByteArray, Result, Value, id,
    resources::{Output, Resource, get_resource},
    values::{dictionary, duration_field, handle_field, object_value, string_field},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use xconn_core::{
    CBORSerializerSpec, JSONSerializerSpec, MsgPackSerializerSpec, SerializerSpec,
    wampproto::authenticators::{
        anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator, cryptosign::CryptoSignAuthenticator,
        ticket::TicketAuthenticator, wampcra::WAMPCRAAuthenticator,
    },
};

#[derive(Debug)]
pub(super) struct ForeignCallbacks {
    events: mpsc::Sender<Value>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<Value>>,
    pending: Mutex<HashMap<u64, std::sync::mpsc::SyncSender<Result<Value>>>>,
    closed: tokio::sync::watch::Sender<bool>,
}

impl ForeignCallbacks {
    fn new() -> Self {
        let (events, receiver) = mpsc::channel(256);
        Self {
            events,
            receiver: tokio::sync::Mutex::new(receiver),
            pending: Mutex::new(HashMap::new()),
            closed: tokio::sync::watch::channel(false).0,
        }
    }

    pub(super) fn call(&self, method: &str, value: Value) -> Result<Value> {
        let id = id();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        {
            let mut pending = self.pending.lock().unwrap();

            if *self.closed.borrow() {
                return Err("callback channel closed".into());
            }

            pending.insert(id, sender);
        }

        struct PendingCallback<'a> {
            callbacks: &'a ForeignCallbacks,
            id: u64,
        }

        impl Drop for PendingCallback<'_> {
            fn drop(&mut self) {
                self.callbacks.pending.lock().unwrap().remove(&self.id);
            }
        }

        let _pending = PendingCallback { callbacks: self, id };
        let event = object_value([
            ("id", Value::UInt(id)),
            ("method", Value::Str(method.into())),
            ("value", value),
        ]);
        let mut closed = self.closed.subscribe();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                tokio::select! {
                    biased;
                    _ = closed.wait_for(|closed| *closed) => Err("callback channel closed".to_string()),
                    result = self.events.send(event) => result.map_err(|_| "callback receiver closed".to_string()),
                }
            })?;
            receiver.recv().map_err(|_| "callback cancelled".to_string())?
        })
    }

    pub(super) fn close(&self) {
        self.closed.send_replace(true);

        for (_, sender) in self.pending.lock().unwrap().drain() {
            let _ = sender.send(Err("callback channel closed".into()));
        }
    }
}

pub(super) fn get_callbacks(handle: u64) -> Result<Arc<ForeignCallbacks>> {
    match get_resource(handle)?.as_ref() {
        Resource::Callbacks(callbacks) => Ok(callbacks.clone()),
        _ => Err("expected callbacks".into()),
    }
}

#[derive(Debug, Clone)]
struct ForeignAuthenticator {
    method: String,
    authid: String,
    extra: HashMap<String, Value>,
    callbacks: Arc<ForeignCallbacks>,
}

impl ClientAuthenticator for ForeignAuthenticator {
    fn auth_method(&self) -> String {
        self.method.clone()
    }

    fn authid(&self) -> String {
        self.authid.clone()
    }

    fn auth_extra(&self) -> HashMap<String, Value> {
        self.extra.clone()
    }

    fn authenticate(
        &self,
        challenge: &xconn_core::wampproto::messages::challenge::Challenge,
    ) -> std::result::Result<
        xconn_core::wampproto::messages::authenticate::Authenticate,
        xconn_core::wampproto::messages::types::Error,
    > {
        let error = |message: String| xconn_core::wampproto::messages::types::Error::new(message);
        let response = self
            .callbacks
            .call(
                "authenticate",
                object_value([
                    ("method", Value::Str(challenge.auth_method.clone())),
                    ("extra", Value::Dict(challenge.extra.clone())),
                ]),
            )
            .map_err(error)?;
        let response = dictionary(&response).map_err(error)?;
        Ok(xconn_core::wampproto::messages::authenticate::Authenticate {
            signature: string_field(&response, "signature").map_err(error)?,
            extra: dictionary(response.get("extra").unwrap_or(&Value::Null)).map_err(error)?,
        })
    }
}

#[derive(Clone)]
struct ForeignServerAuthenticator {
    methods: Vec<String>,
    callbacks: Arc<ForeignCallbacks>,
}

impl xconn_core::wampproto::authenticators::server::ServerAuthenticator for ForeignServerAuthenticator {
    fn methods(&self) -> Vec<String> {
        self.methods.clone()
    }

    fn authenticate(
        &self,
        request: &xconn_core::wampproto::authenticators::server::AuthRequest,
    ) -> std::result::Result<
        xconn_core::wampproto::authenticators::server::AuthResponse,
        xconn_core::wampproto::messages::types::Error,
    > {
        use xconn_core::wampproto::authenticators::server::{AuthResponse, CRAKey, Salt};
        use xconn_core::wampproto::authenticators::wampcra::validate_cra_parameters;
        let error = |message: String| xconn_core::wampproto::messages::types::Error::new(message);
        let response = self
            .callbacks
            .call(
                "server_authenticate",
                object_value([
                    ("method", Value::Str(request.method.clone())),
                    ("realm", Value::Str(request.realm.clone())),
                    ("authid", Value::Str(request.authid.clone())),
                    ("extra", Value::Dict(request.extra.clone())),
                    ("ticket", request.ticket.clone().map(Value::Str).unwrap_or(Value::Null)),
                    (
                        "public_key",
                        request.public_key.clone().map(Value::Str).unwrap_or(Value::Null),
                    ),
                ]),
            )
            .map_err(error)?;
        let response = dictionary(&response).map_err(error)?;
        let mut result = AuthResponse::new(
            string_field(&response, "authid").map_err(error)?,
            string_field(&response, "authrole").map_err(error)?,
        );
        result.ttl = duration_field(&response, "ttl", None)
            .map_err(error)?
            .unwrap_or_default();
        if let Some(Value::Str(secret)) = response.get("secret") {
            let salt = match response.get("salt") {
                Some(Value::Str(salt)) if !salt.is_empty() => Some(Salt {
                    salt: salt.clone(),
                    iterations: u32::try_from(handle_field(&response, "iterations").map_err(error)?)
                        .map_err(|_| error("iterations exceed u32".into()))?,
                    key_length: usize::try_from(handle_field(&response, "keylen").map_err(error)?)
                        .map_err(|_| error("key length exceeds usize".into()))?,
                }),
                _ => None,
            };
            if let Some(parameters) = &salt {
                validate_cra_parameters(
                    i64::from(parameters.iterations),
                    i64::try_from(parameters.key_length).map_err(|_| error("invalid key length".into()))?,
                )?;
            }
            // Foreign policies supply the signing key, already derived for salted CRA.
            result.cra = Some(CRAKey {
                key: secret.as_bytes().to_vec(),
                salt,
            });
        }

        Ok(result)
    }
}

pub(super) fn server_authentication(
    fields: &HashMap<String, Value>,
) -> Result<Option<xconn_core::async_::router::AuthenticationFactory>> {
    let Some(Value::Dict(auth)) = fields.get("server_auth") else {
        return Ok(None);
    };
    let methods = match auth.get("methods") {
        Some(Value::List(methods)) => methods
            .iter()
            .map(|value| match value {
                Value::Str(method) => Ok(method.clone()),
                _ => Err("authentication methods must be strings".to_string()),
            })
            .collect::<Result<Vec<_>>>()?,
        _ => return Err("authentication methods must be an array".into()),
    };
    let callbacks = get_callbacks(handle_field(auth, "callback")?)?;
    Ok(Some(Arc::new(move || {
        Box::new(ForeignServerAuthenticator {
            methods: methods.clone(),
            callbacks: callbacks.clone(),
        })
    })))
}

#[derive(Debug)]
pub(super) struct ForeignPeer {
    pub(super) callbacks: Arc<ForeignCallbacks>,
}

impl ForeignPeer {
    async fn call(&self, method: &str, value: Value) -> std::result::Result<Value, xconn_core::Error> {
        let callbacks = self.callbacks.clone();
        let method = method.to_string();
        tokio::task::spawn_blocking(move || callbacks.call(&method, value))
            .await
            .map_err(|error| xconn_core::Error::new(error.to_string()))?
            .map_err(xconn_core::Error::new)
    }
}

#[async_trait::async_trait]
impl xconn_core::async_::peer::Peer for ForeignPeer {
    fn kind(&self) -> xconn_core::TransportType {
        0
    }

    async fn read(&self) -> std::result::Result<Vec<u8>, xconn_core::Error> {
        match self.call("peer_read", Value::Null).await? {
            Value::Bytes(bytes) => Ok(bytes.0),
            _ => Err(xconn_core::Error::new("peer Read must return bytes")),
        }
    }

    async fn write(&self, bytes: Vec<u8>) -> std::result::Result<(), xconn_core::Error> {
        self.call("peer_write", Value::Bytes(ByteArray(bytes))).await?;
        Ok(())
    }

    async fn close(&self) -> std::result::Result<(), xconn_core::Error> {
        self.call("peer_close", Value::Null).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct SharedPeer(pub(super) Arc<dyn xconn_core::async_::peer::Peer>);

#[async_trait::async_trait]
impl xconn_core::async_::peer::Peer for SharedPeer {
    fn kind(&self) -> xconn_core::TransportType {
        self.0.kind()
    }

    #[cfg(feature = "rpc-streams")]
    fn stream_opener(&self) -> Option<xconn_core::async_::peer::StreamOpener> {
        self.0.stream_opener()
    }

    fn try_write(&self, data: Vec<u8>) -> std::result::Result<bool, xconn_core::Error> {
        self.0.try_write(data)
    }

    async fn read(&self) -> std::result::Result<Vec<u8>, xconn_core::Error> {
        self.0.read().await
    }

    async fn write(&self, bytes: Vec<u8>) -> std::result::Result<(), xconn_core::Error> {
        self.0.write(bytes).await
    }

    async fn close(&self) -> std::result::Result<(), xconn_core::Error> {
        self.0.close().await
    }
}

#[derive(Debug, Clone)]
struct ForeignSerializer {
    name: String,
    callbacks: Arc<ForeignCallbacks>,
    is_static: bool,
}

impl xconn_core::wampproto::serializers::serializer::Serializer for ForeignSerializer {
    fn serialize(
        &self,
        message: &dyn xconn_core::wampproto::messages::message::Message,
    ) -> std::result::Result<Vec<u8>, xconn_core::wampproto::messages::types::Error> {
        let error = |message: String| xconn_core::wampproto::messages::types::Error::new(message);

        match self
            .callbacks
            .call("serialize", Value::List(message.marshal()))
            .map_err(error)?
        {
            Value::Bytes(bytes) => Ok(bytes.0),
            Value::Str(text) => Ok(text.into_bytes()),
            _ => Err(error("serializer must return bytes or text".into())),
        }
    }

    fn deserialize(
        &self,
        payload: Vec<u8>,
    ) -> std::result::Result<
        Box<dyn xconn_core::wampproto::messages::message::Message>,
        xconn_core::wampproto::messages::types::Error,
    > {
        let error = |message: String| xconn_core::wampproto::messages::types::Error::new(message);

        match self
            .callbacks
            .call("deserialize", Value::Bytes(ByteArray(payload)))
            .map_err(error)?
        {
            Value::List(fields) => xconn_core::wampproto::serializers::helpers::to_typed_message(fields),
            _ => Err(error("deserializer must return a WAMP message".into())),
        }
    }

    fn is_static(&self) -> bool {
        self.is_static
    }
}

impl xconn_core::_SerializerSpec for ForeignSerializer {
    fn subprotocol(&self) -> String {
        format!("wamp.2.{}", self.name)
    }

    fn serializer_id(&self) -> xconn_core::wampproto::transports::rawsocket::SerializerID {
        use xconn_core::wampproto::transports::rawsocket::SerializerID;

        match self.name.as_str() {
            "json" => SerializerID::JSON,
            "msgpack" => SerializerID::MSGPACK,
            _ => SerializerID::CBOR,
        }
    }

    fn serializer(&self) -> Box<dyn xconn_core::wampproto::serializers::serializer::Serializer> {
        Box::new(self.clone())
    }

    fn is_binary(&self) -> bool {
        self.name != "json"
    }
}

pub(super) fn codec(fields: &HashMap<String, Value>) -> Result<Box<dyn SerializerSpec>> {
    match fields.get("serializer") {
        Some(Value::Dict(config)) => {
            let name = string_field(config, "name")?;

            if !matches!(name.as_str(), "json" | "msgpack" | "cbor") {
                return Err("unknown serializer".into());
            }

            Ok(Box::new(ForeignSerializer {
                name,
                callbacks: get_callbacks(handle_field(config, "callback")?)?,
                is_static: config.get("static") == Some(&Value::Bool(true)),
            }))
        }
        None => Ok(Box::new(JSONSerializerSpec)),
        Some(Value::Str(v)) => match v.as_str() {
            "json" => Ok(Box::new(JSONSerializerSpec)),
            "cbor" => Ok(Box::new(CBORSerializerSpec)),
            "msgpack" => Ok(Box::new(MsgPackSerializerSpec)),
            _ => Err("unknown serializer".into()),
        },
        _ => Err("serializer must be a string".into()),
    }
}

pub(super) fn authenticator(fields: &HashMap<String, Value>) -> Result<Box<dyn ClientAuthenticator + Send + Sync>> {
    let auth = dictionary(fields.get("auth").unwrap_or(&Value::Null))?;
    let authid = auth
        .get("authid")
        .and_then(|v| if let Value::Str(v) = v { Some(v.as_str()) } else { None })
        .unwrap_or("");
    let method = auth
        .get("method")
        .and_then(|v| if let Value::Str(v) = v { Some(v.as_str()) } else { None })
        .unwrap_or("anonymous");
    if let Some(callback) = auth.get("callback") {
        let fields = HashMap::from([("handle".into(), callback.clone())]);
        return Ok(Box::new(ForeignAuthenticator {
            method: method.into(),
            authid: authid.into(),
            extra: dictionary(auth.get("extra").unwrap_or(&Value::Null))?,
            callbacks: get_callbacks(handle_field(&fields, "handle")?)?,
        }));
    }

    Ok(match method {
        "anonymous" => Box::new(AnonymousAuthenticator::new(authid, HashMap::new())),
        "ticket" => Box::new(TicketAuthenticator::new(
            authid,
            &string_field(&auth, "secret")?,
            HashMap::new(),
        )),
        "wampcra" => Box::new(WAMPCRAAuthenticator::new(
            authid,
            &string_field(&auth, "secret")?,
            HashMap::new(),
        )),
        "cryptosign" => Box::new(
            CryptoSignAuthenticator::try_new(authid, &string_field(&auth, "secret")?, HashMap::new())
                .map_err(|e| e.message)?,
        ),
        _ => return Err("unknown authentication method".into()),
    })
}

pub(super) async fn dispatch(method: &str, f: HashMap<String, Value>) -> Result<Output> {
    let plain = |value| Ok(Output::plain(value));

    match method {
        "callbacks_new" => Ok(Output::resource(
            Resource::Callbacks(Arc::new(ForeignCallbacks::new())),
            HashMap::new(),
        )),
        "callbacks_next" => {
            let callbacks = get_callbacks(handle_field(&f, "handle")?)?;
            let mut receiver = callbacks.receiver.lock().await;
            let mut closed = callbacks.closed.subscribe();
            tokio::select! {
                value = receiver.recv() => plain(value.unwrap_or(Value::Null)),
                _ = closed.wait_for(|value| *value) => plain(Value::Null),
            }
        }
        "callbacks_reply" => {
            let callbacks = get_callbacks(handle_field(&f, "handle")?)?;
            let sender = callbacks
                .pending
                .lock()
                .unwrap()
                .remove(&handle_field(&f, "id")?)
                .ok_or("callback already finished")?;
            let result = match f.get("error") {
                Some(Value::Str(error)) => Err(error.clone()),
                _ => Ok(f.get("value").cloned().unwrap_or(Value::Null)),
            };
            let _ = sender.send(result);
            plain(Value::Null)
        }
        _ => Err(format!("unknown callbacks operation {method}")),
    }
}

/// Apply listener-specific authentication and heartbeat policy to a router clone.
pub(super) fn server_router(fields: &HashMap<String, Value>) -> Result<xconn_core::async_::router::Router> {
    let mut router = crate::resources::get_router(crate::values::handle_field(fields, "handle")?)?;
    if let Some(authentication) = server_authentication(fields)? {
        router = router.with_authentication(authentication);
    }
    if let Some(interval) = crate::values::duration_field(fields, "ping_interval", None)? {
        let timeout = crate::values::duration_field(fields, "ping_timeout", Some(std::time::Duration::from_secs(10)))?
            .ok_or("keepalive timeout is required")?;
        router = router.with_keepalive(interval, timeout).map_err(|e| e.to_string())?;
    }
    Ok(router)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_dispatch_releases_pending_callback() {
        let callbacks = ForeignCallbacks::new();
        callbacks.receiver.lock().await.close();
        assert!(callbacks.call("authenticate", Value::Null).is_err());
        assert!(callbacks.pending.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closing_a_saturated_queue_wakes_dispatch() {
        let callbacks = Arc::new(ForeignCallbacks::new());
        for _ in 0..256 {
            callbacks.events.try_send(Value::Null).unwrap();
        }
        let pending = callbacks.clone();
        let task = tokio::spawn(async move { pending.call("authenticate", Value::Null) });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while callbacks.pending.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        callbacks.close();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(callbacks.pending.lock().unwrap().is_empty());
    }
}
