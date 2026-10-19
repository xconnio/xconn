use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use wampproto::messages::call::Call;
use wampproto::messages::publish::Publish;
pub use wampproto::messages::types::Value;
use wampproto::serializers::cbor::CBORSerializer;
use wampproto::serializers::json::JSONSerializer;
use wampproto::serializers::msgpack::MsgPackSerializer;
use wampproto::serializers::serializer::Serializer;
use wampproto::transports::rawsocket::SerializerID;

#[derive(Debug)]
pub struct Error {
    pub message: String,
    pub application: Option<WampError>,
}

impl Error {
    pub fn application(error: WampError) -> Self {
        Self {
            message: error.uri.clone(),
            application: Some(error),
        }
    }

    pub fn new<T: Into<String>>(msg: T) -> Self {
        Error {
            message: msg.into(),
            application: None,
        }
    }
}

// Implement Display so errors print nicely
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

// Implement the std::error::Error trait
impl std::error::Error for Error {}

#[derive(Debug, Clone)]
pub struct SessionDetails {
    id: i64,
    realm: String,
    authid: String,
    auth_role: String,
    auth_method: String,
    auth_extra: HashMap<String, Value>,
    roles: HashMap<String, Value>,
    created_at: String,
}

impl SessionDetails {
    pub fn new(id: i64, realm: String, authid: String, auth_role: String) -> Self {
        Self {
            id,
            realm,
            authid,
            auth_role,
            auth_method: String::new(),
            auth_extra: HashMap::new(),
            roles: HashMap::new(),
            created_at: String::new(),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn realm(&self) -> String {
        self.realm.clone()
    }

    pub fn authid(&self) -> String {
        self.authid.clone()
    }

    pub fn auth_role(&self) -> String {
        self.auth_role.clone()
    }
}

pub trait _SerializerSpec: Debug + Sync + Send {
    fn subprotocol(&self) -> String;

    fn serializer_id(&self) -> SerializerID;

    fn serializer(&self) -> Box<dyn Serializer>;

    fn is_binary(&self) -> bool;
}

pub trait SerializerSpec: _SerializerSpec {
    fn clone_box(&self) -> Box<dyn SerializerSpec>;
}

impl<T> SerializerSpec for T
where
    T: _SerializerSpec + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn SerializerSpec> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn SerializerSpec> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Debug, Clone, Default)]
pub struct JSONSerializerSpec;

impl _SerializerSpec for JSONSerializerSpec {
    fn subprotocol(&self) -> String {
        "wamp.2.json".to_string()
    }

    fn serializer_id(&self) -> SerializerID {
        SerializerID::JSON
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(JSONSerializer {})
    }

    fn is_binary(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Default)]
pub struct CBORSerializerSpec;

impl _SerializerSpec for CBORSerializerSpec {
    fn subprotocol(&self) -> String {
        "wamp.2.cbor".to_string()
    }

    fn serializer_id(&self) -> SerializerID {
        SerializerID::CBOR
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(CBORSerializer {})
    }

    fn is_binary(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default)]
pub struct MsgPackSerializerSpec;

impl _SerializerSpec for MsgPackSerializerSpec {
    fn subprotocol(&self) -> String {
        "wamp.2.msgpack".to_string()
    }

    fn serializer_id(&self) -> SerializerID {
        SerializerID::MSGPACK
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(MsgPackSerializer {})
    }

    fn is_binary(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct _OutgoingRequest {
    uri: String,
    options: HashMap<String, Value>,
    args: Option<Vec<Value>>,
    kwargs: Option<HashMap<String, Value>>,
}

impl _OutgoingRequest {
    pub fn new<S: Into<String>>(uri: S) -> Self {
        Self {
            uri: uri.into(),
            args: Default::default(),
            kwargs: Default::default(),
            options: Default::default(),
        }
    }

    pub fn arg<T: Into<Value>>(mut self, arg: T) -> Self {
        self.args.get_or_insert_with(Vec::new).push(arg.into());
        self
    }

    pub fn args(mut self, args: Vec<Value>) -> Self {
        self.args = Some(args);
        self
    }

    pub fn kwarg<T: Into<Value>>(mut self, key: &str, value: T) -> Self {
        self.kwargs
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string(), value.into());
        self
    }

    pub fn kwargs(mut self, kwargs: HashMap<String, Value>) -> Self {
        self.kwargs = Some(kwargs);
        self
    }

    pub(crate) fn request_options(&self) -> &HashMap<String, Value> {
        &self.options
    }

    pub fn option<T: Into<Value>>(mut self, key: &str, value: T) -> Self {
        self.options.insert(key.to_string(), value.into());
        self
    }

    pub fn options(mut self, options: HashMap<String, Value>) -> Self {
        self.options = options;
        self
    }
}

pub type CallRequest = _OutgoingRequest;
pub type PublishRequest = _OutgoingRequest;

impl CallRequest {
    pub(crate) fn to_call(&self, request_id: i64) -> Call {
        Call {
            request_id,
            options: self.options.clone(),
            procedure: self.uri.clone(),
            args: self.args.clone(),
            kwargs: self.kwargs.clone(),
        }
    }
}

impl PublishRequest {
    pub(crate) fn to_publish(&self, request_id: i64) -> Publish {
        Publish {
            request_id,
            options: self.options.clone(),
            topic: self.uri.clone(),
            args: self.args.clone(),
            kwargs: self.kwargs.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct _IncomingRequest {
    pub args: Vec<Value>,
    pub kwargs: HashMap<String, Value>,
    pub details: HashMap<String, Value>,
}

pub type Invocation = _IncomingRequest;
pub type Event = _IncomingRequest;

/// A payload shared by invocation replies and received RPC results.
#[derive(Debug, Default)]
pub struct Result {
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
    pub details: HashMap<String, Value>,
    pub error: Option<WampError>,
}

impl Result {
    pub fn is_progress(&self) -> bool {
        self.details.get("progress") == Some(&Value::Bool(true))
    }

    pub fn new(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Self {
        Self {
            args: Some(args),
            kwargs: Some(kwargs),
            ..Default::default()
        }
    }

    pub fn args(args: Vec<Value>) -> Self {
        Self {
            args: Some(args),
            ..Default::default()
        }
    }

    pub fn arg(value: impl Into<Value>) -> Self {
        Self::args(vec![value.into()])
    }

    pub fn kwargs(kwargs: HashMap<String, Value>) -> Self {
        Self {
            kwargs: Some(kwargs),
            ..Default::default()
        }
    }

    pub fn kwarg(key: &str, value: impl Into<Value>) -> Self {
        Self::kwargs(HashMap::from([(key.to_owned(), value.into())]))
    }

    pub fn error(uri: &str) -> Self {
        Self {
            error: Some(WampError {
                uri: uri.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default)]
pub struct SubscribeResponse {
    pub subscription_id: i64,
    pub error: Option<WampError>,
}

#[derive(Debug, Default)]
pub struct WampError {
    pub uri: String,
    pub args: Option<Vec<Value>>,
    pub kwargs: Option<HashMap<String, Value>>,
}

#[derive(Debug, Default)]
pub struct PublishResponse {
    pub error: Option<WampError>,
}

#[derive(Debug, Default)]
pub struct RegisterResponse {
    pub registration_id: i64,
    pub error: Option<WampError>,
}

pub type TransportType = usize;
pub const TRANSPORT_WEB_SOCKET: TransportType = 1;
pub const TRANSPORT_RAW_SOCKET: TransportType = 2;

impl From<wampproto::messages::types::Error> for Error {
    fn from(value: wampproto::messages::types::Error) -> Self {
        Self::new(value.message)
    }
}

impl SessionDetails {
    pub fn from_protocol(details: &wampproto::types::SessionDetails) -> Self {
        Self {
            id: details.id,
            realm: details.realm.clone(),
            authid: details.authid.clone(),
            auth_role: details.auth_role.clone(),
            auth_method: details.auth_method.clone(),
            auth_extra: details.auth_extra.clone(),
            roles: details.roles.clone(),
            created_at: details.created_at.clone(),
        }
    }

    pub fn auth_method(&self) -> &str {
        &self.auth_method
    }

    pub fn auth_extra(&self) -> &HashMap<String, Value> {
        &self.auth_extra
    }

    pub fn roles(&self) -> &HashMap<String, Value> {
        &self.roles
    }

    pub fn created_at(&self) -> &str {
        &self.created_at
    }
}

impl Result {
    /// Leave a progressive invocation open without producing a final reply.
    pub fn no_result() -> Self {
        Self::error("io.xconn.no_result")
    }
}

/// Options for an owned progressive RPC. Zero timeout disables the deadline.
#[derive(Debug, Clone, Copy)]
pub struct StreamOptions {
    pub progressive_input: bool,
    /// Request intermediate results in addition to the final result.
    pub progressive_results: bool,
    pub buffer_size: usize,
    pub timeout: Option<std::time::Duration>,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            progressive_input: false,
            progressive_results: true,
            buffer_size: 32,
            timeout: None,
        }
    }
}
