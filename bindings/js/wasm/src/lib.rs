//! Browser value and ownership adapters. WAMP algorithms remain in wampproto.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use wampproto::{
    authenticators::{
        anonymous::AnonymousAuthenticator, authenticator::ClientAuthenticator, ticket::TicketAuthenticator,
    },
    idgen::SessionScopeIDGenerator,
    joiner::Joiner,
    messages::{
        authenticate::Authenticate,
        challenge::Challenge,
        message::Message,
        schema,
        types::{Error, Value},
    },
    serializers::{helpers::to_message, json::JSONSerializer, serializer::Serializer},
    transports::rawsocket,
};
use wasm_bindgen::{JsCast, prelude::*};

fn error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn fields(value: JsValue) -> Result<HashMap<String, Value>, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(HashMap::new());
    }

    serde_wasm_bindgen::from_value(value).map_err(error)
}

fn parse(wire: JsValue) -> Result<Box<dyn Message>, JsValue> {
    let mut wire: Vec<Value> = serde_wasm_bindgen::from_value(wire).map_err(error)?;
    normalize_ids(&mut wire);
    to_message(wire).map_err(error)
}

// JS can exactly represent WAMP's inclusive upper ID bound (2^53), even though
// serde-wasm-bindgen represents that Number as f64. Payload integers stay lossless BigInts.
fn normalize_ids(wire: &mut [Value]) {
    let Some(Value::Int(code)) = wire.first() else {
        return;
    };
    let Some(layout) = schema::schema(*code) else {
        return;
    };

    for field in layout.fields {
        if matches!(field.rule, schema::Rule::Id)
            && let Some(value @ Value::Float(_)) = wire.get_mut(field.index)
            && let Value::Float(number) = *value
            && number.is_finite()
            && number.fract() == 0.0
            && number.abs() <= schema::MAX_ID as f64
        {
            *value = Value::Int(number as i64);
        }
    }
}

fn describe(message: &dyn Message) -> Result<JsValue, JsValue> {
    let Value::Int(code) = message.message_type() else {
        return Err(error("invalid message code"));
    };
    let layout = schema::schema(code).ok_or_else(|| error("unknown message"))?;
    let mut wire = message.marshal();
    let mut fields = layout
        .parse(&Value::List(wire.clone()))
        .map_err(|errors| error(errors.join(", ")))?;
    for field in layout.fields {
        if matches!(field.rule, schema::Rule::Id)
            && let Some(Value::Int(id)) = fields.get(field.name)
        {
            let number = Value::Float(*id as f64);
            wire[field.index] = number.clone();
            fields.insert(field.name.into(), number);
        }
    }

    let wire = Value::List(wire);
    to_js(&Value::Dict(HashMap::from([
        ("code".into(), Value::Int(code)),
        ("wire".into(), wire),
        ("fields".into(), Value::Dict(fields.into_iter().collect())),
    ])))
}

fn serializer(name: &str) -> Result<Box<dyn Serializer>, JsValue> {
    match name {
        "json" => Ok(Box::new(JSONSerializer {})),
        #[cfg(feature = "binary-codecs")]
        "cbor" => Ok(Box::new(wampproto::serializers::cbor::CBORSerializer {})),
        #[cfg(feature = "binary-codecs")]
        "msgpack" => Ok(Box::new(wampproto::serializers::msgpack::MsgPackSerializer {})),
        _ => Err(error("serializer is not included in this build")),
    }
}

fn authenticator(
    method: &str,
    authid: &str,
    secret: &str,
    extra: HashMap<String, Value>,
) -> Result<Box<dyn ClientAuthenticator>, JsValue> {
    match method {
        "anonymous" => Ok(Box::new(AnonymousAuthenticator::new(authid, extra))),
        "ticket" => Ok(Box::new(TicketAuthenticator::new(authid, secret, extra))),
        #[cfg(feature = "wampcra")]
        "wampcra" => Ok(Box::new(wampproto::authenticators::wampcra::WAMPCRAAuthenticator::new(
            authid, secret, extra,
        ))),
        #[cfg(feature = "cryptosign")]
        "cryptosign" => Ok(Box::new(
            wampproto::authenticators::cryptosign::CryptoSignAuthenticator::try_new(authid, secret, extra)
                .map_err(error)?,
        )),
        _ => Err(error("authenticator is not included in this build")),
    }
}

#[wasm_bindgen]
pub struct NativeCodec(Box<dyn Serializer>);

#[wasm_bindgen]
impl NativeCodec {
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str) -> Result<Self, JsValue> {
        Ok(Self(serializer(name)?))
    }

    pub fn encode(&self, wire: JsValue) -> Result<Vec<u8>, JsValue> {
        self.0.serialize(parse(wire)?.as_ref()).map_err(error)
    }

    pub fn decode(&self, payload: &[u8]) -> Result<JsValue, JsValue> {
        describe(self.0.deserialize(payload.to_vec()).map_err(error)?.as_ref())
    }
}

#[wasm_bindgen]
pub fn make_message(code: u32, value: JsValue) -> Result<JsValue, JsValue> {
    let fields = fields(value)?;
    let schema = schema::schema(i64::from(code)).ok_or_else(|| error("unknown message"))?;
    let Value::List(mut wire) = schema.marshal(&Value::Null, &fields.into_iter().collect()) else {
        return Err(error("expected message array"));
    };
    normalize_ids(&mut wire);
    describe(to_message(wire).map_err(error)?.as_ref())
}

#[wasm_bindgen]
pub fn inspect_message(wire: JsValue) -> Result<JsValue, JsValue> {
    describe(parse(wire)?.as_ref())
}

#[wasm_bindgen]
pub fn has_binary_codecs() -> bool {
    cfg!(feature = "binary-codecs")
}

#[wasm_bindgen]
pub struct RequestIds(SessionScopeIDGenerator);

#[wasm_bindgen]
impl RequestIds {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self(SessionScopeIDGenerator::new())
    }

    pub fn next(&self) -> f64 {
        self.0.next_id() as f64
    }
}

impl Default for RequestIds {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
pub struct NativeAuthenticator(Box<dyn ClientAuthenticator>);

#[wasm_bindgen]
impl NativeAuthenticator {
    #[wasm_bindgen(constructor)]
    pub fn new(method: &str, authid: &str, secret: &str, extra: JsValue) -> Result<Self, JsValue> {
        Ok(Self(authenticator(method, authid, secret, fields(extra)?)?))
    }

    pub fn extra(&self) -> Result<JsValue, JsValue> {
        to_js(&Value::Dict(self.0.auth_extra()))
    }

    pub fn authenticate(&self, wire: JsValue) -> Result<JsValue, JsValue> {
        let message = parse(wire)?;
        let challenge = message
            .as_any()
            .downcast_ref::<Challenge>()
            .ok_or_else(|| error("expected CHALLENGE"))?;
        describe(&self.0.authenticate(challenge).map_err(error)?)
    }
}

type PendingAuthentication = Arc<Mutex<Option<(String, HashMap<String, Value>)>>>;

#[derive(Clone, Debug)]
struct HostAuthenticator {
    method: String,
    authid: String,
    extra: HashMap<String, Value>,
    response: PendingAuthentication,
}

impl ClientAuthenticator for HostAuthenticator {
    fn auth_method(&self) -> String {
        self.method.clone()
    }

    fn authid(&self) -> String {
        self.authid.clone()
    }

    fn auth_extra(&self) -> HashMap<String, Value> {
        self.extra.clone()
    }

    fn authenticate(&self, _: &Challenge) -> Result<Authenticate, Error> {
        let (signature, extra) = self
            .response
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::new("authentication callback has not completed"))?;
        Ok(Authenticate { signature, extra })
    }
}

#[wasm_bindgen]
pub struct ClientProtocol {
    joiner: Joiner,
    response: PendingAuthentication,
}

#[wasm_bindgen]
impl ClientProtocol {
    #[wasm_bindgen(constructor)]
    pub fn new(realm: &str, codec: &str, method: &str, authid: &str, extra: JsValue) -> Result<Self, JsValue> {
        let response = Arc::new(Mutex::new(None));
        let auth = HostAuthenticator {
            method: method.into(),
            authid: authid.into(),
            extra: fields(extra)?,
            response: response.clone(),
        };
        Ok(Self {
            joiner: Joiner::new(realm, serializer(codec)?, Box::new(auth)),
            response,
        })
    }

    pub fn hello(&mut self) -> Result<JsValue, JsValue> {
        describe(self.joiner.send_hello_message(None).as_ref())
    }

    pub fn check_challenge(&self) -> Result<(), JsValue> {
        self.joiner.check_challenge().map_err(error)
    }

    pub fn set_authentication(&self, wire: JsValue) -> Result<(), JsValue> {
        self.joiner.check_challenge().map_err(error)?;
        let message = parse(wire)?;
        let auth = message
            .as_any()
            .downcast_ref::<Authenticate>()
            .ok_or_else(|| error("expected AUTHENTICATE"))?;
        *self.response.lock().unwrap() = Some((auth.signature.clone(), auth.extra.clone()));
        Ok(())
    }

    pub fn receive(&mut self, wire: JsValue) -> Result<JsValue, JsValue> {
        match self.joiner.receive_message(parse(wire)?).map_err(error)? {
            Some(message) => describe(message.as_ref()),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    pub fn joined(&self) -> bool {
        self.joiner.session_details().is_ok()
    }

    pub fn details(&self) -> Result<JsValue, JsValue> {
        let details = self
            .joiner
            .session_details()
            .map_err(error)?
            .ok_or_else(|| error("not joined"))?;
        to_js(&Value::Dict(HashMap::from([
            ("sessionID".into(), Value::Float(details.id as f64)),
            ("realm".into(), Value::Str(details.realm.clone())),
            ("authid".into(), Value::Str(details.authid.clone())),
            ("authrole".into(), Value::Str(details.auth_role.clone())),
            ("authmethod".into(), Value::Str(details.auth_method.clone())),
            ("authextra".into(), Value::Dict(details.auth_extra.clone())),
            ("roles".into(), Value::Dict(details.roles.clone())),
        ])))
    }
}

fn to_js(value: &Value) -> Result<JsValue, JsValue> {
    const MAX_SAFE: i64 = (1_i64 << 53) - 1;
    Ok(match value {
        Value::Null => JsValue::NULL,
        Value::Bool(value) => JsValue::from_bool(*value),
        Value::Str(value) => JsValue::from_str(value),
        Value::Float(value) => JsValue::from_f64(*value),
        Value::Int(value) if (-MAX_SAFE..=MAX_SAFE).contains(value) => JsValue::from_f64(*value as f64),
        Value::UInt(value) if *value <= MAX_SAFE as u64 => JsValue::from_f64(*value as f64),
        Value::Int(value) => js_sys::BigInt::from(*value).into(),
        Value::UInt(value) => js_sys::BigInt::from(*value).into(),
        Value::Bytes(value) => js_sys::Uint8Array::from(value.0.as_slice()).into(),
        Value::List(values) => {
            let array = js_sys::Array::new();

            for value in values {
                array.push(&to_js(value)?);
            }

            array.into()
        }
        Value::Dict(values) => {
            let object = js_sys::Object::create(JsValue::NULL.unchecked_ref::<js_sys::Object>());

            for (key, value) in values {
                js_sys::Reflect::set(&object, &JsValue::from_str(key), &to_js(value)?)?;
            }

            object.into()
        }
    })
}

/// Static adapters allocate no persistent WASM resource handles.
#[cfg(feature = "packet-crypto")]
#[wasm_bindgen]
pub struct PacketCrypto;

#[cfg(feature = "packet-crypto")]
#[wasm_bindgen]
impl PacketCrypto {
    pub fn key_pair() -> Result<JsValue, JsValue> {
        let pair = xconn_crypto::generate_key_pair().map_err(error)?;
        to_js(&Value::Dict(HashMap::from([
            ("public".into(), Value::from(pair.public.to_vec())),
            ("private".into(), Value::from(pair.private.to_vec())),
        ])))
    }

    pub fn shared_secret(private: &[u8], public: &[u8]) -> Result<Vec<u8>, JsValue> {
        xconn_crypto::shared_secret(private, public)
            .map(|secret| secret.to_vec())
            .map_err(error)
    }

    pub fn derive_key(secret: &[u8], info: &[u8]) -> Result<Vec<u8>, JsValue> {
        xconn_crypto::derive_key(secret, info)
            .map(|key| key.to_vec())
            .map_err(error)
    }

    pub fn encrypt(plaintext: &[u8], key: &[u8], additional_data: &[u8]) -> Result<Vec<u8>, JsValue> {
        xconn_crypto::encrypt(plaintext, key, additional_data).map_err(error)
    }

    pub fn decrypt(packet: &[u8], key: &[u8], additional_data: &[u8]) -> Result<Vec<u8>, JsValue> {
        xconn_crypto::decrypt(packet, key, additional_data).map_err(error)
    }
}

/// RawSocket negotiation and bounded headers shared by browser transports.
#[wasm_bindgen]
pub struct RawSocketFraming {
    serializer: rawsocket::SerializerID,
    send_limit: usize,
}

#[wasm_bindgen]
impl RawSocketFraming {
    #[wasm_bindgen(constructor)]
    pub fn new(codec: &str) -> Result<RawSocketFraming, JsValue> {
        let serializer = match codec {
            "json" => rawsocket::SerializerID::JSON,
            "msgpack" => rawsocket::SerializerID::MSGPACK,
            "cbor" => rawsocket::SerializerID::CBOR,
            _ => return Err(error("unsupported RawSocket codec")),
        };
        Ok(Self {
            serializer,
            send_limit: 0,
        })
    }

    pub fn handshake(&self) -> Result<Vec<u8>, JsValue> {
        rawsocket::send_handshake(&rawsocket::Handshake::new(
            self.serializer,
            rawsocket::DEFAULT_MAX_MSG_SIZE,
        ))
        .map_err(error)
    }

    pub fn accept_handshake(&mut self, data: &[u8]) -> Result<(), JsValue> {
        let handshake = rawsocket::receive_handshake(data).map_err(error)?;
        if handshake.serializer_id() as u8 != self.serializer as u8 {
            return Err(error("server selected another serializer"));
        }
        self.send_limit = handshake.max_message_size();
        Ok(())
    }

    pub fn send_limit(&self) -> usize {
        self.send_limit
    }

    pub fn header(&self, kind: u8, length: usize) -> Result<Vec<u8>, JsValue> {
        if length > self.send_limit {
            return Err(error("message exceeds peer limit"));
        }
        let kind = rawsocket::Message::from_u8(kind).ok_or_else(|| error("invalid RawSocket frame kind"))?;
        rawsocket::try_send_message_header(&rawsocket::MessageHeader::new(kind, length)).map_err(error)
    }

    pub fn parse_header(&self, data: &[u8]) -> Result<Vec<u32>, JsValue> {
        let header = rawsocket::receive_message_header(data).map_err(error)?;
        if header.length() > rawsocket::DEFAULT_MAX_MSG_SIZE {
            return Err(error("message exceeds receive limit"));
        }
        Ok(vec![header.kind() as u32, header.length() as u32])
    }
}
