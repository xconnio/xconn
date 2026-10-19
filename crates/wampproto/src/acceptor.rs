//! Router handshake state and verification, independent of network I/O.
use crate::{
    authenticators::{
        cryptosign,
        server::{self, AuthRequest, AuthResponse, ServerAuthenticator},
        wampcra,
    },
    idgen::generate_session_id,
    messages::{
        authenticate::Authenticate,
        hello::Hello,
        message::Message,
        types::{Error, Value},
    },
    routing,
    serializers::{json::JSONSerializer, serializer::Serializer},
    types::SessionDetails,
};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptorState {
    None,
    ChallengeSent,
    AuthenticationPending,
    WelcomeSent,
    Aborted,
}

pub fn router_roles() -> HashMap<String, Value> {
    crate::roles::router_roles(false)
}

/// The host can resolve application policy outside the protocol engine's lock.
pub enum AcceptorAction {
    Send(Box<dyn Message>),
    Authenticate(AuthRequest),
}

pub struct Acceptor {
    state: AcceptorState,
    serializer: Box<dyn Serializer>,
    authenticator: Box<dyn ServerAuthenticator>,
    request: Option<AuthRequest>,
    response: Option<AuthResponse>,
    challenge: String,
    details: Option<SessionDetails>,
    pending_extra: HashMap<String, Value>,
    session_meta_api: bool,
    clock: Box<dyn Fn() -> String + Send>,
}

impl Default for Acceptor {
    fn default() -> Self {
        Self::new(Box::new(JSONSerializer {}), Box::new(server::AnonymousAuthenticator))
    }
}

impl Acceptor {
    pub fn new(serializer: Box<dyn Serializer>, authenticator: Box<dyn ServerAuthenticator>) -> Self {
        Self::with_clock(serializer, authenticator, Box::new(wampcra::utcnow))
    }

    /// WASM hosts can supply a clock without adding an execution runtime to the engine.
    pub fn with_clock(
        serializer: Box<dyn Serializer>,
        authenticator: Box<dyn ServerAuthenticator>,
        clock: Box<dyn Fn() -> String + Send>,
    ) -> Self {
        Self {
            state: AcceptorState::None,
            serializer,
            authenticator,
            request: None,
            response: None,
            challenge: String::new(),
            details: None,
            pending_extra: HashMap::new(),
            session_meta_api: false,
            clock,
        }
    }

    /// Announce the host's realm-provided Session Meta API on both router roles.
    pub fn with_session_meta_api(mut self, enabled: bool) -> Self {
        self.session_meta_api = enabled;
        self
    }

    pub fn state(&self) -> AcceptorState {
        self.state
    }

    pub fn session_details(&self) -> Result<&SessionDetails, Error> {
        self.details
            .as_ref()
            .ok_or_else(|| Error::new("session is not setup yet"))
    }

    pub fn receive(&mut self, data: Vec<u8>) -> Result<(Vec<u8>, bool), Error> {
        let message = self.serializer.deserialize(data)?;
        let response = self.receive_message(message.as_ref())?;
        let welcomed = response.message_type() == Value::Int(2);
        Ok((self.serializer.serialize(response.as_ref())?, welcomed))
    }

    fn abort(&mut self, reason: &str) -> Result<Box<dyn Message>, Error> {
        self.state = AcceptorState::Aborted;
        self.response = None;
        self.challenge.clear();
        routing::make(
            3,
            BTreeMap::from([
                ("details".into(), Value::Dict(HashMap::new())),
                ("reason".into(), Value::Str(reason.into())),
            ]),
        )
    }

    fn welcome(
        &mut self,
        response: AuthResponse,
        peer_extra: HashMap<String, Value>,
    ) -> Result<Box<dyn Message>, Error> {
        let request = self.request.as_ref().unwrap();
        let id = generate_session_id()?;
        let roles = crate::roles::router_roles(self.session_meta_api);
        let mut extra = response.extra.clone();
        extra.extend(peer_extra);
        let details = HashMap::from([
            ("realm".into(), Value::Str(request.realm.clone())),
            ("roles".into(), Value::Dict(roles.clone())),
            ("authid".into(), Value::Str(response.authid.clone())),
            ("authrole".into(), Value::Str(response.auth_role.clone())),
            ("authmethod".into(), Value::Str(request.method.clone())),
            ("authextra".into(), Value::Dict(response.extra.clone())),
        ]);
        let welcome = crate::messages::welcome::Welcome {
            session_id: id,
            realm: request.realm.clone(),
            authid: response.authid.clone(),
            auth_role: response.auth_role.clone(),
            details,
        };
        self.details = Some(
            SessionDetails::new(
                id,
                request.realm.clone(),
                response.authid,
                response.auth_role,
                self.serializer.is_static(),
            )
            .with_authentication(request.method.clone(), extra, roles, (self.clock)()),
        );
        self.state = AcceptorState::WelcomeSent;
        self.challenge.clear();
        self.response = None;
        Ok(Box::new(welcome))
    }

    pub fn receive_message(&mut self, message: &dyn Message) -> Result<Box<dyn Message>, Error> {
        match self.begin_message(message)? {
            AcceptorAction::Send(m) => Ok(m),
            AcceptorAction::Authenticate(r) => {
                let response = self.authenticator.authenticate(&r);
                self.complete_authentication(response)
            }
        }
    }

    pub fn complete_authentication(&mut self, result: Result<AuthResponse, Error>) -> Result<Box<dyn Message>, Error> {
        if self.state != AcceptorState::AuthenticationPending {
            return Err(Error::new("no authentication policy is pending"));
        }

        let response = match result {
            Ok(r) => r,
            Err(_) => return self.abort("wamp.error.authentication_failed"),
        };
        let method = self.request.as_ref().unwrap().method.clone();

        if method == "anonymous" || method == "ticket" {
            let extra = std::mem::take(&mut self.pending_extra);
            return self.welcome(response, extra);
        }

        let mut extra = HashMap::new();
        self.challenge = if method == "wampcra" {
            let cra = response
                .cra
                .as_ref()
                .ok_or_else(|| Error::new("WAMP-CRA response requires a signing key"))?;
            if let Some(s) = &cra.salt {
                wampcra::validate_cra_parameters(
                    i64::from(s.iterations),
                    i64::try_from(s.key_length).map_err(|_| Error::new("invalid key length"))?,
                )?;
                extra.insert("salt".into(), Value::Str(s.salt.clone()));
                extra.insert("iterations".into(), Value::Int(i64::from(s.iterations)));
                extra.insert("keylen".into(), Value::Int(s.key_length as i64));
            }

            wampcra::generate_wampcra_challenge_with_timestamp(
                generate_session_id()?,
                &response.authid,
                &response.auth_role,
                "dynamic",
                &(self.clock)(),
            )?
        } else {
            cryptosign::generate_cryptosign_challenge()?
        };
        extra.insert("challenge".into(), Value::Str(self.challenge.clone()));
        self.response = Some(response);
        self.state = AcceptorState::ChallengeSent;
        routing::make(
            4,
            BTreeMap::from([
                ("authmethod".into(), Value::Str(method)),
                ("extra".into(), Value::Dict(extra)),
            ]),
        )
    }

    pub fn begin_message(&mut self, message: &dyn Message) -> Result<AcceptorAction, Error> {
        if matches!(self.state, AcceptorState::WelcomeSent | AcceptorState::Aborted) {
            return Err(Error::new("acceptor handshake is already complete"));
        }

        if self.state == AcceptorState::AuthenticationPending {
            return Err(Error::new("authentication policy is pending"));
        }

        let (code, _) = routing::fields(message)?;

        match code {
            1 => {
                if self.state != AcceptorState::None {
                    return Err(Error::new("unexpected HELLO"));
                }

                let hello = message
                    .as_any()
                    .downcast_ref::<Hello>()
                    .ok_or_else(|| Error::new("expected typed HELLO"))?;
                let method = match server::select_auth_method(&self.authenticator.methods(), &hello.auth_methods) {
                    Ok(m) => m,
                    Err(_) => return self.abort("wamp.error.authentication_failed").map(AcceptorAction::Send),
                };

                if !matches!(method.as_str(), "anonymous" | "ticket" | "wampcra" | "cryptosign") {
                    return self.abort("wamp.error.authentication_failed").map(AcceptorAction::Send);
                }

                let mut request = AuthRequest {
                    realm: hello.realm.clone(),
                    method: method.clone(),
                    authid: hello.authid.clone(),
                    extra: hello.auth_extra.clone(),
                    ticket: None,
                    public_key: None,
                };

                if method == "cryptosign" {
                    let Some(Value::Str(key)) = hello.auth_extra.get("pubkey") else {
                        return self.abort("wamp.error.authentication_failed").map(AcceptorAction::Send);
                    };

                    if !hex::decode(key).is_ok_and(|bytes| bytes.len() == 32) {
                        return self.abort("wamp.error.authentication_failed").map(AcceptorAction::Send);
                    }

                    request.public_key = Some(key.clone());
                }

                self.request = Some(request.clone());

                if method == "ticket" {
                    self.state = AcceptorState::ChallengeSent;
                    return routing::make(
                        4,
                        BTreeMap::from([
                            ("authmethod".into(), Value::Str(method)),
                            ("extra".into(), Value::Dict(HashMap::new())),
                        ]),
                    )
                    .map(AcceptorAction::Send);
                }

                self.pending_extra.clear();
                self.state = AcceptorState::AuthenticationPending;
                Ok(AcceptorAction::Authenticate(request))
            }

            5 => {
                if self.state != AcceptorState::ChallengeSent {
                    return Err(Error::new("unexpected AUTHENTICATE"));
                }

                let authenticate = message
                    .as_any()
                    .downcast_ref::<Authenticate>()
                    .ok_or_else(|| Error::new("expected typed AUTHENTICATE"))?;
                let request = self.request.as_ref().unwrap();
                let response = match request.method.as_str() {
                    "ticket" => {
                        let mut request = request.clone();
                        request.ticket = Some(authenticate.signature.clone());
                        self.pending_extra = authenticate.extra.clone();
                        self.state = AcceptorState::AuthenticationPending;
                        return Ok(AcceptorAction::Authenticate(request));
                    }
                    "wampcra" => {
                        let r = self.response.as_ref().unwrap();

                        if !wampcra::verify_wampcra_signature(
                            &authenticate.signature,
                            &self.challenge,
                            &r.cra.as_ref().unwrap().key,
                        ) {
                            return self.abort("wamp.error.authentication_failed").map(AcceptorAction::Send);
                        };
                        r.clone()
                    }
                    "cryptosign" => {
                        let key = hex::decode(request.public_key.as_ref().unwrap())
                            .map_err(|_| Error::new("invalid public key"))?;
                        let signed = hex::decode(&authenticate.signature);
                        let challenge = hex::decode(&self.challenge).map_err(|_| Error::new("invalid challenge"))?;

                        if !signed.is_ok_and(|bytes| bytes.len() == 96 && bytes[64..] == challenge)
                            || !cryptosign::verify_cryptosign_signature(&authenticate.signature, &key)
                        {
                            return self.abort("wamp.error.authentication_failed").map(AcceptorAction::Send);
                        }

                        self.response.as_ref().unwrap().clone()
                    }
                    _ => return Err(Error::new("unexpected authentication method")),
                };
                self.welcome(response, authenticate.extra.clone())
                    .map(AcceptorAction::Send)
            }
            _ => Err(Error::new("acceptor: unexpected message")),
        }
    }
}
