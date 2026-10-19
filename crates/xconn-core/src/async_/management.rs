//! Portable xconn management procedures. Runtime/OS instrumentation belongs to
//! the embedding application and can be registered on the same Session.
use super::{
    router::{Permission, RealmConfig, Router},
    session::Session,
    types::RegisterRequest,
};
use crate::{Error, JSONSerializerSpec, RpcResult, Value};
use std::collections::HashMap;
pub const REALM: &str = "io.xconn.mgmt";
pub const LIST_REALMS: &str = "io.xconn.mgmt.realm.list";
pub const LIST_SESSIONS: &str = "io.xconn.mgmt.session.list";
pub const KILL_SESSION: &str = "io.xconn.mgmt.session.kill";

pub struct Management {
    pub session: Session,
    router: Router,
}

impl Management {
    pub async fn start(router: Router, config: Option<RealmConfig>) -> Result<Self, Error> {
        let config = config.unwrap_or_else(|| RealmConfig {
            roles: HashMap::from([("trusted".into(), vec![Permission::all()])]),
            disclose_caller: true,
            ..Default::default()
        });
        router.add_realm(REALM, config)?;
        let session = match router
            .connect_internal(REALM, "management", "trusted", Box::new(JSONSerializerSpec))
            .await
        {
            Ok(session) => session,
            Err(error) => {
                let _ = router.remove_realm(REALM);
                return Err(error);
            }
        };
        let result = Self { session, router };

        if let Err(error) = result.register().await {
            let _ = result.router.remove_realm(REALM);
            return Err(error);
        }

        Ok(result)
    }

    async fn register(&self) -> Result<(), Error> {
        let router = self.router.clone();
        let reply = self
            .session
            .execute_register(RegisterRequest::new(LIST_REALMS, move |_| {
                let router = router.clone();
                async move { RpcResult::arg(Value::List(router.realm_names().into_iter().map(Value::Str).collect())) }
            }))
            .await?;
        if let Some(error) = reply.error {
            return Err(Error::new(error.uri));
        }

        let router = self.router.clone();
        let reply = self
            .session
            .execute_register(RegisterRequest::new(LIST_SESSIONS, move |inv| {
                let router = router.clone();
                async move {
                    let Some(Value::Str(realm)) = inv.args.first() else {
                        return RpcResult::error("wamp.error.invalid_argument");
                    };
                    let Ok(mut sessions) = router.sessions(realm) else {
                        return RpcResult::error("wamp.error.not_found");
                    };
                    sessions.sort_by_key(|session| session.id);
                    let number = |key: &str, default: i64| -> Result<i64, ()> {
                        match inv.kwargs.get(key) {
                            None => Ok(default),
                            Some(Value::Int(v)) => Ok(*v),
                            Some(Value::UInt(v)) => i64::try_from(*v).map_err(|_| ()),
                            _ => Err(()),
                        }
                    };
                    let (Ok(offset), Ok(limit)) = (number("offset", 0), number("limit", 50)) else {
                        return RpcResult::error("wamp.error.invalid_argument");
                    };

                    if offset < 0 || limit < 0 {
                        return RpcResult::error("wamp.error.invalid_argument");
                    }

                    let total = sessions.len() as i64;
                    let records = sessions
                        .into_iter()
                        .skip(offset as usize)
                        .take(limit as usize)
                        .map(|session| {
                            Value::Dict(HashMap::from([
                                ("sessionID".into(), Value::Int(session.id)),
                                ("authid".into(), Value::Str(session.authid)),
                                ("authrole".into(), Value::Str(session.auth_role)),
                                ("serializer".into(), Value::Str("unknown".into())),
                            ]))
                        })
                        .collect();
                    RpcResult::new(
                        vec![Value::List(records)],
                        HashMap::from([
                            ("total".into(), Value::Int(total)),
                            ("offset".into(), Value::Int(offset)),
                            ("limit".into(), Value::Int(limit)),
                        ]),
                    )
                }
            }))
            .await?;
        if let Some(error) = reply.error {
            return Err(Error::new(error.uri));
        }

        let router = self.router.clone();
        let own_id = self.session.details().id();
        let reply = self
            .session
            .execute_register(RegisterRequest::new(KILL_SESSION, move |inv| {
                let router = router.clone();
                async move {
                    let (realm, id) = match inv.args.as_slice() {
                        [Value::Str(realm), Value::Int(id)] => (realm, *id),
                        [Value::Str(realm), Value::UInt(id)] => match i64::try_from(*id) {
                            Ok(id) => (realm, id),
                            Err(_) => return RpcResult::error("wamp.error.invalid_argument"),
                        },
                        _ => return RpcResult::error("wamp.error.invalid_argument"),
                    };

                    if id == own_id
                        || inv.details.get("caller") == Some(&Value::Int(id))
                        || inv.details.get("caller") == Some(&Value::UInt(id as u64))
                    {
                        return RpcResult::error("wamp.error.invalid_argument");
                    }

                    let reason = match inv.kwargs.get("reason") {
                        None => "wamp.close.killed",
                        Some(Value::Str(reason)) => reason,
                        _ => return RpcResult::error("wamp.error.invalid_argument"),
                    };

                    match router.kill_session(realm, id, reason) {
                        Ok(()) => RpcResult::default(),
                        Err(_) => RpcResult::error("wamp.error.not_found"),
                    }
                }
            }))
            .await?;
        if let Some(error) = reply.error {
            return Err(Error::new(error.uri));
        }

        Ok(())
    }

    pub async fn close(&self) {
        let _ = self.session.leave().await;
        let _ = self.router.remove_realm(REALM);
    }
}
