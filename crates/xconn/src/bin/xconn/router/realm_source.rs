//! Load a realm inventory at startup and apply subsequent add/remove RPCs.
//!
//! Deskconn uses this during the Hub migration. Remove Deskconn's inventory and
//! update RPC integration once an authenticated edge connection owns each
//! machine realm's lifetime.

use super::{
    config::{Authenticators, RealmSourceConfig, Result, sourced_realm_config, valid_uri},
    rpc_authentication::RpcAuthentication,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use xconn::{
    Error, JSONSerializerSpec, Result as RpcResult, Value,
    async_::{management, router::Router, session::Session, types::RegisterRequest},
};

pub struct RealmSource {
    session: Session,
    state: Arc<Mutex<Realms>>,
    list: String,
}

struct Realms {
    router: Router,
    control_realm: String,
    authentication: Option<Arc<RpcAuthentication>>,
    active: HashMap<String, String>,
    changes_during_load: Option<StartupChanges>,
}

#[derive(Default)]
struct StartupChanges {
    added: HashSet<String>,
    removed: HashMap<String, HashSet<String>>,
}

impl RealmSource {
    pub async fn start(router: Router, config: RealmSourceConfig, local: Authenticators) -> Result<Self> {
        let session = router
            .connect_internal(
                &config.control_realm,
                "realm-source",
                &config.role,
                Box::new(JSONSerializerSpec),
            )
            .await?;
        let authentication = config.authentication.map(|auth| {
            Arc::new(RpcAuthentication::new(
                auth,
                config.control_realm.clone(),
                session.clone(),
                local,
            ))
        });
        if let Some(auth) = &authentication {
            router.set_async_authenticator(&config.control_realm, auth.clone())?;
        }
        let state = Arc::new(Mutex::new(Realms {
            router,
            control_realm: config.control_realm,
            authentication,
            active: HashMap::new(),
            changes_during_load: Some(StartupChanges::default()),
        }));
        for (uri, remove) in [(config.add, false), (config.remove, true)] {
            let state = state.clone();
            let reply = session
                .execute_register(RegisterRequest::new(uri, move |inv| {
                    let state = state.clone();
                    async move {
                        let [Value::Str(name), Value::Str(authid)] = inv.args.as_slice() else {
                            return RpcResult::error("wamp.error.invalid_argument");
                        };
                        if !inv.kwargs.is_empty() || !valid_pair(name, authid) {
                            return RpcResult::error("wamp.error.invalid_argument");
                        }
                        let mut state = state.lock().unwrap();
                        let result = if remove {
                            state.remove(name, authid)
                        } else {
                            state.add(name, authid)
                        };
                        match result {
                            Ok(()) => {
                                if let Some(changes) = &mut state.changes_during_load {
                                    if remove {
                                        changes.removed.entry(name.clone()).or_default().insert(authid.clone());
                                    } else {
                                        changes.added.insert(name.clone());
                                    }
                                }
                                RpcResult::default()
                            }
                            Err(uri) => RpcResult::error(uri),
                        }
                    }
                }))
                .await?;
            if let Some(error) = reply.error {
                let _ = session.leave().await;
                return Err(Error::application(error).into());
            }
        }
        Ok(Self {
            session,
            state,
            list: config.list,
        })
    }

    pub async fn load(&self) -> Result<()> {
        let reply = self
            .session
            .call(&self.list)
            .execute_when_available(Duration::from_secs(30))
            .await?;
        if let Some(error) = reply.error {
            return Err(Error::application(error).into());
        }
        let Some([Value::List(entries)]) = reply.args.as_deref() else {
            return Err("realm inventory must return one list".into());
        };
        let mut inventory = HashMap::new();
        for entry in entries {
            let Value::Dict(record) = entry else {
                return Err("realm inventory entry must be a dictionary".into());
            };
            let (Some(Value::Str(name)), Some(Value::Str(authid))) = (record.get("realm"), record.get("authid")) else {
                return Err("realm inventory entry requires realm and authid strings".into());
            };
            if !valid_pair(name, authid) || inventory.insert(name, authid).is_some() {
                return Err("realm inventory contains an invalid or duplicate realm".into());
            }
        }

        let mut state = self.state.lock().unwrap();
        let changes = state
            .changes_during_load
            .take()
            .ok_or("realm inventory already loaded")?;
        let mut installed = Vec::new();
        for (name, authid) in inventory {
            // An add supersedes the snapshot entry. A removal can only suppress its own pair.
            if changes.added.contains(name)
                || changes
                    .removed
                    .get(name)
                    .is_some_and(|authids| authids.contains(authid))
            {
                continue;
            }
            if let Err(error) = state.add(name, authid) {
                for (name, authid) in installed {
                    let _ = state.remove(name, authid);
                }
                return Err(format!("cannot start realm {name} from inventory: {error}").into());
            }
            installed.push((name, authid));
        }
        Ok(())
    }
}

impl Realms {
    fn add(&mut self, name: &str, authid: &str) -> std::result::Result<(), &'static str> {
        if let Some(owner) = self.active.get(name) {
            return if owner == authid {
                Ok(())
            } else {
                Err("wamp.error.already_exists")
            };
        }
        let mut config = sourced_realm_config();
        if let Some(auth) = &self.authentication {
            config.meta_api = true;
            config.disclose_caller = true;
            for role in &auth.config.realm_roles {
                config.roles.insert(
                    role.name.clone(),
                    role.rules(None).map_err(|_| "wamp.error.invalid_argument")?,
                );
            }
            if let Some(role) = &auth.config.realm_machine_role {
                let permissions = role.rules(Some(authid)).map_err(|_| "wamp.error.invalid_argument")?;
                if config.roles.insert(role.machine_name(authid), permissions).is_some() {
                    return Err("wamp.error.already_exists");
                }
            }
            let control_role = auth
                .config
                .control_machine_role
                .as_ref()
                .map(|role| {
                    role.rules(Some(authid))
                        .map(|permissions| (role.machine_name(authid), permissions))
                })
                .transpose()
                .map_err(|_| "wamp.error.invalid_argument")?;
            self.router
                .add_realm_with_async_authenticator(name, config, auth.clone())
                .map_err(|_| "wamp.error.already_exists")?;
            if let Some((role, permissions)) = control_role
                && self.router.add_role(&self.control_realm, role, permissions).is_err()
            {
                let _ = self.router.remove_realm(name);
                return Err("wamp.error.already_exists");
            }
        } else {
            self.router
                .add_realm(name, config)
                .map_err(|_| "wamp.error.already_exists")?;
        }
        self.active.insert(name.into(), authid.into());
        Ok(())
    }

    fn remove(&mut self, name: &str, authid: &str) -> std::result::Result<(), &'static str> {
        match self.active.get(name) {
            Some(owner) if owner == authid => {
                self.router.remove_realm(name).map_err(|_| "wamp.error.no_such_realm")?;
                if let Some(role) = self
                    .authentication
                    .as_ref()
                    .and_then(|auth| auth.config.control_machine_role.as_ref())
                {
                    let _ = self.router.remove_role(&self.control_realm, &role.machine_name(authid));
                }
                self.active.remove(name);
                Ok(())
            }
            // Before the snapshot arrives, a removal can target a realm we have not loaded yet.
            None if !self.router.has_realm(name) => Ok(()),
            _ => Err("wamp.error.no_such_realm"),
        }
    }
}

fn valid_pair(name: &str, authid: &str) -> bool {
    valid_uri(name) && name != management::REALM && !authid.is_empty()
}
