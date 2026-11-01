//! Router, realm, policy and listener operations.
use crate::{
    Result, Value,
    callbacks::{get_callbacks, server_router},
    resources::{Output, Resource, get_router},
    values::{dictionary, handle_field, object_value, string_field},
};
use std::{collections::HashMap, sync::Arc};
use xconn_core::async_::{
    router::{RealmConfig, Router},
    server::Server,
};

fn permissions(value: &Value) -> Result<Vec<xconn_core::async_::router::Permission>> {
    let Value::List(values) = value else {
        return Err("permissions must be an array".into());
    };
    values
        .iter()
        .map(|value| {
            let fields = dictionary(value)?;
            let policy = match fields.get("match") {
                Some(Value::Str(policy)) if !policy.is_empty() => policy.as_str(),
                _ => "exact",
            };
            Ok(xconn_core::async_::router::Permission {
                uri: string_field(&fields, "uri")?,
                matching: xconn_core::wampproto::routing::MatchPolicy::parse(policy).map_err(|e| e.message)?,
                call: fields.get("call") == Some(&Value::Bool(true)),
                register: fields.get("register") == Some(&Value::Bool(true)),
                publish: fields.get("publish") == Some(&Value::Bool(true)),
                subscribe: fields.get("subscribe") == Some(&Value::Bool(true)),
            })
        })
        .collect()
}

fn foreign_authorizer(fields: &HashMap<String, Value>) -> Result<xconn_core::async_::router::Authorizer> {
    let callbacks = get_callbacks(handle_field(fields, "callback")?)?;
    Ok(Arc::new(move |details, message| {
        let response = callbacks.call(
            "authorize",
            object_value([
                ("session_id", Value::Int(details.id)),
                ("realm", Value::Str(details.realm.clone())),
                ("authid", Value::Str(details.authid.clone())),
                ("authrole", Value::Str(details.auth_role.clone())),
                ("authmethod", Value::Str(details.auth_method.clone())),
                ("authextra", Value::Dict(details.auth_extra.clone())),
                ("message", Value::List(message.marshal())),
            ]),
        );
        matches!(response, Ok(Value::Bool(true)))
    }))
}

pub(super) async fn dispatch(method: &str, f: HashMap<String, Value>) -> Result<Output> {
    let plain = |value| Ok(Output::plain(value));

    match method {
        "router_new" => {
            let r = Router::new();

            if let Some(Value::Str(realm)) = f.get("realm") {
                r.add_realm(realm, RealmConfig::default()).map_err(|e| e.to_string())?;
            }

            let management = if f.get("management") == Some(&Value::Bool(true)) {
                use xconn_core::async_::router::Permission;
                let config = RealmConfig {
                    roles: HashMap::from([
                        ("trusted".into(), vec![Permission::all()]),
                        (
                            "anonymous".into(),
                            vec![Permission {
                                uri: "io.xconn.mgmt.".into(),
                                matching: xconn_core::wampproto::routing::MatchPolicy::Prefix,
                                call: true,
                                subscribe: true,
                                publish: false,
                                register: false,
                            }],
                        ),
                    ]),
                    ..RealmConfig::default()
                };
                Some(
                    xconn_core::async_::management::Management::start(r.clone(), Some(config))
                        .await
                        .map_err(|error| error.to_string())?,
                )
            } else {
                None
            };
            Ok(Output::resource(
                Resource::Router {
                    router: r,
                    _management: management.map(Box::new),
                },
                HashMap::new(),
            ))
        }
        "permission_match" => {
            let policy = xconn_core::wampproto::routing::MatchPolicy::parse(&string_field(&f, "policy")?)
                .map_err(|e| e.message)?;
            plain(Value::Bool(
                policy.matches(&string_field(&f, "pattern")?, &string_field(&f, "uri")?),
            ))
        }
        "role_add" | "role_remove" | "role_has" | "realm_meta" | "realm_authorizer" => {
            let router = get_router(handle_field(&f, "handle")?)?;
            let realm = string_field(&f, "realm")?;

            match method {
                "role_add" => {
                    router
                        .add_role(
                            &realm,
                            string_field(&f, "name")?,
                            permissions(f.get("permissions").ok_or("missing permissions")?)?,
                        )
                        .map_err(|e| e.to_string())?;
                }
                "role_remove" => {
                    router
                        .remove_role(&realm, &string_field(&f, "name")?)
                        .map_err(|e| e.to_string())?;
                }
                "role_has" => {
                    return plain(Value::Bool(
                        router
                            .has_role(&realm, &string_field(&f, "name")?)
                            .map_err(|e| e.to_string())?,
                    ));
                }
                "realm_meta" => {
                    router.enable_meta_api(&realm).map_err(|e| e.to_string())?;
                }
                _ => {
                    router
                        .set_authorizer(&realm, foreign_authorizer(&f)?)
                        .map_err(|e| e.to_string())?;
                }
            }

            plain(Value::Null)
        }
        "realm_add" | "realm_has" | "realm_remove" | "realm_alias" => {
            let router = get_router(handle_field(&f, "handle")?)?;
            let realm = string_field(&f, "realm")?;

            match method {
                "realm_has" => plain(Value::Bool(router.has_realm(&realm))),
                "realm_remove" => {
                    router.remove_realm(&realm).map_err(|e| e.to_string())?;
                    plain(Value::Null)
                }
                "realm_alias" => {
                    router
                        .add_alias(&realm, string_field(&f, "alias")?)
                        .map_err(|e| e.to_string())?;
                    plain(Value::Null)
                }
                _ => {
                    let config = dictionary(f.get("config").unwrap_or(&Value::Null))?;
                    let mut realm_config = RealmConfig {
                        meta_api: config.get("meta") == Some(&Value::Bool(true)),
                        disclose_caller: config.get("disclose_caller") == Some(&Value::Bool(true)),
                        disclose_publisher: config.get("disclose_publisher") == Some(&Value::Bool(true)),
                        ..RealmConfig::default()
                    };

                    if config.get("legacy_go") == Some(&Value::Bool(true)) {
                        realm_config.roles.remove("anonymous");
                    }

                    if let Some(Value::List(roles)) = config.get("roles") {
                        for role in roles {
                            let role = dictionary(role)?;
                            let name = string_field(&role, "name")?;

                            if realm_config.roles.contains_key(&name) {
                                return Err("role already exists".into());
                            }

                            realm_config.roles.insert(
                                name,
                                permissions(role.get("permissions").ok_or("missing permissions")?)?,
                            );
                        }
                    }

                    if config.get("allow_all") == Some(&Value::Bool(true)) {
                        realm_config.authorizer = Some(Arc::new(|_, _| true));
                    }

                    if config.contains_key("callback") {
                        realm_config.authorizer = Some(foreign_authorizer(&config)?);
                    }

                    router.add_realm(realm, realm_config).map_err(|e| e.to_string())?;
                    plain(Value::Null)
                }
            }
        }
        "router_stop" => {
            get_router(handle_field(&f, "handle")?)?.close();
            plain(Value::Null)
        }
        "router_detach" => {
            get_router(handle_field(&f, "handle")?)?
                .kill_session(
                    &string_field(&f, "realm")?,
                    i64::try_from(handle_field(&f, "session_id")?).map_err(|_| "session ID exceeds i64")?,
                    "wamp.close.normal",
                )
                .map_err(|e| e.to_string())?;
            plain(Value::Null)
        }
        "websocket_protocols" => plain(Value::List(
            xconn_core::async_::server::websocket_protocols()
                .into_iter()
                .map(Value::Str)
                .collect(),
        )),
        "protocol_select" => {
            let supported: Vec<String> = match f.get("supported") {
                Some(Value::List(values)) => values
                    .iter()
                    .filter_map(|v| if let Value::Str(s) = v { Some(s.clone()) } else { None })
                    .collect(),
                _ => return Err("expected supported protocols".into()),
            };
            plain(
                xconn_core::async_::server::select_websocket_protocol(&string_field(&f, "offered")?, &supported)
                    .map(|s| Value::Str(s.into()))
                    .unwrap_or(Value::Null),
            )
        }
        "listen" => {
            let r = server_router(&f)?;

            if f.get("network") == Some(&Value::Str("unix".into())) {
                #[cfg(unix)]
                {
                    let path = string_field(&f, "address")?;
                    let transport = string_field(&f, "transport")?;
                    #[cfg(feature = "yamux")]
                    if transport == "yamux" {
                        let listener = xconn_core::async_::server::UnixServer::yamux(r, &path)
                            .await
                            .map_err(|error| error.to_string())?;
                        return Ok(Output::resource(
                            Resource::UnixListener(listener),
                            HashMap::from([("address".into(), Value::Str(path))]),
                        ));
                    }
                    if transport != "websocket" && transport != "rawsocket" {
                        return Err("unsupported Unix listener transport".into());
                    }
                    let listener = xconn_core::async_::server::UnixServer::listen(r, &path, transport == "websocket")
                        .await
                        .map_err(|error| error.to_string())?;
                    return Ok(Output::resource(
                        Resource::UnixListener(listener),
                        HashMap::from([("address".into(), Value::Str(path))]),
                    ));
                }

                #[cfg(not(unix))]
                return Err("Unix sockets are not available on this platform".into());
            }

            let address_text = string_field(&f, "address")?;
            let address = tokio::net::lookup_host(address_text)
                .await
                .map_err(|e| e.to_string())?
                .next()
                .ok_or("listener address resolved to no addresses")?;
            let server = match string_field(&f, "transport")?.as_str() {
                "websocket" => {
                    if f.contains_key("protocol_callback") {
                        let callbacks = get_callbacks(handle_field(&f, "protocol_callback")?)?;
                        let protocols = Arc::new(move || {
                            let value = callbacks
                                .call("websocket_protocols", Value::Null)
                                .map_err(xconn_core::Error::new)?;
                            match value {
                                Value::List(values) => values
                                    .into_iter()
                                    .map(|v| match v {
                                        Value::Str(s) => Ok(s),
                                        _ => Err(xconn_core::Error::new("expected protocol string")),
                                    })
                                    .collect(),
                                _ => Err(xconn_core::Error::new("expected protocols")),
                            }
                        });
                        Server::websocket_with_protocols(r, address, protocols).await
                    } else {
                        Server::websocket(r, address).await
                    }
                }
                #[cfg(feature = "yamux")]
                "yamux" => {
                    if let Some(Value::List(chain)) = f.get("certificates") {
                        let certificates = chain
                            .iter()
                            .map(|value| match value {
                                Value::Bytes(bytes) => Ok(rustls::pki_types::CertificateDer::from(bytes.0.clone())),
                                _ => Err("expected DER certificate".to_string()),
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let Some(Value::Bytes(key)) = f.get("private_key") else {
                            return Err("expected DER private key".into());
                        };
                        let key = rustls::pki_types::PrivateKeyDer::try_from(key.0.clone()).map_err(str::to_owned)?;
                        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
                            rustls::crypto::ring::default_provider(),
                        ))
                        .with_safe_default_protocol_versions()
                        .map_err(|e| e.to_string())?
                        .with_no_client_auth()
                        .with_single_cert(certificates, key)
                        .map_err(|e| e.to_string())?;
                        Server::yamux_tls(r, address, Arc::new(tls)).await
                    } else {
                        Server::yamux(r, address).await
                    }
                }
                "rawsocket" => Server::rawsocket(r, address).await,
                _ => return Err("unsupported listener transport".into()),
            }
            .map_err(|e| e.to_string())?;
            let address = server.local_addr.to_string();
            Ok(Output::resource(
                Resource::Listener(server),
                HashMap::from([("address".into(), Value::Str(address))]),
            ))
        }
        _ => Err(format!("unknown router operation {method}")),
    }
}
