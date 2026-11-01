//! Managed direct transports own physical connections; exported sessions are views.
use super::{Resource, ice_servers};
use crate::{
    Result, Value,
    callbacks::{authenticator, codec, server_authentication},
    resources::{
        ForeignSession, Output, OwnedResource, Resource as BridgeResource, get_resource, get_router, get_session,
    },
    values::{duration_field, handle_field, session_fields},
};
use std::{collections::HashMap, sync::Arc};
use xconn_webrtc::wamp::WebRTCSession;

pub(super) async fn dispatch(method: &str, f: HashMap<String, Value>) -> Result<Output> {
    let plain = |value| Ok(Output::plain(value));
    match method {
        "webrtc_peer_start" => {
            let hub_handle = handle_field(&f, "session")?;
            let hub = get_session(hub_handle)?;
            let options = peer_config(&f)?;
            // Validate binding codecs/authentication before starting background retries.
            let _ = codec(&f)?;
            let _ = authenticator(&f)?;
            let policy = options.retry;
            let connect: xconn_core::async_::services::peer::Connector<ManagedDirect> = Arc::new(move || {
                let (hub, options, fields) = (hub.clone(), options.clone(), f.clone());
                Box::pin(async move {
                    let session = xconn_webrtc::peer::connect_once(
                        &hub.session,
                        &options,
                        codec(&fields).map_err(xconn_core::Error::new)?,
                        authenticator(&fields).map_err(xconn_core::Error::new)?,
                    )
                    .await?;
                    let view = OwnedResource::new(BridgeResource::SessionView(Arc::new(ForeignSession::new(
                        session.session.clone(),
                    ))));
                    Ok(ManagedDirect {
                        view,
                        session,
                        executor: tokio::runtime::Handle::current(),
                    })
                })
            });
            let hub = get_session(hub_handle)?;
            let manager = xconn_core::async_::services::peer::Peer::start(hub.session.clone(), connect, policy)
                .map_err(|error| error.to_string())?;
            Ok(Output::resource(
                BridgeResource::WebRTC(Resource::Peer(Arc::new(ManagedPeer { manager, hub_handle }))),
                HashMap::new(),
            ))
        }
        "webrtc_peer_select" | "webrtc_peer_status" | "webrtc_peer_error" | "webrtc_peer_wait" => {
            let resource = get_resource(handle_field(&f, "handle")?)?;
            let BridgeResource::WebRTC(Resource::Peer(peer)) = resource.as_ref() else {
                return Err("expected peer manager".into());
            };
            match method {
                "webrtc_peer_select" => {
                    peer.manager.ensure_open().map_err(|error| error.to_string())?;
                    let fields = match peer.manager.direct_for(
                        f.get("procedure")
                            .and_then(|value| match value {
                                Value::Str(value) => Some(value.as_str()),
                                _ => None,
                            })
                            .unwrap_or(""),
                    ) {
                        Some(direct) => {
                            let mut fields = session_fields(direct.session.details());
                            fields.insert("handle".into(), Value::UInt(direct.view.handle()));
                            fields
                        }
                        None => HashMap::from([("handle".into(), Value::UInt(peer.hub_handle))]),
                    };
                    plain(Value::Dict(fields))
                }
                "webrtc_peer_status" => {
                    use xconn_core::async_::services::peer::ConnectionPath;
                    let status = peer.manager.status();
                    let path = match status.path {
                        ConnectionPath::Routed => "routed",
                        ConnectionPath::Direct => "direct",
                        ConnectionPath::Closed => "closed",
                    };
                    plain(Value::Dict(HashMap::from([
                        ("path".into(), Value::Str(path.into())),
                        (
                            "direct_error".into(),
                            status.direct_error.map_or(Value::Null, Value::Str),
                        ),
                    ])))
                }
                "webrtc_peer_error" => plain(peer.manager.last_error().map_or(Value::Null, Value::Str)),
                _ => {
                    peer.manager.wait_closed().await;
                    plain(Value::Null)
                }
            }
        }
        "webrtc_peer_endpoint_close" => {
            let resource = get_resource(handle_field(&f, "handle")?)?;
            let BridgeResource::WebRTC(Resource::Endpoint(endpoint)) = resource.as_ref() else {
                return Err("expected peer endpoint".into());
            };
            endpoint.close().await;
            plain(Value::Null)
        }
        "webrtc_peer_expose" => {
            let signaling = get_session(handle_field(&f, "session")?)?;
            let mut router = get_router(handle_field(&f, "router")?)?;
            if let Some(authentication) = server_authentication(&f)? {
                router = router.with_authentication(authentication);
            }
            let endpoint = xconn_webrtc::peer::Endpoint::expose(signaling.session.clone(), router, peer_config(&f)?)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Output::resource(
                BridgeResource::WebRTC(Resource::Endpoint(Arc::new(endpoint))),
                HashMap::new(),
            ))
        }
        _ => Err(format!("unknown peer operation {method}")),
    }
}

fn peer_config(fields: &HashMap<String, Value>) -> Result<xconn_webrtc::peer::Config> {
    let mut config = xconn_webrtc::peer::Config::default();
    if fields.contains_key("ice_servers") {
        config.ice_servers = ice_servers(fields)?;
    }
    config.retry.initial =
        duration_field(fields, "retry_interval", Some(config.retry.initial))?.ok_or("retry interval is required")?;
    config.retry.connect_timeout = duration_field(fields, "connect_timeout", Some(config.retry.connect_timeout))?
        .ok_or("connect timeout is required")?;
    Ok(config)
}

pub(crate) struct ManagedPeer {
    pub(super) manager: xconn_core::async_::services::peer::Peer<ManagedDirect>,
    hub_handle: u64,
}

// Sole owner of a managed direct transport. Foreign session handles are views.
pub(super) struct ManagedDirect {
    view: OwnedResource,
    session: WebRTCSession,
    executor: tokio::runtime::Handle,
}

#[async_trait::async_trait]
impl xconn_core::async_::services::peer::DirectConnection for ManagedDirect {
    fn session(&self) -> &xconn_core::async_::session::Session {
        &self.session.session
    }

    async fn close(&self) {
        let _ = self.session.connection.close().await;
    }
}

impl Drop for ManagedDirect {
    fn drop(&mut self) {
        // Also covers a completed connector discarded during cancellation.
        let connection = self.session.connection.clone();
        self.executor.spawn(async move {
            let _ = connection.close().await;
        });
    }
}
