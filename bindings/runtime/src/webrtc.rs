//! Resource ownership only; signaling and transport are implemented in xconn-webrtc.
use crate::{
    ByteArray, Result, Value,
    callbacks::{authenticator, codec, server_authentication},
    resources::{ForeignSession, Output, Resource as BridgeResource, get_resource, get_router, get_session},
    runtime,
    values::{duration_field, handle_field, request_options, session_fields, string_field},
};
use std::{collections::HashMap, sync::Arc};
use xconn_webrtc::{
    Connection, DataChannel,
    wamp::{Provider, SignalingConfig, Timeouts, WebRTCSession, connect_with_timeouts},
    webrtc::{data_channel::data_channel_init::RTCDataChannelInit, ice_transport::ice_server::RTCIceServer},
};

pub(super) enum Resource {
    Connection(Connection),
    Peer(Arc<ManagedPeer>),
    Endpoint(Arc<xconn_webrtc::peer::Endpoint>),
    Channel {
        channel: Arc<DataChannel>,
        connection: Connection,
    },
    Provider(Arc<Provider>),
}

impl Resource {
    pub(super) fn close(&self) {
        match self {
            Self::Peer(peer) => {
                let peer = peer.clone();
                runtime().spawn(async move {
                    peer.manager.close().await;
                });
            }
            Self::Endpoint(endpoint) => {
                let endpoint = endpoint.clone();
                runtime().spawn(async move {
                    endpoint.close().await;
                });
            }
            Self::Connection(connection) => {
                let connection = connection.clone();
                runtime().spawn(async move {
                    let _ = connection.close().await;
                });
            }
            Self::Channel { channel, .. } => {
                let channel = channel.clone();
                runtime().spawn(async move {
                    let _ = channel.close().await;
                });
            }
            Self::Provider(provider) => {
                let provider = provider.clone();
                runtime().spawn(async move {
                    provider.close().await;
                });
            }
        }
    }
}

fn connection(handle: u64) -> Result<Connection> {
    match get_resource(handle)?.as_ref() {
        BridgeResource::WebRTC(Resource::Connection(connection)) => Ok(connection.clone()),
        BridgeResource::WebRTC(Resource::Channel { connection, .. }) => Ok(connection.clone()),
        _ => Err("expected WebRTC connection".into()),
    }
}

fn channel(handle: u64) -> Result<Arc<DataChannel>> {
    match get_resource(handle)?.as_ref() {
        BridgeResource::WebRTC(Resource::Channel { channel, .. }) => Ok(channel.clone()),
        _ => Err("expected WebRTC data channel".into()),
    }
}

fn provider(handle: u64) -> Result<Arc<Provider>> {
    match get_resource(handle)?.as_ref() {
        BridgeResource::WebRTC(Resource::Provider(provider)) => Ok(provider.clone()),
        _ => Err("expected WebRTC provider".into()),
    }
}

fn timeouts(f: &HashMap<String, Value>) -> Result<Timeouts> {
    let defaults = Timeouts::default();
    Ok(Timeouts {
        connect: duration_field(f, "connect_timeout", Some(defaults.connect))?.ok_or("connect timeout is required")?,
        open_session: duration_field(f, "open_timeout", Some(defaults.open_session))?
            .ok_or("open timeout is required")?,
        handshake: duration_field(f, "handshake_timeout", Some(defaults.handshake))?
            .ok_or("handshake timeout is required")?,
    })
}

fn ice_servers(f: &HashMap<String, Value>) -> Result<Vec<RTCIceServer>> {
    let Some(value) = f.get("ice_servers") else {
        return Ok(Vec::new());
    };
    let Value::List(servers) = value else {
        return Err("ice_servers must be a list".into());
    };
    let mut servers = servers.clone();
    for server in &mut servers {
        let Value::Dict(fields) = server else {
            return Err("each ICE server must be a dictionary".into());
        };
        // STUN servers normally have only URLs. webrtc-rs requires these
        // optional credential fields during deserialization, even for STUN.
        fields
            .entry("username".into())
            .or_insert_with(|| Value::Str(String::new()));
        fields
            .entry("credential".into())
            .or_insert_with(|| Value::Str(String::new()));
    }
    let codec = xconn_core::wampproto::serializers::codec::Codec::Json;
    codec
        .decode(&codec.encode(&Value::List(servers)).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}

fn config(f: &HashMap<String, Value>) -> Result<SignalingConfig> {
    Ok(SignalingConfig {
        offer_procedure: string_field(f, "offer_procedure")?,
        remote_candidates_topic: string_field(f, "remote_candidates_topic")?,
        local_candidates_topic: string_field(f, "local_candidates_topic")?,
        ice_servers: ice_servers(f)?,
    })
}

fn bytes(f: &HashMap<String, Value>) -> Result<Vec<u8>> {
    match f.get("data") {
        Some(Value::Bytes(data)) => Ok(data.0.clone()),
        _ => Err("expected byte payload".into()),
    }
}

fn channel_output(channel: Arc<DataChannel>, connection: Connection) -> Output {
    let fields = HashMap::from([
        ("label".into(), Value::Str(channel.label().into())),
        ("protocol".into(), Value::Str(channel.protocol().into())),
    ]);
    Output::resource(
        BridgeResource::WebRTC(Resource::Channel { channel, connection }),
        fields,
    )
}

async fn opened_channel(
    channel: Arc<DataChannel>,
    connection: Connection,
    timeout: std::time::Duration,
) -> Result<Output> {
    let mut pending = PendingChannel(Some(channel.clone()));
    channel.wait_open(timeout).await.map_err(|e| e.to_string())?;
    let output = channel_output(channel, connection);
    pending.0 = None;
    Ok(output)
}

struct PendingChannel(Option<Arc<DataChannel>>);

impl Drop for PendingChannel {
    fn drop(&mut self) {
        if let Some(channel) = self.0.take() {
            runtime().spawn(async move {
                let _ = channel.close().await;
            });
        }
    }
}

fn options(f: &HashMap<String, Value>) -> Result<RTCDataChannelInit> {
    let f = request_options(f)?;
    let integer = |name| -> Result<Option<u16>> {
        f.get(name)
            .map(|_| {
                handle_field(&f, name)
                    .and_then(|v| u16::try_from(v).map_err(|_| "data channel option exceeds u16".into()))
            })
            .transpose()
    };
    let ordered = match f.get("ordered") {
        None => None,
        Some(Value::Bool(value)) => Some(*value),
        _ => return Err("ordered must be a boolean".into()),
    };
    Ok(RTCDataChannelInit {
        ordered,
        max_packet_life_time: integer("max_packet_life_time")?,
        max_retransmits: integer("max_retransmits")?,
        negotiated: integer("negotiated")?,
        protocol: None,
    })
}

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
                    let output = Output::resource(
                        BridgeResource::Session(Arc::new(ForeignSession::new(session.session.clone()))),
                        session_fields(session.details()),
                    )
                    .with_resource(
                        "connection",
                        BridgeResource::WebRTC(Resource::Connection(session.connection.clone())),
                    );
                    let value = output.take();
                    let Value::Dict(fields) = value else { unreachable!() };
                    Ok(ManagedDirect {
                        handle: handle_field(&fields, "handle").map_err(xconn_core::Error::new)?,
                        connection: handle_field(&fields, "connection").map_err(xconn_core::Error::new)?,
                        session,
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
        "webrtc_peer_select" | "webrtc_peer_error" | "webrtc_peer_wait" => {
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
                            fields.insert("handle".into(), Value::UInt(direct.handle));
                            fields.insert("connection".into(), Value::UInt(direct.connection));
                            fields
                        }
                        None => HashMap::from([("handle".into(), Value::UInt(peer.hub_handle))]),
                    };
                    plain(Value::Dict(fields))
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
        "webrtc_connect" => {
            let signaling = get_session(handle_field(&f, "signaling")?)?;
            let session = connect_with_timeouts(
                &signaling.session,
                config(&f)?,
                &string_field(&f, "realm")?,
                codec(&f)?,
                authenticator(&f)?,
                timeouts(&f)?,
            )
            .await
            .map_err(|e| e.to_string())?;
            let output = Output::resource(
                BridgeResource::Session(Arc::new(ForeignSession::new(session.session.clone()))),
                session_fields(session.details()),
            );
            Ok(output.with_resource(
                "connection",
                BridgeResource::WebRTC(Resource::Connection(session.connection)),
            ))
        }
        "webrtc_open_session" => {
            let connection = connection(handle_field(&f, "handle")?)?;
            let realm = string_field(&f, "realm")?;
            let spec = codec(&f)?;
            let auth = authenticator(&f)?;
            let timeouts = timeouts(&f)?;
            let channel = connection.open_channel("data").await.map_err(|e| e.to_string())?;
            let session = WebRTCSession::join_with_timeouts(connection, channel, &realm, spec, auth, timeouts)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Output::resource(
                BridgeResource::Session(Arc::new(ForeignSession::new(session.session.clone()))),
                session_fields(session.details()),
            ))
        }
        "webrtc_provider_start" => {
            let signaling = get_session(handle_field(&f, "signaling")?)?;
            let mut router = get_router(handle_field(&f, "router")?)?;

            if let Some(authentication) = server_authentication(&f)? {
                router = router.with_authentication(authentication);
            }

            let provider = Provider::start_with_timeouts(signaling.session.clone(), router, config(&f)?, timeouts(&f)?)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Output::resource(
                BridgeResource::WebRTC(Resource::Provider(Arc::new(provider))),
                HashMap::new(),
            ))
        }
        "webrtc_provider_accept" => {
            let provider = provider(handle_field(&f, "handle")?)?;
            let Some(raw) = provider.accept_raw().await else {
                return plain(Value::Null);
            };
            let mut output = opened_channel(raw.channel, raw.connection, timeouts(&f)?.open_session).await?;

            if let Value::Dict(fields) = &mut output.value {
                fields.insert("connection_id".into(), Value::Str(raw.connection_id));
                fields.insert("first_is_string".into(), Value::Bool(raw.first_is_string));
                fields.insert(
                    "first_packet".into(),
                    raw.first_packet
                        .map(|data| Value::Bytes(ByteArray(data.to_vec())))
                        .unwrap_or(Value::Null),
                );
            }

            Ok(output)
        }
        "webrtc_provider_ice" => {
            provider(handle_field(&f, "handle")?)?.update_ice_servers(ice_servers(&f)?);
            plain(Value::Null)
        }
        "webrtc_provider_close" => {
            provider(handle_field(&f, "handle")?)?.close().await;
            plain(Value::Null)
        }
        "webrtc_channel_open" => {
            let connection = connection(handle_field(&f, "handle")?)?;
            let channel = connection
                .open_raw_channel(&string_field(&f, "label")?, options(&f)?)
                .await
                .map_err(|e| e.to_string())?;
            let mut pending = PendingChannel(Some(channel.clone()));
            channel
                .wait_open(timeouts(&f)?.open_session)
                .await
                .map_err(|e| e.to_string())?;
            let output = channel_output(channel, connection);
            pending.0 = None;
            Ok(output)
        }
        "webrtc_channel_accept" => {
            let connection = connection(handle_field(&f, "handle")?)?;
            let channel = connection.accept_channel().await.map_err(|e| e.to_string())?;
            opened_channel(channel, connection, timeouts(&f)?.open_session).await
        }
        "webrtc_channel_connection" => Ok(Output::resource(
            BridgeResource::WebRTC(Resource::Connection(connection(handle_field(&f, "handle")?)?)),
            HashMap::new(),
        )),
        "webrtc_remote_address" => {
            let address = connection(handle_field(&f, "handle")?)?.selected_remote_address().await;
            plain(address.map(Value::Str).unwrap_or(Value::Null))
        }
        "webrtc_connection_wait" => {
            connection(handle_field(&f, "handle")?)?.closed().await;
            plain(Value::Null)
        }
        "webrtc_connection_close" => {
            connection(handle_field(&f, "handle")?)?
                .close()
                .await
                .map_err(|e| e.to_string())?;
            plain(Value::Null)
        }
        "webrtc_read_data_packet" => {
            let packet = channel(handle_field(&f, "handle")?)?
                .read_data_packet()
                .await
                .map_err(|e| e.to_string())?;
            plain(crate::values::object_value([
                ("data", Value::Bytes(ByteArray(packet.data.to_vec()))),
                ("is_string", Value::Bool(packet.is_string)),
            ]))
        }
        "webrtc_send_text" => {
            channel(handle_field(&f, "handle")?)?
                .send_text(&string_field(&f, "text")?)
                .await
                .map_err(|e| e.to_string())?;
            plain(Value::Null)
        }
        "webrtc_buffered_amount" => plain(Value::UInt(
            channel(handle_field(&f, "handle")?)?.buffered_amount().await as u64,
        )),
        "webrtc_buffered_threshold" => {
            let threshold =
                usize::try_from(handle_field(&f, "threshold")?).map_err(|_| "invalid buffered threshold")?;
            channel(handle_field(&f, "handle")?)?
                .set_buffered_amount_low_threshold(threshold)
                .await;
            plain(Value::Null)
        }
        "webrtc_buffered_low" => {
            let event = channel(handle_field(&f, "handle")?)?
                .next_buffered_low(handle_field(&f, "previous")?)
                .await
                .map_err(|e| e.to_string())?;
            plain(Value::UInt(event))
        }
        "webrtc_channel_failure" => plain(
            channel(handle_field(&f, "handle")?)?
                .failure()
                .await
                .map(Value::Str)
                .unwrap_or(Value::Null),
        ),
        "webrtc_channel_wait" => {
            channel(handle_field(&f, "handle")?)?.closed().await;
            plain(Value::Null)
        }
        "webrtc_channel_close" => {
            channel(handle_field(&f, "handle")?)?
                .close_gracefully(std::time::Duration::from_secs(2))
                .await
                .map_err(|e| e.to_string())?;
            plain(Value::Null)
        }
        "webrtc_channel_unread" => {
            channel(handle_field(&f, "handle")?)?
                .unread_data_packet(xconn_webrtc::Packet {
                    data: bytes(&f)?.into(),
                    is_string: f.get("is_string") == Some(&Value::Bool(true)),
                })
                .await;
            plain(Value::Null)
        }
        "webrtc_read_packet" | "webrtc_read_message" => {
            let channel = channel(handle_field(&f, "handle")?)?;
            let data = if method == "webrtc_read_packet" {
                channel.read_packet().await.map(|data| data.to_vec())
            } else {
                channel.read_message().await
            }
            .map_err(|e| e.to_string())?;
            plain(Value::Bytes(ByteArray(data)))
        }
        "webrtc_write_packet" | "webrtc_write_message" => {
            let channel = channel(handle_field(&f, "handle")?)?;
            let data = bytes(&f)?;
            // Cancelling a partial framed write must not corrupt the next message.
            let mut pending = PendingChannel(if method == "webrtc_write_message" {
                Some(channel.clone())
            } else {
                None
            });

            if method == "webrtc_write_packet" {
                channel.send_packet(data.into()).await
            } else {
                channel.write_message(&data).await
            }
            .map_err(|e| e.to_string())?;
            pending.0 = None;
            plain(Value::Null)
        }
        _ => Err("unknown WebRTC operation".into()),
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

pub(super) struct ManagedPeer {
    manager: xconn_core::async_::services::peer::Peer<ManagedDirect>,
    hub_handle: u64,
}

struct ManagedDirect {
    handle: u64,
    connection: u64,
    session: WebRTCSession,
}

#[async_trait::async_trait]
impl xconn_core::async_::services::peer::DirectConnection for ManagedDirect {
    fn session(&self) -> &xconn_core::async_::session::Session {
        &self.session.session
    }

    async fn close(&self) {
        let _ = self.session.connection.close().await;
        crate::resources::close(self.handle);
        crate::resources::close(self.connection);
    }
}

impl Drop for ManagedDirect {
    fn drop(&mut self) {
        crate::resources::close(self.handle);
        crate::resources::close(self.connection);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration(server: HashMap<String, Value>) -> HashMap<String, Value> {
        HashMap::from([("ice_servers".into(), Value::List(vec![Value::Dict(server)]))])
    }

    #[test]
    fn stun_servers_do_not_require_credentials() {
        let fields = configuration(HashMap::from([(
            "urls".into(),
            Value::List(vec![Value::Str("stun:127.0.0.1:9".into())]),
        )]));
        let servers = ice_servers(&fields).unwrap();
        assert_eq!(servers[0].urls, ["stun:127.0.0.1:9"]);
        assert!(servers[0].username.is_empty());
        assert!(servers[0].credential.is_empty());
    }

    #[test]
    fn turn_credentials_are_preserved() {
        let fields = configuration(HashMap::from([
            (
                "urls".into(),
                Value::List(vec![Value::Str("turn:127.0.0.1:3478".into())]),
            ),
            ("username".into(), Value::Str("test-user".into())),
            ("credential".into(), Value::Str("test-credential".into())),
        ]));
        let servers = ice_servers(&fields).unwrap();
        assert_eq!(servers[0].username, "test-user");
        assert_eq!(servers[0].credential, "test-credential");
    }

    #[test]
    fn invalid_credential_types_are_rejected() {
        let fields = configuration(HashMap::from([
            ("urls".into(), Value::List(vec![Value::Str("stun:127.0.0.1:9".into())])),
            ("credential".into(), Value::Bool(true)),
        ]));
        assert!(ice_servers(&fields).is_err());
    }
}
