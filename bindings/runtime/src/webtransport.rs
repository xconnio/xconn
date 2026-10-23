//! WebTransport handles reuse the bounded native stream I/O adapter.
use crate::{
    ByteArray, Result, Value,
    callbacks::{authenticator, codec, server_router},
    quic::{HostVerification, stream_output},
    resources::{ForeignSession, Output, Resource as BridgeResource, get_resource},
    values::{handle_field, session_fields, string_field},
};
use std::{collections::HashMap, sync::Arc};
use xconn_core::async_::webtransport::{Connection, Listener, backend};

pub(super) enum Resource {
    Connection(Arc<Connection>),
    Listener(Arc<Listener>),
}

impl Resource {
    pub(super) fn close(&self) {
        match self {
            Self::Connection(connection) => connection.close(),
            Self::Listener(listener) => listener.close(),
        }
    }
}

fn connection(handle: u64) -> Result<Arc<Connection>> {
    match get_resource(handle)?.as_ref() {
        BridgeResource::WebTransport(Resource::Connection(connection)) => Ok(connection.clone()),
        _ => Err("expected WebTransport connection".into()),
    }
}

pub(super) async fn dispatch(method: &str, fields: HashMap<String, Value>) -> Result<Output> {
    match method {
        "webtransport_listen" => {
            let router = server_router(&fields)?;
            let address = string_field(&fields, "address")?
                .parse()
                .map_err(|_| "invalid WebTransport address")?;
            let Some(Value::List(chain)) = fields.get("certificates") else {
                return Err("WebTransport listener requires a certificate chain".into());
            };
            let certificates = chain
                .iter()
                .map(|value| match value {
                    Value::Bytes(bytes) => Ok(rustls::pki_types::CertificateDer::from(bytes.0.clone())),
                    _ => Err("expected DER certificate".to_string()),
                })
                .collect::<Result<Vec<_>>>()?;
            let certificate = certificates.first().ok_or("empty certificate chain")?.to_vec();
            let Some(Value::Bytes(key)) = fields.get("private_key") else {
                return Err("expected DER private key".into());
            };
            let key = rustls::pki_types::PrivateKeyDer::try_from(key.0.clone()).map_err(str::to_string)?;
            let mut transport = backend::quion::TransportConfig::default();
            transport
                .set_reset_stream_at(true)
                .set_max_datagram_frame_size(Some(backend::quion::VarInt::from_u32(65_535)))
                .set_retry_enabled(false)
                .set_max_connections(1024)
                .set_max_pending_handshakes(256)
                .set_max_endpoint_memory_bytes(64 * 1024 * 1024);
            let server = backend::ServerBuilder::new()
                .with_addr(address)
                .with_transport_config(transport)
                .with_handshake_timeout(std::time::Duration::from_secs(10))
                .with_max_pending_handshakes(std::num::NonZeroUsize::new(256).unwrap())
                .with_certificate(certificates, key)
                .map_err(|e| e.to_string())?;
            let path = string_field(&fields, "path")?;
            if !path.starts_with('/') {
                return Err("WebTransport path must start with /".into());
            }
            let listener = Listener::with_server_path(router, server, certificate, Some(path))
                .await
                .map_err(|e| e.to_string())?;
            let fields = HashMap::from([
                ("address".into(), Value::Str(listener.local_addr.to_string())),
                (
                    "certificate".into(),
                    Value::Bytes(ByteArray(listener.certificate.clone())),
                ),
            ]);
            Ok(Output::resource(
                BridgeResource::WebTransport(Resource::Listener(Arc::new(listener))),
                fields,
            ))
        }
        "webtransport_dial" => {
            let builder =
                rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                    .with_safe_default_protocol_versions()
                    .map_err(|e| e.to_string())?;
            let tls = if fields.get("verify_server") == Some(&Value::Bool(true)) {
                let mut roots = rustls::RootCertStore::empty();
                if let Some(Value::Bytes(certificate)) = fields.get("certificate") {
                    roots
                        .add(rustls::pki_types::CertificateDer::from(certificate.0.clone()))
                        .map_err(|e| e.to_string())?;
                } else {
                    roots.add_parsable_certificates(crate::tls::trusted_certificates());
                }
                builder.with_root_certificates(roots).with_no_client_auth()
            } else {
                // The existing Go adapter verifies this captured chain itself.
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(HostVerification::new()))
                    .with_no_client_auth()
            };
            let connected = Connection::connect_with_tls(&string_field(&fields, "url")?, tls)
                .await
                .map_err(|e| e.to_string())?;
            let certificates = connected
                .certificates
                .into_iter()
                .map(|cert| Value::Bytes(ByteArray(cert)))
                .collect();
            Ok(Output::resource(
                BridgeResource::WebTransport(Resource::Connection(Arc::new(connected.connection))),
                HashMap::from([
                    ("certificates".into(), Value::List(certificates)),
                    ("local_address".into(), Value::Str(connected.local_addr.to_string())),
                    ("remote_address".into(), Value::Str(connected.remote_addr.to_string())),
                ]),
            ))
        }
        "webtransport_session" => {
            let session = connection(handle_field(&fields, "handle")?)?
                .open_session(
                    &string_field(&fields, "realm")?,
                    codec(&fields)?,
                    authenticator(&fields)?,
                )
                .await
                .map_err(|e| e.to_string())?;
            let fields = session_fields(session.details());
            Ok(Output::resource(
                BridgeResource::Session(Arc::new(ForeignSession::new(session))),
                fields,
            ))
        }
        "webtransport_open" | "webtransport_accept_remote" => {
            let connection = connection(handle_field(&fields, "handle")?)?;
            let stream = if method == "webtransport_open" {
                connection.open_raw().await
            } else {
                connection.accept_raw().await
            }
            .map_err(|e| e.to_string())?;
            Ok(stream_output(Box::new(stream), HashMap::new()))
        }
        "webtransport_accept" => {
            let resource = get_resource(handle_field(&fields, "handle")?)?;
            let BridgeResource::WebTransport(Resource::Listener(listener)) = resource.as_ref() else {
                return Err("expected WebTransport listener".into());
            };
            let stream = listener.accept_raw().await.ok_or("WebTransport listener closed")?;
            let fields = session_fields(&xconn_core::SessionDetails::from_protocol(&stream.session));
            Ok(stream_output(stream.stream, fields))
        }
        "webtransport_wait" => {
            connection(handle_field(&fields, "handle")?)?.connection.closed().await;
            Ok(Output::plain(Value::Null))
        }
        _ => Err(format!("unknown WebTransport operation {method}")),
    }
}
