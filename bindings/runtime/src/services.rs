//! Thin handle adapters for optional shared communication services.
use crate::{
    Result, Value,
    callbacks::get_callbacks,
    resources::{Output, Resource, get_resource, get_session},
    values::{dictionary, handle_field, object_value, string_field},
};
use std::{collections::HashMap, sync::Arc};
use xconn_core::async_::services::bridge::{Bridge, BridgeRoute, SessionFactory};

pub(super) async fn dispatch(method: &str, fields: HashMap<String, Value>) -> Result<Output> {
    match method {
        "service_discovery_start" => {
            use xconn_core::async_::services::discovery::{DiscoveryAction, DiscoveryService};
            let session = get_session(handle_field(&fields, "session")?)?;
            let prefix = string_field(&fields, "prefix")?;
            let callbacks = get_callbacks(handle_field(&fields, "callback")?)?;
            let policy = Arc::new(
                move |caller: &xconn_core::async_::services::bridge::Caller,
                      action,
                      endpoint: &xconn_core::async_::services::discovery::Endpoint| {
                    let action = match action {
                        DiscoveryAction::Advertise => "advertise",
                        DiscoveryAction::Discover => "discover",
                        DiscoveryAction::Withdraw => "withdraw",
                    };
                    matches!(
                        callbacks.call(
                            "discovery_authorize",
                            object_value([
                                ("session_id", Value::Int(caller.session_id)),
                                ("authid", Value::Str(caller.authid.clone())),
                                ("authrole", Value::Str(caller.authrole.clone())),
                                ("action", Value::Str(action.into())),
                                ("endpoint", endpoint.to_value()),
                            ])
                        ),
                        Ok(Value::Bool(true))
                    )
                },
            );
            let service = DiscoveryService::start(session.session.clone(), &prefix, policy)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Output::resource(Resource::Discovery(Arc::new(service)), HashMap::new()))
        }
        "service_discovery_close" => {
            let resource = get_resource(handle_field(&fields, "handle")?)?;
            let Resource::Discovery(service) = resource.as_ref() else {
                return Err("expected discovery service".into());
            };
            service.close().await.map_err(|error| error.to_string())?;
            Ok(Output::plain(Value::Null))
        }
        "service_discovery_request" => {
            use xconn_core::async_::services::discovery::DiscoveryClient;
            let session = get_session(handle_field(&fields, "session")?)?;
            let prefix = string_field(&fields, "prefix")?;
            let action = string_field(&fields, "action")?;
            let args = crate::values::request_args(&fields)?;
            let client = DiscoveryClient::new(session.session.clone(), prefix).map_err(|error| error.to_string())?;
            let value = match client.request(&action, args).await {
                Ok(value) => value,
                Err(error) => {
                    if let Some(error) = error.application {
                        return Ok(Output::plain(object_value([(
                            "error",
                            crate::values::wamp_error(Some(error)),
                        )])));
                    } else {
                        return Err(error.to_string());
                    }
                }
            };
            Ok(Output::plain(value))
        }
        "service_attachment_run" => {
            use xconn_core::async_::services::attachment::{AttachmentOutcome, RetryPolicy, supervise};
            let callbacks = get_callbacks(handle_field(&fields, "callback")?)?;
            let report = callbacks.clone();
            let policy = RetryPolicy {
                initial: crate::values::duration_field(&fields, "initial", Some(std::time::Duration::from_secs(1)))?
                    .ok_or("initial retry duration is required")?,
                maximum: crate::values::duration_field(&fields, "maximum", Some(std::time::Duration::from_secs(30)))?
                    .ok_or("maximum retry duration is required")?,
            };
            supervise(
                policy,
                move || {
                    let callbacks = callbacks.clone();
                    async move {
                        let response = callbacks
                            .call("attachment_cycle", Value::Null)
                            .and_then(|value| dictionary(&value));
                        let response = match response {
                            Ok(response) => response,
                            Err(error) => return AttachmentOutcome::Retry(xconn_core::Error::new(error)),
                        };
                        let error = xconn_core::Error::new(
                            string_field(&response, "error").unwrap_or_else(|_| "attachment disconnected".into()),
                        );
                        match string_field(&response, "state").as_deref() {
                            Ok("detached") => AttachmentOutcome::Detached,
                            Ok("disconnected") => AttachmentOutcome::Disconnected(error),
                            _ => AttachmentOutcome::Retry(error),
                        }
                    }
                },
                move |error| {
                    let _ = report.call("attachment_error", Value::Str(error.to_string()));
                },
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(Output::plain(Value::Null))
        }
        "service_bridge_new" => {
            let source = get_session(handle_field(&fields, "source")?)?;
            let target = get_session(handle_field(&fields, "target")?)?;
            let routes = match fields.get("routes") {
                Some(Value::List(routes)) => routes
                    .iter()
                    .map(|route| {
                        let route = dictionary(route)?;
                        Ok(BridgeRoute {
                            procedure: string_field(&route, "procedure")?,
                            options: dictionary(route.get("options").unwrap_or(&Value::Null))?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                _ => return Err("expected bridge routes".into()),
            };
            let factory: Option<SessionFactory> = if fields.contains_key("callback") {
                let callbacks = get_callbacks(handle_field(&fields, "callback")?)?;
                Some(Arc::new(move |caller| {
                    let callbacks = callbacks.clone();
                    Box::pin(async move {
                        let value = callbacks
                            .call(
                                "bridge_session",
                                object_value([
                                    ("session_id", Value::Int(caller.session_id)),
                                    ("authid", Value::Str(caller.authid)),
                                    ("authrole", Value::Str(caller.authrole)),
                                ]),
                            )
                            .map_err(xconn_core::Error::new)?;
                        let fields = dictionary(&value).map_err(xconn_core::Error::new)?;
                        let session = get_session(handle_field(&fields, "handle").map_err(xconn_core::Error::new)?)
                            .map_err(xconn_core::Error::new)?;
                        Ok(session.session.clone())
                    })
                }))
            } else {
                None
            };
            let bridge = Bridge::new(source.session.clone(), target.session.clone(), routes, factory)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Output::resource(Resource::Bridge(Arc::new(bridge)), HashMap::new()))
        }
        "service_bridge_close" => {
            let resource = get_resource(handle_field(&fields, "handle")?)?;
            let Resource::Bridge(bridge) = resource.as_ref() else {
                return Err("expected bridge".into());
            };
            bridge.close().await.map_err(|error| error.to_string())?;
            Ok(Output::plain(Value::Null))
        }
        _ => Err(format!("unknown service operation {method}")),
    }
}
