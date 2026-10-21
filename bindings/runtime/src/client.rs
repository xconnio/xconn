//! Client/session operations and progressive call adapters.
use crate::{
    ByteArray, Result, Value,
    callbacks::{ForeignPeer, SharedPeer, authenticator, codec, get_callbacks},
    close, id,
    resources::{
        CallInput, Cleanup, ForeignBase, ForeignCall, ForeignSession, InvocationGuard, Output, Resource, get_base,
        get_call, get_resource, get_router, get_session,
    },
    values::{
        call_response, connection_timeout, dictionary, duration_field, handle_field, object_value, request,
        request_args, request_kwargs, request_options, session_fields, string_field, wamp_error,
    },
};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};
use tokio::sync::{mpsc, oneshot};
use xconn_core::{
    Result as RpcResult, WampError,
    async_::{client::Client, session::Session},
};

pub(super) async fn dispatch(method: &str, f: HashMap<String, Value>) -> Result<Output> {
    let plain = |value| Ok(Output::plain(value));

    match method {
        "connect_base" => {
            let router = get_router(handle_field(&f, "handle")?)?;
            let spec = codec(&f)?;
            let capacity =
                usize::try_from(handle_field(&f, "out_queue_size")?).map_err(|_| "queue size exceeds usize")?;
            let capacity = if capacity == 0 { 16 } else { capacity };
            let realm = string_field(&f, "realm")?;
            let authid = string_field(&f, "authid")?;
            let authrole = string_field(&f, "authrole")?;
            let (peer, details) = if f.contains_key("session_id") {
                let details = xconn_core::wampproto::types::SessionDetails::new(
                    i64::try_from(handle_field(&f, "session_id")?).map_err(|_| "session ID exceeds i64")?,
                    realm,
                    authid,
                    authrole,
                    false,
                );
                router
                    .connect_internal_peer_with_details(details, spec.clone(), capacity)
                    .await
            } else {
                router
                    .connect_internal_peer(&realm, &authid, &authrole, spec.clone(), capacity)
                    .await
            }
            .map_err(|e| e.to_string())?;
            let fields = session_fields(&details);
            Ok(Output::resource(
                Resource::Base(Arc::new(ForeignBase {
                    peer: Arc::from(peer),
                    details,
                    serializer: spec.serializer(),
                    transferred: std::sync::atomic::AtomicBool::new(false),
                })),
                fields,
            ))
        }
        "session_from_peer" => {
            let details = xconn_core::wampproto::types::SessionDetails::new(
                i64::try_from(handle_field(&f, "session_id")?).map_err(|_| "session ID exceeds i64")?,
                string_field(&f, "realm")?,
                string_field(&f, "authid")?,
                string_field(&f, "authrole")?,
                false,
            )
            .with_authentication(
                string_field(&f, "authmethod")?,
                HashMap::new(),
                dictionary(f.get("authextra").unwrap_or(&Value::Null))?,
                String::new(),
            );
            let details = xconn_core::SessionDetails::from_protocol(&details);
            let fields = session_fields(&details);
            let peer = ForeignPeer {
                callbacks: get_callbacks(handle_field(&f, "callback")?)?,
            };
            let session = Session::new(details, Box::new(peer), codec(&f)?.serializer());
            Ok(Output::resource(
                Resource::Session(Arc::new(ForeignSession::new(session))),
                fields,
            ))
        }
        "session_new" => {
            let base = get_base(handle_field(&f, "handle")?)?;

            if base.transferred.swap(true, Ordering::AcqRel) {
                return Err("base session already belongs to a Session".into());
            }

            let session = Session::new(
                base.details.clone(),
                Box::new(SharedPeer(base.peer.clone())),
                base.serializer.clone(),
            );
            Ok(Output::resource(
                Resource::Session(Arc::new(ForeignSession::new(session))),
                session_fields(&base.details),
            ))
        }
        "base_read" | "base_write" | "base_try_write" | "base_close" => {
            let base = get_base(handle_field(&f, "handle")?)?;

            if method == "base_close" {
                base.peer.close().await.map_err(|e| e.to_string())?;
                return plain(Value::Null);
            }

            if base.transferred.load(Ordering::Acquire) {
                return Err("base session belongs to a Session".into());
            }

            if method == "base_read" {
                return plain(Value::Bytes(ByteArray(
                    base.peer.read().await.map_err(|e| e.to_string())?,
                )));
            }

            let bytes = match f.get("bytes") {
                Some(Value::Bytes(bytes)) => bytes.0.clone(),
                _ => return Err("expected bytes".into()),
            };

            if method == "base_try_write" {
                return plain(Value::Bool(base.peer.try_write(bytes).map_err(|e| e.to_string())?));
            }

            base.peer.write(bytes).await.map_err(|e| e.to_string())?;
            plain(Value::Null)
        }
        "connect" | "connect_local" => {
            let spec = codec(&f)?;
            let auth = authenticator(&f)?;
            let realm = string_field(&f, "realm")?;
            let s = if method == "connect" {
                Client::new(spec, auth)
                    .connect_with_timeout(&string_field(&f, "uri")?, &realm, connection_timeout(&f)?)
                    .await
            } else {
                get_router(handle_field(&f, "handle")?)?
                    .connect(&realm, spec, auth)
                    .await
            }
            .map_err(|e| e.to_string())?;
            let details = HashMap::from([
                ("session_id".into(), Value::Int(s.details().id())),
                ("authid".into(), Value::Str(s.details().authid())),
                ("authrole".into(), Value::Str(s.details().auth_role())),
                ("realm".into(), Value::Str(s.details().realm())),
                ("authmethod".into(), Value::Str(s.details().auth_method().into())),
                ("authextra".into(), Value::Dict(s.details().auth_extra().clone())),
            ]);
            Ok(Output::resource(
                Resource::Session({
                    let session = Arc::new(ForeignSession::new(s));
                    session.keep_alive(&f)?;
                    session
                }),
                details,
            ))
        }
        "stream_receive" | "stream_input" | "stream_cancel" => {
            let call = match get_resource(handle_field(&f, "handle")?)?.as_ref() {
                Resource::Stream(call) => call.clone(),
                _ => return Err("expected active call".into()),
            };

            match method {
                "stream_receive" => match call.receive().await {
                    Ok(Some(result)) => plain(call_response(result)),
                    Ok(None) => plain(Value::Null),
                    Err(error) => match error.application {
                        Some(application) => plain(object_value([("error", wamp_error(Some(application)))])),
                        None => Err(error.to_string()),
                    },
                },
                "stream_input" => {
                    let args = request_args(&f)?;
                    let kwargs = request_kwargs(&f)?;

                    if f.get("finish") == Some(&Value::Bool(true)) {
                        call.finish(args, kwargs).await
                    } else {
                        call.send(args, kwargs).await
                    }
                    .map_err(|error| error.to_string())?;
                    plain(Value::Null)
                }
                _ => {
                    call.cancel();
                    plain(Value::Null)
                }
            }
        }
        "call_next" => {
            let call = get_call(handle_field(&f, "handle")?)?;
            let response = call
                .output
                .lock()
                .await
                .recv()
                .await
                .ok_or("call stream finished")?
                .map_err(|e| e.to_string())?;
            plain(call_response(response))
        }
        "call_input" => {
            let call = get_call(handle_field(&f, "handle")?)?;
            let (reply, receiver) = oneshot::channel();
            call.input
                .send(CallInput {
                    args: request_args(&f)?,
                    kwargs: request_kwargs(&f)?,
                    more: f.get("more") == Some(&Value::Bool(true)),
                    reply,
                })
                .await
                .map_err(|_| "call input finished")?;
            receiver.await.map_err(|_| "call input cancelled")??;
            plain(Value::Null)
        }
        "invocation_progress" => {
            let session = get_session(handle_field(&f, "handle")?)?;
            let context = session
                .invocation_contexts
                .lock()
                .unwrap()
                .get(&handle_field(&f, "id")?)
                .cloned()
                .ok_or("invocation no longer active")?;
            context
                .send_progress(request_args(&f)?, request_kwargs(&f)?)
                .await
                .map_err(|e| e.to_string())?;
            plain(Value::Null)
        }
        "reply" => {
            let s = get_session(handle_field(&f, "handle")?)?;
            let tx = s
                .invocations
                .lock()
                .unwrap()
                .remove(&handle_field(&f, "id")?)
                .ok_or("invocation no longer active")?;
            let error = match f.get("error") {
                None | Some(Value::Null) => None,
                Some(value) => {
                    let e = dictionary(value)?;
                    Some(WampError {
                        uri: string_field(&e, "uri")?,
                        args: if matches!(e.get("args"), None | Some(Value::Null)) {
                            None
                        } else {
                            Some(request_args(&e)?)
                        },
                        kwargs: if matches!(e.get("kwargs"), None | Some(Value::Null)) {
                            None
                        } else {
                            Some(request_kwargs(&e)?)
                        },
                    })
                }
            };
            tx.send(RpcResult {
                args: if matches!(f.get("args"), None | Some(Value::Null)) {
                    None
                } else {
                    Some(request_args(&f)?)
                },
                kwargs: if matches!(f.get("kwargs"), None | Some(Value::Null)) {
                    None
                } else {
                    Some(request_kwargs(&f)?)
                },
                error,
                details: HashMap::new(),
            })
            .map_err(|_| "invocation cancelled")?;
            plain(Value::Null)
        }
        _ => {
            let s = get_session(handle_field(&f, "handle")?)?;

            match method {
                "ping" => {
                    let timeout = duration_field(&f, "timeout", Some(std::time::Duration::from_secs(10)))?;
                    plain(Value::Float(
                        s.session
                            .ping_with_timeout(timeout)
                            .await
                            .map_err(|e| e.to_string())?
                            .as_secs_f64(),
                    ))
                }
                "stream_open" => {
                    let options = xconn_core::StreamOptions {
                        progressive_input: f.get("progressive_input") == Some(&Value::Bool(true)),
                        progressive_results: match f.get("progressive_results") {
                            None => true,
                            Some(Value::Bool(enabled)) => *enabled,
                            Some(_) => return Err("progressive_results must be a boolean".into()),
                        },
                        buffer_size: match f.get("buffer_size") {
                            Some(_) => {
                                usize::try_from(handle_field(&f, "buffer_size")?).map_err(|_| "invalid buffer size")?
                            }
                            None => 32,
                        },
                        timeout: duration_field(&f, "timeout", None)?,
                    };
                    let call = xconn_core::async_::call::Call::open(&s.session, request(&f)?, options)
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok(Output::resource(Resource::Stream(call), HashMap::new()))
                }
                "call_open" => {
                    let mut stream = s.session.call_stream(request(&f)?).await.map_err(|e| e.to_string())?;
                    let (input, mut commands) = mpsc::channel::<CallInput>(16);
                    let (output, receiver) = mpsc::channel(16);
                    let task = tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                command = commands.recv() => {
                                    let Some(command) = command else {
                                        break;
                                    };
                                    let result = stream
                                        .send_input(command.args, command.kwargs, command.more)
                                        .await
                                        .map_err(|e| e.to_string());
                                    let _ = command.reply.send(result);
                                }
                                response = stream.next() => {
                                    match response {
                                        Ok(Some(response)) => {
                                            let finished = response.details.get("progress") != Some(&Value::Bool(true));

                                            if output.send(Ok(response)).await.is_err() || finished {
                                                break;
                                            }
                                        }
                                        Ok(None) => break,
                                        Err(error) => {
                                            let _ = output.send(Err(error)).await;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    });
                    Ok(Output::resource(
                        Resource::Call(Arc::new(ForeignCall {
                            input,
                            output: tokio::sync::Mutex::new(receiver),
                            task: task.abort_handle(),
                        })),
                        HashMap::new(),
                    ))
                }
                "call" => {
                    let response = s.session.execute_call(request(&f)?).await.map_err(|e| e.to_string())?;
                    plain(object_value([
                        ("args", response.args.map(Value::List).unwrap_or(Value::Null)),
                        ("kwargs", response.kwargs.map(Value::Dict).unwrap_or(Value::Null)),
                        ("details", Value::Dict(response.details)),
                        ("error", wamp_error(response.error)),
                    ]))
                }
                "publish" => {
                    let response = s
                        .session
                        .execute_publish(request(&f)?)
                        .await
                        .map_err(|e| e.to_string())?;
                    plain(object_value([("error", wamp_error(response.and_then(|r| r.error)))]))
                }
                "register" => {
                    let token = handle_field(&f, "token")?;
                    let weak = Arc::downgrade(&s);
                    let response = s
                        .session
                        .register_context(string_field(&f, "uri")?, request_options(&f)?, move |context| {
                            let weak = weak.clone();
                            async move {
                                let Some(s) = weak.upgrade() else {
                                    return RpcResult::error("wamp.error.canceled");
                                };
                                let inv_id = id();
                                let (tx, rx) = oneshot::channel();
                                s.invocations.lock().unwrap().insert(inv_id, tx);
                                s.invocation_contexts.lock().unwrap().insert(inv_id, context.clone());
                                let _guard = InvocationGuard {
                                    session: s.clone(),
                                    id: inv_id,
                                };
                                let event = object_value([
                                    ("kind", Value::Str("invocation".into())),
                                    ("token", Value::UInt(token)),
                                    ("id", Value::UInt(inv_id)),
                                    (
                                        "args",
                                        context.payload_args().clone().map(Value::List).unwrap_or(Value::Null),
                                    ),
                                    (
                                        "kwargs",
                                        context.payload_kwargs().clone().map(Value::Dict).unwrap_or(Value::Null),
                                    ),
                                    ("details", Value::Dict(context.invocation.details.clone())),
                                ]);

                                if s.events.send(event).await.is_err() {
                                    return RpcResult::error("wamp.error.canceled");
                                }

                                rx.await.unwrap_or_else(|_| RpcResult::error("wamp.error.canceled"))
                            }
                        })
                        .await
                        .map_err(|e| e.to_string())?;
                    let owned = response
                        .error
                        .is_none()
                        .then(|| Cleanup::Registration(s.clone(), response.registration_id));
                    Ok(Output {
                        value: object_value([
                            ("id", Value::Int(response.registration_id)),
                            ("error", wamp_error(response.error)),
                        ]),
                        owned,
                    })
                }
                "subscribe" => {
                    let token = handle_field(&f, "token")?;
                    let weak = Arc::downgrade(&s);
                    let response = s
                        .session
                        .subscribe_context(string_field(&f, "uri")?, request_options(&f)?, move |event| {
                            let weak = weak.clone();
                            async move {
                                if let Some(s) = weak.upgrade() {
                                    let _ = s
                                        .events
                                        .send(object_value([
                                            ("kind", Value::Str("event".into())),
                                            ("token", Value::UInt(token)),
                                            ("args", event.args.map(Value::List).unwrap_or(Value::Null)),
                                            ("kwargs", event.kwargs.map(Value::Dict).unwrap_or(Value::Null)),
                                            ("details", Value::Dict(event.event.details)),
                                        ]))
                                        .await;
                                }
                            }
                        })
                        .await
                        .map_err(|e| e.to_string())?;
                    let id = response.id;
                    s.subscriptions.lock().unwrap().insert(token, Arc::new(response));
                    Ok(Output {
                        value: object_value([("id", Value::Int(id)), ("error", Value::Null)]),
                        owned: Some(Cleanup::Subscription(s.clone(), token)),
                    })
                }
                "unregister" => plain(object_value([(
                    "error",
                    wamp_error(
                        s.session
                            .unregister(handle_field(&f, "id")? as i64)
                            .await
                            .map_err(|e| e.to_string())?,
                    ),
                )])),
                "unsubscribe" => {
                    let token = handle_field(&f, "token")?;
                    let subscription = s
                        .subscriptions
                        .lock()
                        .unwrap()
                        .get(&token)
                        .cloned()
                        .ok_or("subscription already closed")?;
                    let error = subscription.unsubscribe().await.map_err(|e| e.to_string())?;

                    if error.is_none() {
                        s.subscriptions.lock().unwrap().remove(&token);
                    }

                    plain(object_value([("error", wamp_error(error))]))
                }
                "next_event" => {
                    let mut rx = s.receiver.lock().await;
                    tokio::select! {
                        event = rx.recv() => plain(event.unwrap_or(Value::Null)),
                        _ = s.session.wait_disconnect() => plain(Value::Null),
                    }
                }
                "leave" => {
                    let result = s.session.leave().await.map_err(|e| e.to_string());
                    close(handle_field(&f, "handle")?);
                    result?;
                    plain(Value::Null)
                }
                _ => Err(format!("unknown operation {method}")),
            }
        }
    }
}

#[cfg(test)]
mod compensation_tests {
    use super::*;
    use crate::{cancel, ready, runtime, submit};
    use xconn_core::{
        CallRequest, JSONSerializerSpec,
        async_::router::{RealmConfig, Router},
        wampproto::authenticators::anonymous::AnonymousAuthenticator,
    };

    #[test]
    fn cancelled_registration_acknowledgement_does_not_leave_an_orphan() {
        runtime().block_on(async {
            let router = Router::new();
            router.add_realm("realm", RealmConfig::default()).unwrap();
            let callee = router
                .connect(
                    "realm",
                    Box::new(JSONSerializerSpec),
                    Box::new(AnonymousAuthenticator::default()),
                )
                .await
                .unwrap();
            let caller = router
                .connect(
                    "realm",
                    Box::new(JSONSerializerSpec),
                    Box::new(AnonymousAuthenticator::default()),
                )
                .await
                .unwrap();
            let output =
                Output::resource(Resource::Session(Arc::new(ForeignSession::new(callee))), HashMap::new()).take();
            let handle = handle_field(&dictionary(&output).unwrap(), "handle").unwrap();
            let operation = submit(object_value([
                ("method", Value::Str("register".into())),
                ("handle", Value::UInt(handle)),
                ("uri", Value::Str("orphan".into())),
                ("token", Value::UInt(1)),
            ]));
            ready(operation).await.unwrap();
            cancel(operation);
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let result = tokio::time::timeout(
                        std::time::Duration::from_millis(20),
                        caller.execute_call(CallRequest::new("orphan")),
                    )
                    .await;

                    if let Ok(Ok(result)) = result
                        && result.error.is_some_and(|e| e.uri == "wamp.error.no_such_procedure")
                    {
                        break;
                    }

                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled registration was not compensated");
            close(handle);
            caller.close().await.unwrap();
            router.close();
        });
    }
}
